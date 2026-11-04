"""Validation of event records against the data contract.

Producers call :func:`validate_event` before emitting a record, and consumers or
tests can use :func:`validate_records` for a whole batch. A record that fails here
must never reach a topic.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from pipeline_healer.pipeline.producers.event_types import EventType
from pipeline_healer.pipeline.producers.identifiers import (
    DRIVER_ID_PATTERN,
    EVENT_ID_PATTERN,
    MERCHANT_ID_PATTERN,
    ORDER_ID_PATTERN,
    RIDE_ID_PATTERN,
    USER_ID_PATTERN,
)
from pipeline_healer.pipeline.producers.timestamps import MAX_FUTURE_SKEW, utc_now

SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
PRODUCER_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,31}$")
PAYMENT_ID_PATTERN = re.compile(r"^payment_\d{5,}$")

CURRENCIES = frozenset({"DZD"})
PAYMENT_METHODS = frozenset({"بطاقة", "محفظة إلكترونية", "نقداً", "الدفع عند الاستلام"})
EVENT_TYPE_VALUES = frozenset(member.value for member in EventType)

REQUIRED_ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "occurred_at",
    "produced_at",
    "producer",
    "payload",
)


class RecordValidationError(ValueError):
    """Raised when one or more records violate the data contract."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        preview = "; ".join(problems[:5])
        remainder = len(problems) - 5
        suffix = f" (+{remainder} more)" if remainder > 0 else ""
        super().__init__(f"invalid record: {preview}{suffix}")


def _check_string(
    problems: list[str],
    path: str,
    value: Any,
    pattern: re.Pattern[str] | None = None,
) -> None:
    if not isinstance(value, str) or not value:
        problems.append(f"{path}: must be a non-empty string")
    elif pattern is not None and not pattern.match(value):
        problems.append(f"{path}: does not match {pattern.pattern}")


def _check_number(
    problems: list[str],
    path: str,
    value: Any,
    *,
    minimum: float | None = None,
    integer: bool = False,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        kind = "integer" if integer else "number"
        problems.append(f"{path}: must be a {kind}")
        return
    if integer and not isinstance(value, int):
        problems.append(f"{path}: must be an integer")
        return
    if minimum is not None and value < minimum:
        problems.append(f"{path}: must be >= {minimum}")


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    if parsed.utcoffset() != timedelta(0):
        return None
    return parsed


def _validate_timestamps(problems: list[str], event: dict[str, Any]) -> None:
    occurred = _parse_timestamp(event["occurred_at"])
    produced = _parse_timestamp(event["produced_at"])

    if occurred is None:
        problems.append("occurred_at: must be RFC 3339 UTC with a Z suffix")
    if produced is None:
        problems.append("produced_at: must be RFC 3339 UTC with a Z suffix")

    if occurred is not None and occurred > utc_now() + MAX_FUTURE_SKEW:
        problems.append(f"occurred_at: more than {MAX_FUTURE_SKEW} in the future")
    if occurred is not None and produced is not None and produced < occurred:
        problems.append("produced_at: must not precede occurred_at")


def _validate_rides(problems: list[str], event_type: str, payload: dict[str, Any]) -> None:
    _check_string(problems, "payload.ride_id", payload.get("ride_id"), RIDE_ID_PATTERN)
    _check_string(problems, "payload.user_id", payload.get("user_id"), USER_ID_PATTERN)
    _check_string(problems, "payload.city", payload.get("city"))

    if event_type == "ride_requested":
        _check_string(problems, "payload.pickup_area", payload.get("pickup_area"))
        if _parse_timestamp(payload.get("requested_at")) is None:
            problems.append("payload.requested_at: must be RFC 3339 UTC with a Z suffix")
        return

    if event_type == "ride_completed":
        _check_string(problems, "payload.driver_id", payload.get("driver_id"), DRIVER_ID_PATTERN)
        _check_string(problems, "payload.pickup_area", payload.get("pickup_area"))
        _check_string(problems, "payload.dropoff_area", payload.get("dropoff_area"))
        _check_number(problems, "payload.distance_km", payload.get("distance_km"), minimum=0)
        _check_number(
            problems, "payload.duration_min", payload.get("duration_min"), minimum=1, integer=True
        )
        _check_number(problems, "payload.fare_amount", payload.get("fare_amount"), minimum=0)
        if payload.get("currency") not in CURRENCIES:
            problems.append("payload.currency: must be one of DZD")
        if payload.get("payment_method") not in PAYMENT_METHODS:
            problems.append("payload.payment_method: unknown value")
        return

    _check_string(problems, "payload.reason", payload.get("reason"))
    if _parse_timestamp(payload.get("cancelled_at")) is None:
        problems.append("payload.cancelled_at: must be RFC 3339 UTC with a Z suffix")


def _validate_items(problems: list[str], items: Any) -> None:
    if not isinstance(items, list) or not items:
        problems.append("payload.items: must be a non-empty list")
        return

    for index, item in enumerate(items):
        path = f"payload.items[{index}]"
        if not isinstance(item, dict):
            problems.append(f"{path}: must be an object")
            continue
        _check_string(problems, f"{path}.sku", item.get("sku"))
        _check_string(problems, f"{path}.name", item.get("name"))
        _check_number(problems, f"{path}.quantity", item.get("quantity"), minimum=1, integer=True)
        _check_number(problems, f"{path}.unit_price", item.get("unit_price"), minimum=0)


def _validate_orders(problems: list[str], event_type: str, payload: dict[str, Any]) -> None:
    _check_string(problems, "payload.order_id", payload.get("order_id"), ORDER_ID_PATTERN)

    if event_type == "order_placed":
        _check_string(problems, "payload.user_id", payload.get("user_id"), USER_ID_PATTERN)
        _check_string(
            problems, "payload.merchant_id", payload.get("merchant_id"), MERCHANT_ID_PATTERN
        )
        _check_string(problems, "payload.city", payload.get("city"))
        _check_string(problems, "payload.district", payload.get("district"))
        _validate_items(problems, payload.get("items"))
        _check_number(problems, "payload.total_amount", payload.get("total_amount"), minimum=0)
        if isinstance(payload.get("items"), list):
            items_total = sum(
                item.get("quantity", 0) * item.get("unit_price", 0)
                for item in payload["items"]
                if isinstance(item, dict)
            )
            total = payload.get("total_amount")
            if (
                isinstance(total, int | float)
                and not isinstance(total, bool)
                and abs(round(items_total, 2) - total) > 1e-9
            ):
                problems.append("payload.total_amount: does not match items")
        if payload.get("currency") not in CURRENCIES:
            problems.append("payload.currency: must be one of DZD")
        if payload.get("payment_method") not in PAYMENT_METHODS:
            problems.append("payload.payment_method: unknown value")
        return

    _check_string(problems, "payload.reason", payload.get("reason"))
    if _parse_timestamp(payload.get("cancelled_at")) is None:
        problems.append("payload.cancelled_at: must be RFC 3339 UTC with a Z suffix")


def _validate_payments(problems: list[str], event_type: str, payload: dict[str, Any]) -> None:
    _check_string(problems, "payload.payment_id", payload.get("payment_id"), PAYMENT_ID_PATTERN)
    _check_string(problems, "payload.event_id", payload.get("event_id"), EVENT_ID_PATTERN)
    _check_number(problems, "payload.amount", payload.get("amount"), minimum=0)
    if payload.get("currency") not in CURRENCIES:
        problems.append("payload.currency: must be one of DZD")

    if event_type == "payment_failed":
        _check_string(problems, "payload.failure_code", payload.get("failure_code"))
    else:
        _check_string(problems, "payload.method", payload.get("method"))


def _validate_payload(problems: list[str], event_type: str, payload: dict[str, Any]) -> None:
    if event_type.startswith("ride_"):
        _validate_rides(problems, event_type, payload)
    elif event_type.startswith("order_"):
        _validate_orders(problems, event_type, payload)
    elif event_type.startswith("payment_"):
        _validate_payments(problems, event_type, payload)


def validate_event(event: Any) -> list[str]:
    """Return a list of contract violations; an empty list means the record is valid."""
    if not isinstance(event, dict):
        return ["record: must be a JSON object"]

    problems: list[str] = []
    for field in REQUIRED_ENVELOPE_FIELDS:
        if field not in event:
            problems.append(f"{field}: missing")
    if problems:
        return problems

    _check_string(problems, "event_id", event["event_id"], EVENT_ID_PATTERN)
    _check_string(problems, "event_type", event["event_type"])
    if event["event_type"] not in EVENT_TYPE_VALUES:
        problems.append(f"event_type: unknown value {event['event_type']!r}")
    _check_string(problems, "schema_version", event["schema_version"], SEMVER_PATTERN)
    _check_string(problems, "producer", event["producer"], PRODUCER_PATTERN)
    _validate_timestamps(problems, event)

    payload = event["payload"]
    if not isinstance(payload, dict):
        problems.append("payload: must be an object")
    elif event["event_type"] in EVENT_TYPE_VALUES:
        _validate_payload(problems, event["event_type"], payload)

    return problems


def validate_records(events: Iterable[Any]) -> None:
    """Validate a batch, including event-id uniqueness (DQ-002)."""
    problems: list[str] = []
    seen: set[str] = set()

    for index, event in enumerate(events):
        for problem in validate_event(event):
            problems.append(f"record[{index}] {problem}")
        event_id = event.get("event_id") if isinstance(event, dict) else None
        if isinstance(event_id, str):
            if event_id in seen:
                problems.append(f"record[{index}] event_id: duplicate {event_id}")
            seen.add(event_id)

    if problems:
        raise RecordValidationError(problems)
