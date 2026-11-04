"""Data cleaning: normalize fields, resolve nulls, and convert values.

``clean_event`` is the single entry point used by the output transformation. It
returns a deep-copied event whose timestamps are aware UTC datetimes and whose
money fields are exact ``Decimal`` values; the input is never mutated.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from copy import deepcopy
from datetime import UTC, tzinfo
from typing import Any

from pipeline_healer.pipeline.transforms.conversion import to_decimal, to_int, to_money
from pipeline_healer.pipeline.transforms.normalization import (
    normalize_currency,
    normalize_event_type,
    normalize_keys,
    normalize_text,
)
from pipeline_healer.pipeline.transforms.nulls import is_null, normalize_nulls
from pipeline_healer.pipeline.transforms.timestamps import normalize_timestamp
from pipeline_healer.pipeline.transforms.validation import validate_for_transformation
from pipeline_healer.pipeline.validation import RecordValidationError

MONEY_FIELDS = ("fare_amount", "total_amount", "amount")
DECIMAL_FIELDS = ("distance_km",)
INT_FIELDS = ("duration_min", "item_count")
PAYLOAD_TIMESTAMP_FIELDS = ("requested_at", "cancelled_at")


def _trim_strings(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _trim_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_trim_strings(item) for item in value]
    if isinstance(value, str):
        return None if is_null(value) else normalize_text(value)
    return value


def _convert_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("currency") is not None:
        payload["currency"] = normalize_currency(payload["currency"])

    for field in MONEY_FIELDS:
        if payload.get(field) is not None:
            payload[field] = to_money(payload[field], field=f"payload.{field}")

    for field in DECIMAL_FIELDS:
        if payload.get(field) is not None:
            payload[field] = to_decimal(payload[field], field=f"payload.{field}", minimum=0)

    for field in INT_FIELDS:
        if payload.get(field) is not None:
            payload[field] = to_int(payload[field], field=f"payload.{field}")

    for field in PAYLOAD_TIMESTAMP_FIELDS:
        if payload.get(field) is not None:
            payload[field] = normalize_timestamp(payload[field], field=f"payload.{field}")

    items = payload.get("items")
    if isinstance(items, list):
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            if item.get("quantity") is not None:
                item["quantity"] = to_int(
                    item["quantity"], field=f"payload.items[{index}].quantity"
                )
            if item.get("unit_price") is not None:
                item["unit_price"] = to_money(
                    item["unit_price"], field=f"payload.items[{index}].unit_price"
                )

    return payload


def clean_event(
    event: dict[str, Any],
    *,
    assume_timezone: str | tzinfo = UTC,
) -> dict[str, Any]:
    """Validate and clean one event without mutating the input."""
    problems = validate_for_transformation(event, assume_timezone=assume_timezone)
    if problems:
        raise RecordValidationError(problems)

    cleaned = deepcopy(event)
    cleaned["event_type"] = normalize_event_type(cleaned["event_type"])
    cleaned["occurred_at"] = normalize_timestamp(
        cleaned["occurred_at"], field="occurred_at", assume_timezone=assume_timezone
    )
    cleaned["produced_at"] = normalize_timestamp(
        cleaned["produced_at"], field="produced_at", assume_timezone=assume_timezone
    )

    payload = _trim_strings(normalize_nulls(normalize_keys(cleaned["payload"])))
    cleaned["payload"] = _convert_payload(payload)
    return cleaned


def clean_events(
    events: Iterable[dict[str, Any]],
    *,
    assume_timezone: str | tzinfo = UTC,
) -> Iterator[dict[str, Any]]:
    """Clean events lazily."""
    for event in events:
        yield clean_event(event, assume_timezone=assume_timezone)
