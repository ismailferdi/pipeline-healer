"""Transformation-stage validation.

Contract validation is the baseline; this layer additionally proves that every
timestamp and numeric field can actually be converted, so cleaning can proceed
without partial failures.
"""

from __future__ import annotations

from datetime import UTC, tzinfo
from typing import Any

from pipeline_healer.pipeline.transforms.conversion import to_decimal, to_int
from pipeline_healer.pipeline.transforms.errors import TransformError
from pipeline_healer.pipeline.transforms.timestamps import normalize_timestamp
from pipeline_healer.pipeline.validation import validate_event as validate_contract_event

TIMESTAMP_FIELDS = ("occurred_at", "produced_at")
PAYLOAD_TIMESTAMP_FIELDS = ("requested_at", "cancelled_at")
MONEY_FIELDS = ("fare_amount", "total_amount", "amount")
DECIMAL_FIELDS = ("distance_km",)
INT_FIELDS = ("duration_min", "item_count")
ITEM_MONEY_FIELDS = ("unit_price",)
ITEM_INT_FIELDS = ("quantity",)


def _check_timestamp(value: Any, field: str, assume_timezone: str | tzinfo) -> list[str]:
    try:
        normalize_timestamp(value, field=field, assume_timezone=assume_timezone)
    except TransformError as exc:
        return [str(exc)]
    return []


def _check_number(
    value: Any,
    field: str,
    *,
    integer: bool = False,
    minimum: float | None = None,
) -> list[str]:
    try:
        if integer:
            to_int(value, field=field)
        else:
            to_decimal(value, field=field, minimum=minimum)
    except TransformError as exc:
        return [str(exc)]
    return []


def validate_for_transformation(
    event: Any,
    *,
    assume_timezone: str | tzinfo = UTC,
) -> list[str]:
    """Return every reason an otherwise valid event cannot be transformed."""
    problems = list(validate_contract_event(event))
    if problems:
        return problems

    for field in TIMESTAMP_FIELDS:
        problems.extend(_check_timestamp(event.get(field), field, assume_timezone))

    payload = event["payload"]
    for field in PAYLOAD_TIMESTAMP_FIELDS:
        if payload.get(field) is not None:
            problems.extend(_check_timestamp(payload[field], f"payload.{field}", assume_timezone))

    for field in MONEY_FIELDS:
        if payload.get(field) is not None:
            problems.extend(_check_number(payload[field], f"payload.{field}", minimum=0))
    for field in DECIMAL_FIELDS:
        if payload.get(field) is not None:
            problems.extend(_check_number(payload[field], f"payload.{field}", minimum=0))
    for field in INT_FIELDS:
        if payload.get(field) is not None:
            problems.extend(_check_number(payload[field], f"payload.{field}", integer=True))

    items = payload.get("items")
    if isinstance(items, list):
        for index, item in enumerate(items):
            for field in ITEM_MONEY_FIELDS:
                if isinstance(item, dict) and item.get(field) is not None:
                    problems.extend(
                        _check_number(item[field], f"payload.items[{index}].{field}", minimum=0)
                    )
            for field in ITEM_INT_FIELDS:
                if isinstance(item, dict) and item.get(field) is not None:
                    problems.extend(
                        _check_number(item[field], f"payload.items[{index}].{field}", integer=True)
                    )

    return problems
