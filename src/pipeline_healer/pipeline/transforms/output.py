"""Output transformation to the canonical warehouse rows.

Rows mirror the cleaned ride/order tables in ``docs/data-contracts.md``: aware UTC
datetimes, exact decimal amounts, and explicit batch/source provenance.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, tzinfo
from typing import Any

from pipeline_healer.pipeline.producers.identifiers import BATCH_ID_PATTERN
from pipeline_healer.pipeline.transforms.cleaning import clean_event
from pipeline_healer.pipeline.transforms.errors import TransformError
from pipeline_healer.pipeline.transforms.timestamps import to_utc


def _base_columns(
    cleaned: dict[str, Any],
    *,
    batch_id: str,
    processed_at: datetime,
    source_topic: str,
) -> dict[str, Any]:
    return {
        "event_id": cleaned["event_id"],
        "event_type": cleaned["event_type"],
        "occurred_at": cleaned["occurred_at"],
        "processed_at": processed_at,
        "schema_version": cleaned["schema_version"],
        "source_topic": source_topic,
        "batch_id": batch_id,
    }


def _ride_row(
    cleaned: dict[str, Any],
    *,
    batch_id: str,
    processed_at: datetime,
    source_topic: str,
) -> dict[str, Any]:
    payload = cleaned["payload"]
    return {
        **_base_columns(
            cleaned, batch_id=batch_id, processed_at=processed_at, source_topic=source_topic
        ),
        "ride_id": payload.get("ride_id"),
        "user_id": payload.get("user_id"),
        "driver_id": payload.get("driver_id"),
        "city": payload.get("city"),
        "distance_km": payload.get("distance_km"),
        "duration_min": payload.get("duration_min"),
        "fare_amount": payload.get("fare_amount"),
        "currency": payload.get("currency"),
        "payment_method": payload.get("payment_method"),
    }


def _order_row(
    cleaned: dict[str, Any],
    *,
    batch_id: str,
    processed_at: datetime,
    source_topic: str,
) -> dict[str, Any]:
    payload = cleaned["payload"]
    items = payload.get("items") or []
    item_count = sum(item["quantity"] for item in items) if items else None
    return {
        **_base_columns(
            cleaned, batch_id=batch_id, processed_at=processed_at, source_topic=source_topic
        ),
        "order_id": payload.get("order_id"),
        "user_id": payload.get("user_id"),
        "merchant_id": payload.get("merchant_id"),
        "city": payload.get("city"),
        "item_count": item_count,
        "total_amount": payload.get("total_amount"),
        "currency": payload.get("currency"),
    }


def to_output_row(
    event: dict[str, Any],
    *,
    batch_id: str,
    processed_at: datetime,
    source_topic: str,
    assume_timezone: str | tzinfo = UTC,
) -> dict[str, Any]:
    """Clean one event and map it to its canonical output row."""
    if not isinstance(batch_id, str) or not BATCH_ID_PATTERN.match(batch_id):
        raise TransformError(f"invalid batch id {batch_id!r}")
    if not isinstance(source_topic, str) or not source_topic.strip():
        raise TransformError("source_topic must be a non-empty string")

    cleaned = clean_event(event, assume_timezone=assume_timezone)
    processed = to_utc(processed_at, field="processed_at", assume_timezone=assume_timezone)
    event_type = cleaned["event_type"]

    if event_type.startswith("ride_"):
        return _ride_row(
            cleaned, batch_id=batch_id, processed_at=processed, source_topic=source_topic
        )
    if event_type.startswith("order_"):
        return _order_row(
            cleaned, batch_id=batch_id, processed_at=processed, source_topic=source_topic
        )

    raise TransformError(f"no canonical output row for event type {event_type!r}")


def transform_events(
    events: Iterable[dict[str, Any]],
    *,
    batch_id: str,
    processed_at: datetime,
    source_topic: str,
    assume_timezone: str | tzinfo = UTC,
) -> list[dict[str, Any]]:
    """Map a batch of events to canonical output rows."""
    return [
        to_output_row(
            event,
            batch_id=batch_id,
            processed_at=processed_at,
            source_topic=source_topic,
            assume_timezone=assume_timezone,
        )
        for event in events
    ]
