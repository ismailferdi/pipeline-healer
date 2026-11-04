from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pipeline_healer.pipeline.producers.order_events import generate_order_events
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events
from pipeline_healer.pipeline.transforms.errors import TransformError
from pipeline_healer.pipeline.transforms.output import to_output_row, transform_events

PROCESSED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

RIDE_COLUMNS = {
    "event_id",
    "event_type",
    "occurred_at",
    "processed_at",
    "schema_version",
    "source_topic",
    "batch_id",
    "ride_id",
    "user_id",
    "driver_id",
    "city",
    "distance_km",
    "duration_min",
    "fare_amount",
    "currency",
    "payment_method",
}

ORDER_COLUMNS = {
    "event_id",
    "event_type",
    "occurred_at",
    "processed_at",
    "schema_version",
    "source_topic",
    "batch_id",
    "order_id",
    "user_id",
    "merchant_id",
    "city",
    "item_count",
    "total_amount",
    "currency",
}


def ride_of(event_type: str) -> dict:
    return next(
        event for event in generate_ride_events(300, seed=5) if event["event_type"] == event_type
    )


def order_of(event_type: str) -> dict:
    return next(
        event for event in generate_order_events(300, seed=5) if event["event_type"] == event_type
    )


def payment_event() -> dict:
    return {
        "event_id": "evt_300001",
        "event_type": "payment_captured",
        "schema_version": "1.4.0",
        "occurred_at": "2026-01-01T00:10:00Z",
        "produced_at": "2026-01-01T00:10:01Z",
        "producer": "payment-service",
        "payload": {
            "payment_id": "payment_00042",
            "event_id": "evt_100001",
            "amount": 1500.0,
            "currency": "DZD",
            "method": "بطاقة",
        },
    }


def test_ride_row_matches_the_canonical_columns() -> None:
    event = ride_of("ride_completed")

    row = to_output_row(
        event,
        batch_id="batch_001",
        processed_at=PROCESSED_AT,
        source_topic="ride_events",
    )

    assert set(row) == RIDE_COLUMNS
    assert row["event_id"] == event["event_id"]
    assert row["batch_id"] == "batch_001"
    assert row["source_topic"] == "ride_events"
    assert row["occurred_at"].tzinfo is UTC
    assert row["processed_at"] == PROCESSED_AT
    assert isinstance(row["fare_amount"], Decimal)
    assert row["currency"] == "DZD"


def test_requested_ride_row_leaves_unknown_fields_as_none() -> None:
    row = to_output_row(
        ride_of("ride_requested"),
        batch_id="batch_001",
        processed_at=PROCESSED_AT,
        source_topic="ride_events",
    )

    assert row["driver_id"] is None
    assert row["fare_amount"] is None
    assert row["distance_km"] is None


def test_order_row_computes_item_count() -> None:
    event = order_of("order_placed")
    payload = event["payload"]

    row = to_output_row(
        event,
        batch_id="batch_002",
        processed_at=PROCESSED_AT,
        source_topic="order_events",
    )

    assert set(row) == ORDER_COLUMNS
    assert row["order_id"] == payload["order_id"]
    assert row["item_count"] == sum(item["quantity"] for item in payload["items"])
    assert isinstance(row["total_amount"], Decimal)


def test_cancelled_order_row_has_none_for_absent_fields() -> None:
    row = to_output_row(
        order_of("order_cancelled"),
        batch_id="batch_002",
        processed_at=PROCESSED_AT,
        source_topic="order_events",
    )

    assert row["user_id"] is None
    assert row["city"] is None
    assert row["item_count"] is None
    assert row["total_amount"] is None


def test_payment_events_have_no_canonical_output_row() -> None:
    with pytest.raises(TransformError, match="no canonical output row"):
        to_output_row(
            payment_event(),
            batch_id="batch_003",
            processed_at=PROCESSED_AT,
            source_topic="payment_events",
        )


def test_output_row_validates_provenance() -> None:
    event = ride_of("ride_completed")

    with pytest.raises(TransformError, match="invalid batch id"):
        to_output_row(
            event, batch_id="not-a-batch", processed_at=PROCESSED_AT, source_topic="ride_events"
        )
    with pytest.raises(TransformError, match="source_topic"):
        to_output_row(event, batch_id="batch_001", processed_at=PROCESSED_AT, source_topic="  ")


def test_transform_events_maps_a_batch() -> None:
    events = list(generate_ride_events(5, seed=3))

    rows = transform_events(
        events,
        batch_id="batch_004",
        processed_at=PROCESSED_AT,
        source_topic="ride_events",
    )

    assert len(rows) == 5
    assert all(row["batch_id"] == "batch_004" for row in rows)
    assert [row["event_id"] for row in rows] == [event["event_id"] for event in events]
