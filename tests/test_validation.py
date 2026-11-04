from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from pipeline_healer.pipeline.producers.order_events import generate_order_events
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events
from pipeline_healer.pipeline.producers.timestamps import rfc3339, utc_now
from pipeline_healer.pipeline.validation import (
    RecordValidationError,
    validate_event,
    validate_records,
)

START = datetime(2026, 11, 3, 10, 0, tzinfo=UTC)


def generated_records(count: int = 200) -> list[dict]:
    rides = list(generate_ride_events(count, seed=5, start_at=START))
    orders = list(generate_order_events(count, seed=5, start_at=START))
    return [*rides, *orders]


def sample(event_type: str) -> dict:
    return deepcopy(
        next(event for event in generated_records(400) if event["event_type"] == event_type)
    )


def payment_event(event_type: str = "payment_captured") -> dict:
    payload = {
        "payment_id": "payment_00042",
        "event_id": "evt_100001",
        "amount": 38.5,
        "currency": "DZD",
        "method": "بطاقة",
    }
    if event_type == "payment_failed":
        payload = {
            "payment_id": "payment_00043",
            "event_id": "evt_100002",
            "amount": 38.5,
            "currency": "DZD",
            "failure_code": "insufficient_funds",
        }
    return {
        "event_id": "evt_300001",
        "event_type": event_type,
        "schema_version": "1.4.0",
        "occurred_at": rfc3339(START),
        "produced_at": rfc3339(START + timedelta(seconds=1)),
        "producer": "payment-service",
        "payload": payload,
    }


def test_all_generated_records_are_valid() -> None:
    records = generated_records()

    assert len(records) == 400
    assert all(validate_event(record) == [] for record in records)
    validate_records(records)


def test_generated_event_ids_are_unique_across_streams() -> None:
    records = generated_records()

    assert len({record["event_id"] for record in records}) == len(records)


def test_payment_records_validate() -> None:
    assert validate_event(payment_event()) == []
    assert validate_event(payment_event("payment_failed")) == []


def test_non_object_record_is_rejected() -> None:
    assert validate_event(["not", "a", "record"]) == ["record: must be a JSON object"]


def test_missing_envelope_field_is_reported() -> None:
    record = sample("ride_completed")
    del record["producer"]

    assert validate_event(record) == ["producer: missing"]


def test_unknown_event_type_is_rejected() -> None:
    record = sample("ride_completed")
    record["event_type"] = "ride_teleported"

    assert "event_type: unknown value" in " ".join(validate_event(record))


def test_malformed_identifier_is_rejected() -> None:
    record = sample("ride_completed")
    record["event_id"] = "ride_100001"

    assert any("event_id: does not match" in problem for problem in validate_event(record))


def test_missing_payload_field_is_rejected() -> None:
    record = sample("ride_completed")
    del record["payload"]["driver_id"]

    assert any("payload.driver_id" in problem for problem in validate_event(record))


def test_non_utc_timestamp_is_rejected() -> None:
    record = sample("ride_requested")
    record["occurred_at"] = "2026-11-03T10:00:00+03:00"

    assert any("occurred_at" in problem for problem in validate_event(record))


def test_produced_before_occurred_is_rejected() -> None:
    record = sample("ride_requested")
    occurred = datetime.fromisoformat(record["occurred_at"].replace("Z", "+00:00"))
    record["produced_at"] = rfc3339(occurred - timedelta(seconds=5))

    assert "produced_at: must not precede occurred_at" in validate_event(record)


def test_future_occurred_at_is_rejected() -> None:
    record = sample("ride_requested")
    record["occurred_at"] = rfc3339(utc_now() + timedelta(minutes=10))

    assert any("in the future" in problem for problem in validate_event(record))


def test_order_total_must_match_items() -> None:
    record = sample("order_placed")
    record["payload"]["total_amount"] += 1

    assert "payload.total_amount: does not match items" in validate_event(record)


def test_empty_order_items_are_rejected() -> None:
    record = sample("order_placed")
    record["payload"]["items"] = []

    assert any("payload.items" in problem for problem in validate_event(record))


def test_unknown_currency_is_rejected() -> None:
    record = sample("ride_completed")
    record["payload"]["currency"] = "XYZ"

    assert any("payload.currency" in problem for problem in validate_event(record))


def test_duplicate_event_ids_in_a_batch_are_rejected() -> None:
    record = sample("ride_completed")

    with pytest.raises(RecordValidationError, match="duplicate"):
        validate_records([record, deepcopy(record)])
