from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pipeline_healer.pipeline.producers.order_events import generate_order_events
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events
from pipeline_healer.pipeline.transforms.cleaning import clean_event, clean_events
from pipeline_healer.pipeline.transforms.dedupe import dedupe_events
from pipeline_healer.pipeline.transforms.errors import TransformError
from pipeline_healer.pipeline.validation import RecordValidationError


def ride_of(event_type: str) -> dict:
    return next(
        event for event in generate_ride_events(300, seed=5) if event["event_type"] == event_type
    )


def order_of(event_type: str) -> dict:
    return next(
        event for event in generate_order_events(300, seed=5) if event["event_type"] == event_type
    )


def test_clean_event_normalizes_values_without_mutating_input() -> None:
    event = ride_of("ride_completed")
    original = deepcopy(event)

    cleaned = clean_event(event)

    assert event == original
    assert isinstance(cleaned["occurred_at"], datetime)
    assert cleaned["occurred_at"].tzinfo is UTC
    fare = cleaned["payload"]["fare_amount"]
    assert isinstance(fare, Decimal)
    assert fare == fare.quantize(Decimal("0.01"))
    assert isinstance(cleaned["payload"]["duration_min"], int)
    assert cleaned["payload"]["currency"] == "DZD"


def test_clean_event_trims_strings_normalizes_nulls_and_extra_keys() -> None:
    event = ride_of("ride_requested")
    event["payload"]["city"] = "  الجزائر  "
    event["payload"]["vendorNote"] = " N/A "

    cleaned = clean_event(event)

    assert cleaned["payload"]["city"] == "الجزائر"
    assert cleaned["payload"]["vendor_note"] is None


def test_clean_event_rejects_invalid_events() -> None:
    event = ride_of("ride_requested")
    event["event_id"] = "bad-id"

    with pytest.raises(RecordValidationError):
        clean_event(event)


def test_clean_events_is_lazy_and_preserves_order() -> None:
    events = [ride_of("ride_requested"), ride_of("ride_completed")]

    cleaned = list(clean_events(events))

    assert [item["event_id"] for item in cleaned] == [item["event_id"] for item in events]


def test_dedupe_keeps_first_occurrence_and_collects_duplicates() -> None:
    events = list(generate_ride_events(3, seed=9))
    stream = [events[0], events[1], events[0], events[2], events[1]]

    result = dedupe_events(stream)

    assert result.unique == tuple(events)
    assert result.duplicates == (events[0], events[1])
    assert result.duplicate_count == 2


def test_dedupe_accepts_a_custom_key() -> None:
    events = list(generate_ride_events(3, seed=9))

    result = dedupe_events(events, key=lambda event: event["payload"]["user_id"])

    assert len(result.unique) >= 1
    assert len(result.unique) + len(result.duplicates) == 3


def test_dedupe_requires_the_key_to_exist() -> None:
    with pytest.raises(TransformError, match="dedupe key"):
        dedupe_events([{"event_id": "evt_1"}], key="missing")


def test_order_event_cleaning_uses_decimal_money() -> None:
    cleaned = clean_event(order_of("order_placed"))

    total = cleaned["payload"]["total_amount"]
    assert isinstance(total, Decimal)
    assert total == total.quantize(Decimal("0.01"))
    assert isinstance(cleaned["payload"]["items"][0]["quantity"], int)
