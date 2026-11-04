import json

from pipeline_healer.pipeline.producers.event_types import (
    ORDER_EVENT_TYPES,
    PAYMENT_EVENT_TYPES,
    RIDE_EVENT_TYPES,
    TOPIC_BY_EVENT_TYPE,
    EventType,
)
from pipeline_healer.pipeline.producers.order_events import EVENT_TYPES as ORDER_PRODUCER_TYPES
from pipeline_healer.pipeline.producers.ride_events import EVENT_TYPES as RIDE_PRODUCER_TYPES


def test_catalogue_matches_the_contract() -> None:
    assert RIDE_EVENT_TYPES == ("ride_requested", "ride_completed", "ride_cancelled")
    assert ORDER_EVENT_TYPES == ("order_placed", "order_cancelled")
    assert PAYMENT_EVENT_TYPES == ("payment_captured", "payment_failed")


def test_every_event_type_is_assigned_to_a_topic() -> None:
    assert set(TOPIC_BY_EVENT_TYPE) == set(EventType)


def test_event_types_map_to_the_declared_topics() -> None:
    assert {TOPIC_BY_EVENT_TYPE[EventType(value)] for value in RIDE_EVENT_TYPES} == {"ride_events"}
    assert {TOPIC_BY_EVENT_TYPE[EventType(value)] for value in ORDER_EVENT_TYPES} == {
        "order_events"
    }
    assert {TOPIC_BY_EVENT_TYPE[EventType(value)] for value in PAYMENT_EVENT_TYPES} == {
        "payment_events"
    }


def test_producers_expose_the_catalogue() -> None:
    assert RIDE_PRODUCER_TYPES == RIDE_EVENT_TYPES
    assert ORDER_PRODUCER_TYPES == ORDER_EVENT_TYPES


def test_event_types_serialize_as_plain_strings() -> None:
    assert (
        json.dumps({"event_type": EventType.RIDE_COMPLETED}) == '{"event_type": "ride_completed"}'
    )
