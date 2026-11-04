import pytest

from pipeline_healer.pipeline.producers.identifiers import (
    DRIVER_ID_PATTERN,
    EVENT_ID_PATTERN,
    MERCHANT_ID_PATTERN,
    ORDER_ID_PATTERN,
    RIDE_ID_PATTERN,
    USER_ID_PATTERN,
    driver_id,
    event_id,
    merchant_id,
    order_id,
    ride_id,
    user_id,
)
from pipeline_healer.pipeline.producers.order_events import (
    generate_order_events as generate_orders,
)
from pipeline_healer.pipeline.producers.ride_events import (
    generate_ride_events as generate_rides,
)


@pytest.mark.parametrize(
    ("factory", "value", "expected", "pattern"),
    [
        (event_id, 1, "evt_000001", EVENT_ID_PATTERN),
        (event_id, 1234567, "evt_1234567", EVENT_ID_PATTERN),
        (ride_id, 88213, "ride_88213", RIDE_ID_PATTERN),
        (order_id, 55210, "order_55210", ORDER_ID_PATTERN),
        (user_id, 52, "user_0052", USER_ID_PATTERN),
        (driver_id, 917, "driver_917", DRIVER_ID_PATTERN),
        (merchant_id, 331, "merchant_331", MERCHANT_ID_PATTERN),
    ],
)
def test_identifier_formats(factory, value, expected, pattern) -> None:
    result = factory(value)

    assert result == expected
    assert pattern.match(result)


@pytest.mark.parametrize("factory", [event_id, ride_id, order_id, user_id, driver_id, merchant_id])
def test_identifiers_reject_non_positive_values(factory) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        factory(0)

    with pytest.raises(ValueError, match="positive integer"):
        factory(-1)


def test_event_ids_are_unique_across_a_sequence() -> None:
    ids = [event_id(sequence) for sequence in range(1, 1001)]

    assert len(ids) == len(set(ids))


def test_ride_producer_ids_match_the_contract() -> None:
    for event in generate_rides(50, seed=3):
        payload = event["payload"]

        assert EVENT_ID_PATTERN.match(event["event_id"])
        assert RIDE_ID_PATTERN.match(payload["ride_id"])
        assert USER_ID_PATTERN.match(payload["user_id"])

        if "driver_id" in payload:
            assert DRIVER_ID_PATTERN.match(payload["driver_id"])


def test_order_producer_ids_match_the_contract() -> None:
    for event in generate_orders(50, seed=3):
        payload = event["payload"]

        assert EVENT_ID_PATTERN.match(event["event_id"])
        assert ORDER_ID_PATTERN.match(payload["order_id"])

        if event["event_type"] == "order_placed":
            assert USER_ID_PATTERN.match(payload["user_id"])
            assert MERCHANT_ID_PATTERN.match(payload["merchant_id"])
