import json
import re
from datetime import UTC, datetime

import pytest

from pipeline_healer.pipeline.producers.ride_events import (
    CURRENCIES,
    EVENT_TYPES,
    SCHEMA_VERSION,
    generate_ride_events,
    main,
)

START = datetime(2026, 11, 3, 10, 0, tzinfo=UTC)
EVENT_ID_PATTERN = re.compile(r"^evt_[0-9]{6,}$")

PAYLOAD_FIELDS = {
    "ride_requested": {"ride_id", "user_id", "city", "pickup_area", "requested_at"},
    "ride_completed": {
        "ride_id",
        "user_id",
        "driver_id",
        "city",
        "pickup_area",
        "dropoff_area",
        "distance_km",
        "duration_min",
        "fare_amount",
        "currency",
        "payment_method",
    },
    "ride_cancelled": {"ride_id", "user_id", "city", "reason", "cancelled_at"},
}


def generate(count: int = 200, *, seed: int = 7) -> list[dict]:
    return list(generate_ride_events(count, seed=seed, start_at=START))


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_envelope_is_complete() -> None:
    for event in generate(50):
        assert set(event) == {
            "event_id",
            "event_type",
            "schema_version",
            "occurred_at",
            "produced_at",
            "producer",
            "payload",
        }
        assert event["schema_version"] == SCHEMA_VERSION
        assert event["producer"] == "ride-service"
        assert event["event_type"] in EVENT_TYPES
        assert isinstance(event["payload"], dict)


def test_event_ids_are_unique_and_well_formed() -> None:
    events = generate(200)
    ids = [event["event_id"] for event in events]

    assert len(ids) == len(set(ids))
    assert all(EVENT_ID_PATTERN.match(event_id) for event_id in ids)


def test_all_ride_event_types_are_generated() -> None:
    assert {event["event_type"] for event in generate(200)} == set(EVENT_TYPES)


def test_payload_matches_event_type() -> None:
    events = generate(200)

    for event in events:
        event_type = event["event_type"]
        payload = event["payload"]
        assert set(payload) == PAYLOAD_FIELDS[event_type]
        assert payload["ride_id"].startswith("ride_")
        assert payload["city"]

        if event_type == "ride_completed":
            assert payload["distance_km"] > 0
            assert payload["duration_min"] >= 1
            assert payload["fare_amount"] > 0
            assert payload["currency"] in CURRENCIES
            assert payload["pickup_area"] != payload["dropoff_area"]


def test_output_is_reproducible_with_a_seed() -> None:
    assert generate(100, seed=42) == generate(100, seed=42)


def test_different_seeds_produce_different_streams() -> None:
    assert generate(20, seed=1) != generate(20, seed=2)


def test_timestamps_are_utc_and_ordered() -> None:
    events = generate(100)
    occurred = [parse_timestamp(event["occurred_at"]) for event in events]
    produced = [parse_timestamp(event["produced_at"]) for event in events]

    assert all(moment.tzinfo is UTC for moment in occurred)
    assert occurred == sorted(occurred)
    assert all(start <= end for start, end in zip(occurred, produced, strict=True))
    assert all(
        (end - start).total_seconds() <= 2 for start, end in zip(occurred, produced, strict=True)
    )


def test_negative_count_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        list(generate_ride_events(-1))


def test_cli_writes_json_lines(tmp_path) -> None:
    target = tmp_path / "events.jsonl"

    assert main(["--count", "5", "--seed", "3", "--out", str(target)]) == 0

    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5
    assert all(json.loads(line)["event_type"] in EVENT_TYPES for line in lines)
