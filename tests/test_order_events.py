import json
import re
from datetime import UTC, datetime

import pytest

from pipeline_healer.pipeline.producers.order_events import (
    CURRENCIES,
    EVENT_TYPES,
    SCHEMA_VERSION,
    generate_order_events,
    main,
)

START = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
EVENT_ID_PATTERN = re.compile(r"^evt_[0-9]{6,}$")

PAYLOAD_FIELDS = {
    "order_placed": {
        "order_id",
        "user_id",
        "merchant_id",
        "city",
        "district",
        "items",
        "total_amount",
        "currency",
        "payment_method",
    },
    "order_cancelled": {"order_id", "reason", "cancelled_at"},
}


def generate(count: int = 200, *, seed: int = 7) -> list[dict]:
    return list(generate_order_events(count, seed=seed, start_at=START))


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
        assert event["producer"] == "order-service"
        assert event["event_type"] in EVENT_TYPES
        assert isinstance(event["payload"], dict)


def test_event_ids_are_unique_and_well_formed() -> None:
    events = generate(200)
    ids = [event["event_id"] for event in events]

    assert len(ids) == len(set(ids))
    assert all(EVENT_ID_PATTERN.match(event_id) for event_id in ids)


def test_all_order_event_types_are_generated() -> None:
    assert {event["event_type"] for event in generate(200)} == set(EVENT_TYPES)


def test_payload_matches_event_type() -> None:
    for event in generate(200):
        event_type = event["event_type"]
        payload = event["payload"]

        assert set(payload) == PAYLOAD_FIELDS[event_type]
        assert payload["order_id"].startswith("order_")

        match event_type:
            case "order_placed":
                assert payload["items"]
                assert payload["total_amount"] > 0
                assert payload["currency"] in CURRENCIES
            case "order_cancelled":
                assert payload["reason"]


def test_placed_order_items_are_consistent() -> None:
    placed = [event["payload"] for event in generate(200) if event["event_type"] == "order_placed"]

    for order in placed:
        skus = [item["sku"] for item in order["items"]]
        assert len(skus) == len(set(skus))
        assert all(item["quantity"] >= 1 for item in order["items"])
        assert all(item["unit_price"] > 0 for item in order["items"])
        expected_total = round(
            sum(item["quantity"] * item["unit_price"] for item in order["items"]), 2
        )
        assert order["total_amount"] == expected_total


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
        list(generate_order_events(-1))


def test_cli_writes_json_lines(tmp_path) -> None:
    target = tmp_path / "orders.jsonl"

    assert main(["--count", "5", "--seed", "3", "--out", str(target)]) == 0

    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5
    assert all(json.loads(line)["producer"] == "order-service" for line in lines)
