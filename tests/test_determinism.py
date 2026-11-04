import argparse
import json
from datetime import UTC, datetime, timedelta

import pytest

from pipeline_healer.pipeline.producers.order_events import (
    generate_order_events,
)
from pipeline_healer.pipeline.producers.order_events import (
    main as order_main,
)
from pipeline_healer.pipeline.producers.output import parse_start_at
from pipeline_healer.pipeline.producers.ride_events import (
    generate_ride_events,
)
from pipeline_healer.pipeline.producers.ride_events import (
    main as ride_main,
)
from pipeline_healer.pipeline.producers.timestamps import (
    DETERMINISTIC_START,
    parse_rfc3339,
    resolve_start,
    rfc3339,
    utc_now,
)


def test_seeded_cli_output_is_byte_identical(tmp_path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"

    ride_main(["--count", "20", "--seed", "42", "--out", str(first)])
    ride_main(["--count", "20", "--seed", "42", "--out", str(second)])
    assert first.read_bytes() == second.read_bytes()

    order_first = tmp_path / "orders_a.jsonl"
    order_second = tmp_path / "orders_b.jsonl"
    order_main(["--count", "20", "--seed", "42", "--out", str(order_first)])
    order_main(["--count", "20", "--seed", "42", "--out", str(order_second)])
    assert order_first.read_bytes() == order_second.read_bytes()


def test_seeded_generation_uses_the_deterministic_start() -> None:
    events = list(generate_ride_events(5, seed=7))

    first_occurred = events[0]["occurred_at"]

    assert first_occurred.startswith("2026-01-01T")
    assert first_occurred < rfc3339(DETERMINISTIC_START + timedelta(minutes=1))
    assert [event["event_id"] for event in events] == [
        "evt_100001",
        "evt_100002",
        "evt_100003",
        "evt_100004",
        "evt_100005",
    ]


def test_explicit_start_at_wins_over_the_seeded_default() -> None:
    start = datetime(2025, 5, 1, 12, 0, tzinfo=UTC)

    events = list(generate_order_events(3, seed=1, start_at=start))

    first_occurred = datetime.fromisoformat(events[0]["occurred_at"].replace("Z", "+00:00"))
    assert first_occurred > start
    assert first_occurred <= start + timedelta(seconds=45)


def test_unseeded_start_backdates_the_stream() -> None:
    resolved = resolve_start(start_at=None, seed=None, count=10, max_interval_seconds=30)

    assert abs((utc_now() - resolved) - timedelta(seconds=300)) < timedelta(seconds=5)


def test_parse_rfc3339_accepts_zulu_and_offsets() -> None:
    assert parse_rfc3339("2026-11-03T10:30:00Z") == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)
    assert parse_rfc3339("2026-11-03T13:30:00+03:00") == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "value", ["2026-11-03T10:30:00", "not-a-timestamp", "2026-13-40T10:30:00Z"]
)
def test_parse_rfc3339_rejects_naive_or_malformed_values(value: str) -> None:
    with pytest.raises(ValueError, match="timestamp"):
        parse_rfc3339(value)


def test_parse_start_at_is_an_argparse_type() -> None:
    parsed = parse_start_at("2026-11-03T10:30:00Z")

    assert parsed == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)
    with pytest.raises(argparse.ArgumentTypeError):
        parse_start_at("not-a-timestamp")


def test_cli_start_at_controls_the_window(tmp_path) -> None:
    target = tmp_path / "events.jsonl"
    start = "2025-12-31T23:59:00Z"

    assert (
        ride_main(["--count", "3", "--seed", "3", "--start-at", start, "--out", str(target)]) == 0
    )

    first = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    occurred = datetime.fromisoformat(first["occurred_at"].replace("Z", "+00:00"))
    assert occurred > datetime(2025, 12, 31, 23, 59, tzinfo=UTC)


def test_cli_rejects_an_invalid_start_at() -> None:
    with pytest.raises(SystemExit):
        ride_main(["--count", "3", "--start-at", "2025-12-31"])
