import json

import pytest

from pipeline_healer.pipeline.consumers.readers import (
    EventReadError,
    IngestionStats,
    aggregate_stats,
    ingest_batch,
    ingest_batches,
    ingest_events,
)
from pipeline_healer.pipeline.producers.output import render_events, write_event_batches
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events


def make_events(count: int = 3, seed: int = 1) -> list[dict]:
    return list(generate_ride_events(count, seed=seed))


def write_jsonl(path, events: list[dict]):
    path.write_text(render_events(events, "jsonl"), encoding="utf-8")
    return path


def test_ingest_valid_file_reports_statistics(tmp_path) -> None:
    events = make_events(5)
    path = write_jsonl(tmp_path / "events.jsonl", events)

    result = ingest_events(path)

    assert result.events == tuple(events)
    assert result.invalid == ()
    assert result.batch_id is None
    assert result.stats == IngestionStats(total=5, valid=5, invalid=0, malformed=0, duplicates=0)
    assert result.stats.as_dict() == {
        "total": 5,
        "valid": 5,
        "invalid": 0,
        "malformed": 0,
        "duplicates": 0,
    }


def test_ingest_separates_every_kind_of_invalid_record(tmp_path) -> None:
    events = make_events(2)
    bad_event = dict(events[1])
    bad_event["event_id"] = "not-an-event-id"

    path = tmp_path / "mixed.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(events[0], ensure_ascii=False),
                "{broken",
                "[1, 2, 3]",
                json.dumps(bad_event, ensure_ascii=False),
                json.dumps(events[0], ensure_ascii=False),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = ingest_events(path)

    assert [event["event_id"] for event in result.events] == [events[0]["event_id"]]
    assert [item.reason for item in result.invalid] == [
        "malformed_json",
        "not_an_object",
        "invalid_event",
        "duplicate_event_id",
    ]
    assert [item.position for item in result.invalid] == [2, 3, 4, 5]
    assert result.invalid[0].raw == "{broken"
    assert result.invalid[0].source == path
    assert result.stats == IngestionStats(total=5, valid=1, invalid=4, malformed=1, duplicates=1)


def test_ingest_json_array_separates_invalid_entries(tmp_path) -> None:
    events = make_events(2)
    bad_event = dict(events[0])
    bad_event["event_type"] = "unknown_type"

    path = tmp_path / "events.json"
    path.write_text(
        json.dumps([events[0], bad_event, "not-an-object"], ensure_ascii=False),
        encoding="utf-8",
    )

    result = ingest_events(path)

    assert len(result.events) == 1
    assert [item.reason for item in result.invalid] == ["invalid_event", "not_an_object"]
    assert result.stats.total == 3
    assert result.stats.valid == 1
    assert result.stats.invalid == 2


def test_ingest_missing_file_and_bad_format_fail_fast(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        ingest_events(tmp_path / "missing.jsonl")

    path = tmp_path / "events.txt"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(EventReadError, match="unsupported format"):
        ingest_events(path)


def test_ingest_batch_sets_and_validates_the_batch_id(tmp_path) -> None:
    events = make_events(3)
    write_event_batches(events, 3, tmp_path)

    result = ingest_batch(tmp_path / "batch_001.jsonl")
    assert result.batch_id == "batch_001"
    assert result.stats.valid == 3

    with pytest.raises(EventReadError, match="batch id"):
        ingest_batch(tmp_path / "batch_001.jsonl", batch_id="events")


def test_ingest_batches_tracks_ids_across_batches(tmp_path) -> None:
    events = make_events(4)
    write_event_batches(events, 2, tmp_path)

    second = tmp_path / "batch_002.jsonl"
    lines = second.read_text(encoding="utf-8").splitlines()
    first = json.loads(lines[0])
    first["event_id"] = events[0]["event_id"]
    lines[0] = json.dumps(first, ensure_ascii=False)
    second.write_text("\n".join(lines) + "\n", encoding="utf-8")

    results = list(ingest_batches(tmp_path))

    assert [result.batch_id for result in results] == ["batch_001", "batch_002"]
    assert [result.stats.valid for result in results] == [2, 1]
    assert results[1].invalid[0].reason == "duplicate_event_id"
    assert "first seen in batch_001" in results[1].invalid[0].detail
    assert aggregate_stats(results) == IngestionStats(
        total=4, valid=3, invalid=1, malformed=0, duplicates=1
    )


def test_aggregate_stats_of_empty_input() -> None:
    assert aggregate_stats([]) == IngestionStats(
        total=0, valid=0, invalid=0, malformed=0, duplicates=0
    )
