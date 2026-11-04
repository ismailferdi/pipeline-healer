import json
from pathlib import Path

import pytest

from pipeline_healer.pipeline.consumers.readers import (
    EventBatch,
    EventReadError,
    read_batch,
    read_event_batches,
    read_events,
)
from pipeline_healer.pipeline.producers.output import render_events, write_event_batches
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events
from pipeline_healer.pipeline.validation import RecordValidationError


def make_events(count: int = 4, seed: int = 1) -> list[dict]:
    return list(generate_ride_events(count, seed=seed))


def write_file(path, events: list[dict], output_format: str = "jsonl"):
    path.write_text(render_events(events, output_format), encoding="utf-8")
    return path


def test_reads_jsonl(tmp_path) -> None:
    events = make_events()
    path = write_file(tmp_path / "events.jsonl", events)

    assert list(read_events(path)) == events


def test_reads_json_array(tmp_path) -> None:
    events = make_events()
    path = write_file(tmp_path / "events.json", events, "json")

    assert list(read_events(path)) == events


def test_blank_lines_are_skipped(tmp_path) -> None:
    events = make_events(2)
    body = render_events(events)
    path = tmp_path / "events.jsonl"
    path.write_text(f"\n{body}\n\n", encoding="utf-8")

    assert list(read_events(path)) == events


def test_empty_file_yields_no_events(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("", encoding="utf-8")

    assert list(read_events(path)) == []


def test_unsupported_extension_is_rejected(tmp_path) -> None:
    path = tmp_path / "events.txt"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(EventReadError, match="unsupported format"):
        list(read_events(path))


def test_missing_file_is_reported(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        list(read_events(tmp_path / "missing.jsonl"))


def test_malformed_json_reports_position(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"not": "closed"\n', encoding="utf-8")

    with pytest.raises(EventReadError, match=r"events\.jsonl:1"):
        list(read_events(path))


def test_non_object_record_is_rejected(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text("[1, 2, 3]\n", encoding="utf-8")

    with pytest.raises(EventReadError, match="expected an event object"):
        list(read_events(path))


def test_invalid_record_reports_path_and_position(tmp_path) -> None:
    events = make_events(2)
    events[1]["event_id"] = "not-an-event-id"
    path = write_file(tmp_path / "events.jsonl", events)

    with pytest.raises(RecordValidationError, match=r"events\.jsonl:2: event_id"):
        list(read_events(path))


def test_duplicate_event_ids_in_one_file_are_rejected(tmp_path) -> None:
    events = make_events(1)
    path = write_file(tmp_path / "events.jsonl", [events[0], events[0]])

    with pytest.raises(RecordValidationError, match="duplicate event_id"):
        list(read_events(path))


def test_batch_reader_exposes_batch_metadata(tmp_path) -> None:
    events = make_events(4)
    write_event_batches(events, 2, tmp_path)

    batches = list(read_event_batches(tmp_path))

    assert [batch.batch_id for batch in batches] == ["batch_001", "batch_002"]
    assert [len(batch.events) for batch in batches] == [2, 2]
    assert all(isinstance(batch, EventBatch) for batch in batches)
    assert batches[0].source.name == "batch_001.jsonl"


def test_read_batch_rejects_mismatched_batch_file_name(tmp_path) -> None:
    path = write_file(tmp_path / "events.jsonl", make_events(2))

    with pytest.raises(EventReadError, match="batch id"):
        read_batch(path)


def test_duplicate_event_ids_across_batches_are_rejected(tmp_path) -> None:
    events = make_events(4)
    write_event_batches(events, 2, tmp_path)

    second = tmp_path / "batch_002.jsonl"
    lines = second.read_text(encoding="utf-8").splitlines()
    first_event = json.loads(lines[0])
    first_event["event_id"] = events[0]["event_id"]
    lines[0] = json.dumps(first_event, ensure_ascii=False)
    second.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(RecordValidationError, match="duplicate event_id"):
        list(read_event_batches(tmp_path))


def test_missing_batch_directory_is_reported(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        list(read_event_batches(tmp_path / "missing"))


def test_reads_the_committed_sample_datasets() -> None:
    samples = Path(__file__).resolve().parents[1] / "data" / "samples"

    rides = list(read_events(samples / "ride_events.jsonl"))
    orders = list(read_events(samples / "order_events.jsonl"))

    assert len(rides) == 5000
    assert len(orders) == 5000
    assert len({event["event_id"] for event in rides + orders}) == 10000
