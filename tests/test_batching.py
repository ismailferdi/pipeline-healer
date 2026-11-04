import json

import pytest

from pipeline_healer.pipeline.batching import chunk
from pipeline_healer.pipeline.producers.identifiers import BATCH_ID_PATTERN, batch_id
from pipeline_healer.pipeline.producers.order_events import main as order_main
from pipeline_healer.pipeline.producers.ride_events import main as ride_main
from pipeline_healer.pipeline.validation import validate_records


def read_batch_files(directory) -> list[list[dict]]:
    return [
        [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for path in sorted(directory.glob("batch_*.jsonl"))
    ]


def test_chunk_splits_into_equal_batches() -> None:
    assert list(chunk(range(6), 3)) == [[0, 1, 2], [3, 4, 5]]


def test_chunk_keeps_a_partial_final_batch() -> None:
    assert list(chunk(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]


def test_chunk_with_size_one() -> None:
    assert list(chunk(["a", "b"], 1)) == [["a"], ["b"]]


def test_chunk_of_empty_input_yields_nothing() -> None:
    assert list(chunk([], 5)) == []


@pytest.mark.parametrize("size", [0, -1])
def test_chunk_rejects_non_positive_sizes(size: int) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        list(chunk(range(3), size))


def test_batch_id_format() -> None:
    assert batch_id(104) == "batch_104"
    assert BATCH_ID_PATTERN.match(batch_id(1))
    assert BATCH_ID_PATTERN.match(batch_id(123456))


def test_ride_cli_writes_configurable_batches(tmp_path) -> None:
    assert (
        ride_main(["--count", "7", "--seed", "1", "--batch-size", "3", "--out", str(tmp_path)]) == 0
    )

    batches = read_batch_files(tmp_path)

    assert [len(batch) for batch in batches] == [3, 3, 1]
    assert [path.name for path in sorted(tmp_path.glob("batch_*.jsonl"))] == [
        "batch_001.jsonl",
        "batch_002.jsonl",
        "batch_003.jsonl",
    ]
    validate_records(event for batch in batches for event in batch)


def test_order_cli_writes_configurable_batches(tmp_path) -> None:
    assert (
        order_main(["--count", "5", "--seed", "2", "--batch-size", "2", "--out", str(tmp_path)])
        == 0
    )

    batches = read_batch_files(tmp_path)

    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert all(event["producer"] == "order-service" for batch in batches for event in batch)


def test_batch_size_requires_an_output_directory() -> None:
    with pytest.raises(SystemExit):
        ride_main(["--count", "3", "--batch-size", "2"])


def test_batch_size_must_be_positive(tmp_path) -> None:
    with pytest.raises(SystemExit):
        ride_main(["--count", "3", "--batch-size", "0", "--out", str(tmp_path)])


def test_ride_cli_writes_json_batches(tmp_path) -> None:
    args = ["--count", "7", "--seed", "1", "--batch-size", "3", "--format", "json"]
    assert ride_main([*args, "--out", str(tmp_path)]) == 0

    files = sorted(tmp_path.glob("batch_*.json"))
    assert [path.name for path in files] == [
        "batch_001.json",
        "batch_002.json",
        "batch_003.json",
    ]

    batches = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert [len(batch) for batch in batches] == [3, 3, 1]
    assert all(isinstance(batch, list) for batch in batches)
    validate_records(event for batch in batches for event in batch)


def test_single_file_json_output(tmp_path) -> None:
    target = tmp_path / "events.json"

    assert ride_main(["--count", "4", "--seed", "2", "--format", "json", "--out", str(target)]) == 0

    events = json.loads(target.read_text(encoding="utf-8"))
    assert len(events) == 4
    assert isinstance(events, list)
    validate_records(events)


def test_stdout_json_output(capsys) -> None:
    assert ride_main(["--count", "2", "--seed", "2", "--format", "json"]) == 0

    events = json.loads(capsys.readouterr().out)
    assert len(events) == 2


def test_unknown_format_is_rejected() -> None:
    with pytest.raises(SystemExit):
        ride_main(["--count", "2", "--format", "yaml"])
