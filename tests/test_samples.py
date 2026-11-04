import hashlib
import json
from pathlib import Path

import pytest

from pipeline_healer.pipeline.producers.samples import (
    SAMPLE_COUNT,
    SAMPLE_SEED,
    generate_samples,
    main,
)
from pipeline_healer.pipeline.validation import validate_records

REPO_ROOT = Path(__file__).resolve().parents[1]
COMMITTED = REPO_ROOT / "data" / "samples"
FILES = ("ride_events.jsonl", "order_events.jsonl", "manifest.json")


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_generate_samples_writes_valid_datasets(tmp_path) -> None:
    manifest = generate_samples(tmp_path, count=20, seed=7)

    assert manifest["seed"] == 7
    assert manifest["count"] == 20
    assert set(manifest["datasets"]) == {"ride_events", "order_events"}

    for metadata in manifest["datasets"].values():
        target = tmp_path / metadata["file"]
        events = load_jsonl(target)

        assert len(events) == 20 == metadata["count"]
        assert metadata["sha256"] == hashlib.sha256(target.read_bytes()).hexdigest()
        validate_records(events)


def test_generation_is_byte_identical(tmp_path) -> None:
    generate_samples(tmp_path / "first", count=10, seed=3)
    generate_samples(tmp_path / "second", count=10, seed=3)

    for name in FILES:
        assert (tmp_path / "first" / name).read_bytes() == (tmp_path / "second" / name).read_bytes()


def test_committed_samples_match_the_generator(tmp_path) -> None:
    generate_samples(tmp_path, count=SAMPLE_COUNT, seed=SAMPLE_SEED)

    for name in FILES:
        assert (tmp_path / name).read_bytes() == (COMMITTED / name).read_bytes(), (
            f"{name} is stale; regenerate with `uv run generate-samples`"
        )


def test_committed_samples_are_valid() -> None:
    for name in ("ride_events.jsonl", "order_events.jsonl"):
        validate_records(load_jsonl(COMMITTED / name))


def test_zero_count_is_rejected(tmp_path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        generate_samples(tmp_path, count=0)


def test_cli_writes_samples(tmp_path) -> None:
    assert main(["--out", str(tmp_path), "--count", "5", "--seed", "1"]) == 0

    assert len(load_jsonl(tmp_path / "ride_events.jsonl")) == 5
    assert len(load_jsonl(tmp_path / "order_events.jsonl")) == 5
    assert (tmp_path / "manifest.json").exists()
