"""Event ingestion: parsing, validation, batching, and statistics.

Two modes are supported:

- **Strict** (:func:`read_events`, :func:`read_batch`): fail on the first
  malformed or invalid record. Use this when a corrupt stream must stop the run.
- **Tolerant** (:func:`ingest_events`, :func:`ingest_batch`,
  :func:`ingest_batches`): separate valid events from invalid ones, keep
  per-file statistics, and never abort on a bad record.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_healer.pipeline.producers.identifiers import BATCH_ID_PATTERN
from pipeline_healer.pipeline.validation import RecordValidationError, validate_event

SUPPORTED_SUFFIXES = (".jsonl", ".json")


class EventReadError(ValueError):
    """Raised when a file cannot be read as a stream of events."""


@dataclass(frozen=True)
class EventBatch:
    """A validated, named group of events read from one file."""

    batch_id: str
    source: Path
    events: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class InvalidRecord:
    """A record that could not be ingested, with enough context to quarantine it."""

    source: Path
    position: int
    reason: str
    detail: str
    raw: str


@dataclass(frozen=True)
class IngestionStats:
    """Counters for one ingestion unit (a file or a batch)."""

    total: int
    valid: int
    invalid: int
    malformed: int
    duplicates: int

    def as_dict(self) -> dict[str, int]:
        return {
            "total": self.total,
            "valid": self.valid,
            "invalid": self.invalid,
            "malformed": self.malformed,
            "duplicates": self.duplicates,
        }


@dataclass(frozen=True)
class IngestionResult:
    """Valid and invalid records from one ingestion unit."""

    source: Path
    batch_id: str | None
    events: tuple[dict[str, Any], ...]
    invalid: tuple[InvalidRecord, ...]
    stats: IngestionStats


@dataclass(frozen=True)
class _RawRecord:
    position: int
    value: Any = None
    error: str | None = None
    raw: str = ""


def _require_supported(path: Path) -> None:
    if path.suffix.lower() not in SUPPORTED_SUFFIXES:
        raise EventReadError(
            f"{path}: unsupported format {path.suffix!r}; expected one of {SUPPORTED_SUFFIXES}"
        )
    if not path.is_file():
        raise FileNotFoundError(path)


def _iter_jsonl_raw(path: Path) -> Iterator[_RawRecord]:
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                yield _RawRecord(
                    position=number,
                    error=f"invalid JSON: {exc.msg} (column {exc.colno})",
                    raw=line.rstrip("\n"),
                )
                continue
            yield _RawRecord(position=number, value=value, raw=line.rstrip("\n"))


def _iter_json_raw(path: Path) -> Iterator[_RawRecord]:
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        yield _RawRecord(position=1, error=f"invalid JSON: {exc.msg}", raw=text)
        return

    if not isinstance(data, list):
        yield _RawRecord(position=1, error="expected a JSON array of events", raw=text)
        return

    for position, value in enumerate(data, start=1):
        yield _RawRecord(
            position=position,
            value=value,
            raw=json.dumps(value, ensure_ascii=False),
        )


def _iter_raw_records(path: Path) -> Iterator[_RawRecord]:
    if path.suffix.lower() == ".jsonl":
        yield from _iter_jsonl_raw(path)
    else:
        yield from _iter_json_raw(path)


def read_events(path: Path) -> Iterator[dict[str, Any]]:
    """Strictly stream validated events from a ``.jsonl`` or ``.json`` file.

    Malformed JSON raises :class:`EventReadError`; contract violations and
    duplicate ``event_id`` values raise :class:`RecordValidationError`. Messages
    include the file and position.
    """
    path = Path(path)
    _require_supported(path)

    seen: set[str] = set()
    for record in _iter_raw_records(path):
        if record.error is not None:
            raise EventReadError(f"{path}:{record.position}: {record.error}")
        if not isinstance(record.value, dict):
            raise EventReadError(f"{path}:{record.position}: expected an event object")

        problems = validate_event(record.value)
        if problems:
            raise RecordValidationError(
                [f"{path}:{record.position}: {problem}" for problem in problems]
            )

        event_id = record.value["event_id"]
        if event_id in seen:
            raise RecordValidationError(
                [f"{path}:{record.position}: duplicate event_id {event_id}"]
            )
        seen.add(event_id)

        yield record.value


def read_batch(path: Path, batch_id: str | None = None) -> EventBatch:
    """Strictly read one batch file and return its validated events."""
    path = Path(path)
    resolved_id = batch_id or path.stem
    _require_batch_id(path, resolved_id)

    return EventBatch(batch_id=resolved_id, source=path, events=tuple(read_events(path)))


def read_event_batches(directory: Path) -> Iterator[EventBatch]:
    """Strictly read every ``batch_*`` file in ``directory``, sorted by name.

    Duplicate event ids across batches are rejected, since event ids are unique
    across the whole stream even though DQ-002 is checked per batch.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(directory)

    seen: dict[str, str] = {}
    for path in _batch_files(directory):
        batch = read_batch(path)
        for event in batch.events:
            event_id = event["event_id"]
            if event_id in seen:
                raise RecordValidationError(
                    [f"{path}: duplicate event_id {event_id} already seen in {seen[event_id]}"]
                )
            seen[event_id] = batch.batch_id
        yield batch


def _batch_files(directory: Path) -> list[Path]:
    return sorted([*directory.glob("batch_*.jsonl"), *directory.glob("batch_*.json")])


def _require_batch_id(path: Path, batch_id: str) -> None:
    if not BATCH_ID_PATTERN.match(batch_id):
        raise EventReadError(
            f"{path}: batch id {batch_id!r} does not match {BATCH_ID_PATTERN.pattern}"
        )


def _ingest(path: Path, batch_id: str | None, seen: dict[str, str | None]) -> IngestionResult:
    events: list[dict[str, Any]] = []
    invalid: list[InvalidRecord] = []
    total = 0

    for record in _iter_raw_records(path):
        total += 1

        if record.error is not None:
            invalid.append(
                InvalidRecord(path, record.position, "malformed_json", record.error, record.raw)
            )
            continue
        if not isinstance(record.value, dict):
            invalid.append(
                InvalidRecord(
                    path,
                    record.position,
                    "not_an_object",
                    "expected an event object",
                    record.raw,
                )
            )
            continue

        problems = validate_event(record.value)
        if problems:
            invalid.append(
                InvalidRecord(
                    path,
                    record.position,
                    "invalid_event",
                    "; ".join(problems),
                    record.raw,
                )
            )
            continue

        event_id = record.value["event_id"]
        if event_id in seen:
            first_seen = seen[event_id]
            detail = f"duplicate event_id {event_id}"
            if first_seen is not None:
                detail = f"{detail} (first seen in {first_seen})"
            invalid.append(
                InvalidRecord(path, record.position, "duplicate_event_id", detail, record.raw)
            )
            continue

        seen[event_id] = batch_id
        events.append(record.value)

    stats = IngestionStats(
        total=total,
        valid=len(events),
        invalid=len(invalid),
        malformed=sum(1 for item in invalid if item.reason == "malformed_json"),
        duplicates=sum(1 for item in invalid if item.reason == "duplicate_event_id"),
    )
    return IngestionResult(
        source=path,
        batch_id=batch_id,
        events=tuple(events),
        invalid=tuple(invalid),
        stats=stats,
    )


def ingest_events(path: Path) -> IngestionResult:
    """Tolerantly ingest a single file, separating valid and invalid records."""
    path = Path(path)
    _require_supported(path)
    return _ingest(path, None, {})


def ingest_batch(path: Path, batch_id: str | None = None) -> IngestionResult:
    """Tolerantly ingest one batch file, validating the batch identifier."""
    path = Path(path)
    resolved_id = batch_id or path.stem
    _require_batch_id(path, resolved_id)
    _require_supported(path)
    return _ingest(path, resolved_id, {})


def ingest_batches(directory: Path) -> Iterator[IngestionResult]:
    """Tolerantly ingest every ``batch_*`` file, tracking ids across batches."""
    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(directory)

    seen: dict[str, str | None] = {}
    for path in _batch_files(directory):
        batch_id = path.stem
        _require_batch_id(path, batch_id)
        yield _ingest(path, batch_id, seen)


def aggregate_stats(results: Iterable[IngestionResult]) -> IngestionStats:
    """Sum the statistics of several ingestion results."""
    total = valid = invalid = malformed = duplicates = 0
    for result in results:
        total += result.stats.total
        valid += result.stats.valid
        invalid += result.stats.invalid
        malformed += result.stats.malformed
        duplicates += result.stats.duplicates

    return IngestionStats(
        total=total,
        valid=valid,
        invalid=invalid,
        malformed=malformed,
        duplicates=duplicates,
    )
