"""Event reader for the ingestion stage.

Reads JSON Lines and JSON array files, validates every record against the data
contract, and exposes batch-oriented reading for directories of ``batch_*`` files.
Malformed input never reaches the transform stage.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
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


def _iter_jsonl(path: Path) -> Iterator[tuple[int, Any]]:
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                yield number, json.loads(line)
            except json.JSONDecodeError as exc:
                raise EventReadError(f"{path}:{number}: invalid JSON: {exc.msg}") from exc


def _iter_json_array(path: Path) -> Iterator[tuple[int, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise EventReadError(f"{path}: invalid JSON: {exc.msg}") from exc

    if not isinstance(data, list):
        raise EventReadError(f"{path}: expected a JSON array of events")

    yield from enumerate(data, start=1)


def read_events(path: Path) -> Iterator[dict[str, Any]]:
    """Yield validated events from a ``.jsonl`` or ``.json`` file.

    Validation errors and duplicate ``event_id`` values raise
    :class:`RecordValidationError` with the file and position in the message.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix not in SUPPORTED_SUFFIXES:
        raise EventReadError(
            f"{path}: unsupported format {suffix!r}; expected one of {SUPPORTED_SUFFIXES}"
        )
    if not path.is_file():
        raise FileNotFoundError(path)

    records = _iter_jsonl(path) if suffix == ".jsonl" else _iter_json_array(path)
    seen: set[str] = set()

    for position, event in records:
        if not isinstance(event, dict):
            raise EventReadError(f"{path}:{position}: expected an event object")

        problems = validate_event(event)
        if problems:
            raise RecordValidationError([f"{path}:{position}: {problem}" for problem in problems])

        event_id = event["event_id"]
        if event_id in seen:
            raise RecordValidationError([f"{path}:{position}: duplicate event_id {event_id}"])
        seen.add(event_id)

        yield event


def read_batch(path: Path, batch_id: str | None = None) -> EventBatch:
    """Read one batch file and return its validated events."""
    path = Path(path)
    resolved_id = batch_id or path.stem

    if not BATCH_ID_PATTERN.match(resolved_id):
        raise EventReadError(
            f"{path}: batch id {resolved_id!r} does not match {BATCH_ID_PATTERN.pattern}"
        )

    return EventBatch(batch_id=resolved_id, source=path, events=tuple(read_events(path)))


def read_event_batches(directory: Path) -> Iterator[EventBatch]:
    """Yield every ``batch_*`` file in ``directory``, sorted by name.

    Duplicate event ids across batches are rejected, since event ids are unique
    across the whole stream even though DQ-002 is checked per batch.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(directory)

    files = sorted([*directory.glob("batch_*.jsonl"), *directory.glob("batch_*.json")])
    seen: dict[str, str] = {}

    for path in files:
        batch = read_batch(path)
        for event in batch.events:
            event_id = event["event_id"]
            if event_id in seen:
                raise RecordValidationError(
                    [f"{path}: duplicate event_id {event_id} already seen in {seen[event_id]}"]
                )
            seen[event_id] = batch.batch_id
        yield batch
