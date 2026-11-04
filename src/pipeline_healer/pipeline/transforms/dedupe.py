"""Duplicate detection over event streams.

The first occurrence wins and order is preserved; later occurrences are returned
separately so they can be counted or quarantined.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass
from typing import Any

from pipeline_healer.pipeline.transforms.errors import TransformError


@dataclass(frozen=True)
class DedupeResult:
    """Unique events plus the duplicates that were dropped."""

    unique: tuple[dict[str, Any], ...]
    duplicates: tuple[dict[str, Any], ...]

    @property
    def duplicate_count(self) -> int:
        return len(self.duplicates)


def _event_key(event: dict[str, Any], key: str | Callable[[dict[str, Any]], Hashable]) -> Hashable:
    if callable(key):
        return key(event)
    if key not in event:
        raise TransformError(f"event is missing dedupe key {key!r}")
    return event[key]


def dedupe_events(
    events: Iterable[dict[str, Any]],
    *,
    key: str | Callable[[dict[str, Any]], Hashable] = "event_id",
) -> DedupeResult:
    """Split events into unique and duplicate records."""
    unique: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen: set[Hashable] = set()

    for event in events:
        value = _event_key(event, key)
        if value in seen:
            duplicates.append(event)
            continue
        seen.add(value)
        unique.append(event)

    return DedupeResult(unique=tuple(unique), duplicates=tuple(duplicates))
