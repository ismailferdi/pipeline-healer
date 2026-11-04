"""Batch grouping shared by producers and the pipeline.

A batch is a bounded group of events that the pipeline processes and validates
together (DQ-002 applies per batch). A partial final batch is allowed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator


def chunk[T](events: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield consecutive lists of at most ``size`` events."""
    if size < 1:
        raise ValueError("batch size must be at least 1")

    batch: list[T] = []
    for event in events:
        batch.append(event)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch
