"""Output and CLI helpers shared by the event producers."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_healer.pipeline.batching import chunk
from pipeline_healer.pipeline.producers.identifiers import batch_id
from pipeline_healer.pipeline.producers.timestamps import parse_rfc3339

OUTPUT_FORMATS = ("jsonl", "json")
FILE_EXTENSIONS = {"jsonl": "jsonl", "json": "json"}


def parse_start_at(value: str) -> datetime:
    """Argparse type for ``--start-at``: RFC 3339 with an explicit offset."""
    try:
        return parse_rfc3339(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def render_events(events: Iterable[dict[str, Any]], output_format: str = "jsonl") -> str:
    """Serialize events as JSONL lines or as a single JSON array."""
    if output_format not in OUTPUT_FORMATS:
        raise ValueError(f"unsupported output format: {output_format!r}")

    if output_format == "jsonl":
        return "".join(f"{json.dumps(event, ensure_ascii=False)}\n" for event in events)
    return f"{json.dumps(list(events), ensure_ascii=False, indent=2)}\n"


def write_events(
    events: Iterable[dict[str, Any]],
    target: Path,
    output_format: str = "jsonl",
) -> None:
    """Write a single stream to one file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_events(events, output_format), encoding="utf-8")


def write_event_batches(
    events: Iterable[dict[str, Any]],
    batch_size: int,
    out_dir: Path,
    output_format: str = "jsonl",
) -> int:
    """Write one file per batch, named by batch id, and return the batch count."""
    if output_format not in OUTPUT_FORMATS:
        raise ValueError(f"unsupported output format: {output_format!r}")

    out_dir.mkdir(parents=True, exist_ok=True)
    extension = FILE_EXTENSIONS[output_format]

    written = 0
    for number, batch in enumerate(chunk(events, batch_size), start=1):
        target = out_dir / f"{batch_id(number)}.{extension}"
        target.write_text(render_events(batch, output_format), encoding="utf-8")
        written = number

    return written
