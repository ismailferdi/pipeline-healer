"""Generate reusable, deterministic sample datasets.

Fixtures are written as JSONL plus a manifest recording the exact seed, start
time, counts, and SHA-256 of each file. Regenerating with the same inputs yields
byte-identical output, so committed samples can be verified in CI.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_healer.pipeline.producers.order_events import generate_order_events
from pipeline_healer.pipeline.producers.output import parse_start_at, render_events
from pipeline_healer.pipeline.producers.ride_events import SCHEMA_VERSION, generate_ride_events
from pipeline_healer.pipeline.producers.timestamps import DETERMINISTIC_START, rfc3339

SAMPLE_SEED = 2026
SAMPLE_COUNT = 200
SAMPLE_START = DETERMINISTIC_START


def build_sample_events(
    *,
    count: int = SAMPLE_COUNT,
    seed: int = SAMPLE_SEED,
    start_at: datetime = SAMPLE_START,
) -> dict[str, list[dict[str, Any]]]:
    """Build the ride and order datasets in memory."""
    if count < 1:
        raise ValueError("count must be at least 1")

    return {
        "ride_events": list(generate_ride_events(count, seed=seed, start_at=start_at)),
        "order_events": list(generate_order_events(count, seed=seed, start_at=start_at)),
    }


def generate_samples(
    out_dir: Path,
    *,
    count: int = SAMPLE_COUNT,
    seed: int = SAMPLE_SEED,
    start_at: datetime = SAMPLE_START,
) -> dict[str, Any]:
    """Write the sample datasets and manifest, returning the manifest."""
    datasets = build_sample_events(count=count, seed=seed, start_at=start_at)

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "generator": "pipeline_healer.pipeline.producers.samples",
        "schema_version": SCHEMA_VERSION,
        "seed": seed,
        "start_at": rfc3339(start_at),
        "count": count,
        "datasets": {},
    }

    for name, events in datasets.items():
        text = render_events(events, "jsonl")
        target = out_dir / f"{name}.jsonl"
        target.write_text(text, encoding="utf-8")
        manifest["datasets"][name] = {
            "file": target.name,
            "count": len(events),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(
        f"{json.dumps(manifest, ensure_ascii=False, indent=2)}\n", encoding="utf-8"
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="generate-samples",
        description="Generate deterministic ride and order sample datasets.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/samples"),
        help="output directory (default: data/samples)",
    )
    parser.add_argument("--count", type=int, default=SAMPLE_COUNT)
    parser.add_argument("--seed", type=int, default=SAMPLE_SEED)
    parser.add_argument(
        "--start-at",
        type=parse_start_at,
        default=SAMPLE_START,
        help="first event time, RFC 3339 with offset (default: 2026-01-01T00:00:00Z)",
    )
    args = parser.parse_args(argv)

    if args.count < 1:
        parser.error("--count must be at least 1")

    manifest = generate_samples(
        args.out,
        count=args.count,
        seed=args.seed,
        start_at=args.start_at,
    )
    total = sum(dataset["count"] for dataset in manifest["datasets"].values())
    sys.stdout.write(
        f"wrote {total} events in {len(manifest['datasets'])} datasets to {args.out}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
