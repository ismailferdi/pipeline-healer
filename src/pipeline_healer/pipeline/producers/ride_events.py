"""Synthetic ride event producer.

Emits ``ride_requested``, ``ride_completed``, and ``ride_cancelled`` events in the
envelope format defined in ``docs/data-contracts.md``. Output is JSON Lines, so it
can be piped into a Kafka producer, loaded by the stream job, or saved as a fixture.
"""

from __future__ import annotations

import argparse
import random
import sys
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline_healer.pipeline.producers.event_types import RIDE_EVENT_TYPES
from pipeline_healer.pipeline.producers.identifiers import (
    driver_id,
    event_id,
    ride_id,
    user_id,
)
from pipeline_healer.pipeline.producers.locations import WILAYAS
from pipeline_healer.pipeline.producers.output import (
    OUTPUT_FORMATS,
    parse_start_at,
    render_events,
    write_event_batches,
    write_events,
)
from pipeline_healer.pipeline.producers.timestamps import resolve_start, rfc3339
from pipeline_healer.pipeline.validation import RecordValidationError, validate_event

SCHEMA_VERSION = "1.4.0"
PRODUCER = "ride-service"
EVENT_ID_BASE = 100_000

EVENT_TYPES = RIDE_EVENT_TYPES
EVENT_WEIGHTS = (0.35, 0.55, 0.10)
MAX_INTERVAL_SECONDS = 30

PAYMENT_METHODS = ("بطاقة", "محفظة إلكترونية", "نقداً")
CANCEL_REASONS = ("لم يصل السائق", "تغيير الخطة", "خطأ في العنوان")
CURRENCIES = ("DZD",)

BASE_FARE = 100.0
FARE_PER_KM = 40.0
FARE_PER_MINUTE = 5.0


def _ride_payload(
    rng: random.Random,
    event_type: str,
    ride_key: str,
    city: str,
    occurred_at: str,
) -> dict[str, Any]:
    areas = WILAYAS[city]
    pickup_area, dropoff_area = rng.sample(areas, k=2) if len(areas) >= 2 else (areas[0], areas[0])

    match event_type:
        case "ride_requested":
            return {
                "ride_id": ride_key,
                "user_id": user_id(rng.randint(1, 9999)),
                "city": city,
                "pickup_area": pickup_area,
                "requested_at": occurred_at,
            }
        case "ride_completed":
            distance_km = round(rng.uniform(1.0, 25.0), 1)
            duration_min = max(1, round(distance_km * rng.uniform(1.5, 3.5)))
            fare_amount = round(
                BASE_FARE + distance_km * FARE_PER_KM + duration_min * FARE_PER_MINUTE, 2
            )
            return {
                "ride_id": ride_key,
                "user_id": user_id(rng.randint(1, 9999)),
                "driver_id": driver_id(rng.randint(1, 999)),
                "city": city,
                "pickup_area": pickup_area,
                "dropoff_area": dropoff_area,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "fare_amount": fare_amount,
                "currency": rng.choice(CURRENCIES),
                "payment_method": rng.choice(PAYMENT_METHODS),
            }
        case "ride_cancelled":
            return {
                "ride_id": ride_key,
                "user_id": user_id(rng.randint(1, 9999)),
                "city": city,
                "reason": rng.choice(CANCEL_REASONS),
                "cancelled_at": occurred_at,
            }
        case _:
            raise ValueError(f"unsupported ride event type: {event_type}")


def generate_ride_events(
    count: int,
    *,
    seed: int | None = None,
    start_at: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield ``count`` ride events with a valid envelope.

    Passing the same ``seed`` and ``start_at`` produces an identical stream.
    A seeded run without ``start_at`` starts at ``DETERMINISTIC_START``; an
    unseeded run is backdated so it ends at the current time.
    """
    if count < 0:
        raise ValueError("count must be non-negative")

    rng = random.Random(seed)
    clock = resolve_start(
        start_at=start_at,
        seed=seed,
        count=count,
        max_interval_seconds=MAX_INTERVAL_SECONDS,
    )

    for index in range(1, count + 1):
        clock += timedelta(seconds=rng.randint(1, MAX_INTERVAL_SECONDS))
        event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        city = rng.choice(list(WILAYAS))
        occurred_at = rfc3339(clock)
        produced_at = rfc3339(clock + timedelta(seconds=rng.randint(0, 2)))

        record = {
            "event_id": event_id(EVENT_ID_BASE + index),
            "event_type": event_type,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": occurred_at,
            "produced_at": produced_at,
            "producer": PRODUCER,
            "payload": _ride_payload(rng, event_type, ride_id(100_000 + index), city, occurred_at),
        }

        problems = validate_event(record)
        if problems:
            raise RecordValidationError([f"event[{index}] {problem}" for problem in problems])

        yield record


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ride-event-producer",
        description="Generate synthetic ride events as JSON or JSON Lines.",
    )
    parser.add_argument("--count", type=int, default=10, help="number of events (default: 10)")
    parser.add_argument("--seed", type=int, default=None, help="seed for reproducible output")
    parser.add_argument(
        "--start-at",
        type=parse_start_at,
        default=None,
        help="first event time, RFC 3339 with offset "
        "(default: now, or 2026-01-01T00:00:00Z with --seed)",
    )
    parser.add_argument(
        "--format",
        choices=OUTPUT_FORMATS,
        default="jsonl",
        help="output format (default: jsonl)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="events per batch file (requires --out to be a directory)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output file, or directory when --batch-size is set (default: stdout)",
    )
    args = parser.parse_args(argv)

    events = generate_ride_events(args.count, seed=args.seed, start_at=args.start_at)

    if args.batch_size is not None:
        if args.batch_size < 1:
            parser.error("--batch-size must be at least 1")
        if args.out is None:
            parser.error("--batch-size requires --out to be a directory")
        batches = write_event_batches(events, args.batch_size, args.out, args.format)
        sys.stdout.write(f"wrote {batches} batch(es) to {args.out}\n")
        return 0

    if args.out is None:
        sys.stdout.write(render_events(events, args.format))
    else:
        write_events(events, args.out, args.format)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
