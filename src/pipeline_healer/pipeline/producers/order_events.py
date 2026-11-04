"""Synthetic order event producer.

Emits ``order_placed`` and ``order_cancelled`` events in the envelope format
defined in ``docs/data-contracts.md``. Output is JSON Lines, matching the ride
producer so both streams can be piped into Kafka or saved as fixtures.
"""

from __future__ import annotations

import argparse
import random
import sys
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline_healer.pipeline.producers.event_types import ORDER_EVENT_TYPES
from pipeline_healer.pipeline.producers.furniture import FURNITURE_ITEMS
from pipeline_healer.pipeline.producers.identifiers import (
    event_id,
    merchant_id,
    order_id,
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
PRODUCER = "order-service"
EVENT_ID_BASE = 200_000

EVENT_TYPES = ORDER_EVENT_TYPES
EVENT_WEIGHTS = (0.90, 0.10)
MAX_INTERVAL_SECONDS = 45

PAYMENT_METHODS = ("بطاقة", "محفظة إلكترونية", "الدفع عند الاستلام")
CANCEL_REASONS = ("تغيير الخطة", "تأخر التحضير", "خطأ في العنوان")
CURRENCIES = ("DZD",)


def _order_payload(
    rng: random.Random,
    event_type: str,
    order_key: str,
    city: str,
    occurred_at: str,
) -> dict[str, Any]:
    match event_type:
        case "order_placed":
            items = [
                {
                    "sku": sku,
                    "name": name,
                    "quantity": rng.randint(1, 2),
                    "unit_price": unit_price,
                }
                for sku, name, unit_price in rng.sample(FURNITURE_ITEMS, k=rng.randint(1, 3))
            ]
            total_amount = round(sum(item["quantity"] * item["unit_price"] for item in items), 2)
            return {
                "order_id": order_key,
                "user_id": user_id(rng.randint(1, 9999)),
                "merchant_id": merchant_id(rng.randint(1, 999)),
                "city": city,
                "district": rng.choice(WILAYAS[city]),
                "items": items,
                "total_amount": total_amount,
                "currency": rng.choice(CURRENCIES),
                "payment_method": rng.choice(PAYMENT_METHODS),
            }
        case "order_cancelled":
            return {
                "order_id": order_key,
                "reason": rng.choice(CANCEL_REASONS),
                "cancelled_at": occurred_at,
            }
        case _:
            raise ValueError(f"unsupported order event type: {event_type}")


def generate_order_events(
    count: int,
    *,
    seed: int | None = None,
    start_at: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield ``count`` order events with a valid envelope.

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
            "payload": _order_payload(
                rng, event_type, order_id(100_000 + index), city, occurred_at
            ),
        }

        problems = validate_event(record)
        if problems:
            raise RecordValidationError([f"event[{index}] {problem}" for problem in problems])

        yield record


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="order-event-producer",
        description="Generate synthetic order events as JSON or JSON Lines.",
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

    events = generate_order_events(args.count, seed=args.seed, start_at=args.start_at)

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
