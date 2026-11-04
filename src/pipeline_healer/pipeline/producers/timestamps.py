"""UTC timestamp helpers and contract bounds.

Timestamps are RFC 3339 in UTC with a ``Z`` suffix everywhere. The bounds below
mirror ``docs/data-contracts.md`` (timestamp semantics and DQ-007/DQ-010) and are
the single place those numbers are defined in code.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

MAX_FUTURE_SKEW = timedelta(minutes=5)
EVENT_TIME_WATERMARK = timedelta(minutes=10)
LATE_DATA_THRESHOLD = timedelta(hours=48)
FRESHNESS_THRESHOLD = timedelta(minutes=15)

DETERMINISTIC_START = datetime(2026, 1, 1, tzinfo=UTC)


def utc_now() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def rfc3339(moment: datetime) -> str:
    """Format a datetime as RFC 3339 UTC with a ``Z`` suffix."""
    return moment.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_rfc3339(value: str) -> datetime:
    """Parse an RFC 3339 timestamp that carries an explicit UTC offset."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid RFC 3339 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include an offset: {value!r}")
    return parsed.astimezone(UTC)


def resolve_start(
    *,
    start_at: datetime | None,
    seed: int | None,
    count: int,
    max_interval_seconds: int,
) -> datetime:
    """Choose the first event time for a generated stream.

    An explicit ``start_at`` wins. A seeded run without one starts at
    ``DETERMINISTIC_START`` so the output is byte-identical across runs. An
    unseeded run is backdated so the stream ends near the current time.
    """
    if start_at is not None:
        return start_at.astimezone(UTC)
    if seed is not None:
        return DETERMINISTIC_START
    return (utc_now() - timedelta(seconds=max_interval_seconds * count)).astimezone(UTC)
