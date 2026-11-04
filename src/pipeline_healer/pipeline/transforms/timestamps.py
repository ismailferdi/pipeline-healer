"""Timestamp parsing and timezone normalization.

All transformed timestamps are timezone-aware UTC datetimes. Naive input is
attached to an explicitly supplied assumption timezone (UTC by default) instead of
silently using the machine clock.
"""

from __future__ import annotations

from datetime import UTC, datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pipeline_healer.pipeline.transforms.errors import TransformError


class TimestampError(TransformError):
    """Raised when a timestamp cannot be parsed or normalized."""


def _resolve_timezone(value: str | tzinfo) -> tzinfo:
    if isinstance(value, str):
        try:
            return ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise TimestampError(f"unknown timezone: {value!r}") from exc
    return value


def parse_timestamp(
    value: str,
    *,
    field: str = "timestamp",
    assume_timezone: str | tzinfo = UTC,
) -> datetime:
    """Parse an RFC 3339/ISO 8601 timestamp, attaching an assumption if naive."""
    if not isinstance(value, str) or not value.strip():
        raise TimestampError(f"{field}: expected a timestamp string, got {value!r}")

    try:
        moment = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise TimestampError(f"{field}: invalid timestamp {value!r}") from exc

    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_resolve_timezone(assume_timezone))
    return moment


def to_utc(
    moment: datetime,
    *,
    field: str = "timestamp",
    assume_timezone: str | tzinfo = UTC,
) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if not isinstance(moment, datetime):
        raise TimestampError(f"{field}: expected a datetime, got {type(moment).__name__}")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_resolve_timezone(assume_timezone))
    return moment.astimezone(UTC)


def normalize_timestamp(
    value: str,
    *,
    field: str = "timestamp",
    assume_timezone: str | tzinfo = UTC,
) -> datetime:
    """Parse a timestamp string and return it as an aware UTC datetime."""
    return to_utc(
        parse_timestamp(value, field=field, assume_timezone=assume_timezone),
        field=field,
        assume_timezone=assume_timezone,
    )
