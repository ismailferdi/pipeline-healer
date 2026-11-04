from datetime import datetime, timedelta, timezone

from pipeline_healer.pipeline.producers.timestamps import (
    EVENT_TIME_WATERMARK,
    FRESHNESS_THRESHOLD,
    LATE_DATA_THRESHOLD,
    MAX_FUTURE_SKEW,
    rfc3339,
    utc_now,
)


def test_rfc3339_formats_utc_with_z_suffix() -> None:
    assert rfc3339(utc_now()).endswith("Z")


def test_rfc3339_converts_offsets_to_utc() -> None:
    riyadh = timezone(timedelta(hours=3))

    moment = rfc3339(datetime(2026, 11, 3, 13, 30, 45, tzinfo=riyadh))

    assert moment == "2026-11-03T10:30:45Z"


def test_utc_now_is_timezone_aware() -> None:
    now = utc_now()

    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


def test_contract_bounds() -> None:
    assert timedelta(minutes=5) == MAX_FUTURE_SKEW
    assert timedelta(minutes=10) == EVENT_TIME_WATERMARK
    assert timedelta(hours=48) == LATE_DATA_THRESHOLD
    assert timedelta(minutes=15) == FRESHNESS_THRESHOLD
