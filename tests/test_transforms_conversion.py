from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pipeline_healer.pipeline.transforms.conversion import (
    ConversionError,
    to_decimal,
    to_int,
    to_money,
)
from pipeline_healer.pipeline.transforms.timestamps import (
    TimestampError,
    normalize_timestamp,
    parse_timestamp,
    to_utc,
)


@pytest.mark.parametrize("value", [5, 5.0, "5", " 42 ", "3.0", Decimal("7")])
def test_to_int_accepts_integral_values(value) -> None:
    assert to_int(value) in {5, 42, 3, 7}


@pytest.mark.parametrize("value", [True, 5.5, "5.5", "abc", None, "NaN"])
def test_to_int_rejects_non_integral_values(value) -> None:
    with pytest.raises(ConversionError, match="duration"):
        to_int(value, field="duration")


def test_to_decimal_preserves_value_and_precision() -> None:
    assert to_decimal("7.40") == Decimal("7.40")
    assert to_decimal(0.1) == Decimal("0.1")
    assert to_decimal(38.5) == Decimal("38.5")


def test_to_money_quantizes_to_two_places() -> None:
    assert to_money("38.5") == Decimal("38.50")
    assert str(to_money(1200)) == "1200.00"


@pytest.mark.parametrize("value", [-1, "-0.01"])
def test_to_money_rejects_negative_amounts(value) -> None:
    with pytest.raises(ConversionError, match=">= 0"):
        to_money(value)


@pytest.mark.parametrize("value", [True, "abc", None, float("nan"), float("inf")])
def test_to_decimal_rejects_invalid_numbers(value) -> None:
    with pytest.raises(ConversionError):
        to_decimal(value)


def test_parse_timestamp_accepts_zulu_and_offsets() -> None:
    assert parse_timestamp("2026-11-03T10:30:00Z") == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)
    assert parse_timestamp("2026-11-03T13:30:00+03:00") == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)


def test_parse_timestamp_attaches_assumed_timezone_to_naive_values() -> None:
    offset = timezone(timedelta(hours=1))

    parsed = parse_timestamp("2026-11-03T10:30:00", assume_timezone=offset)

    assert parsed == datetime(2026, 11, 3, 10, 30, tzinfo=offset)
    assert to_utc(parsed) == datetime(2026, 11, 3, 9, 30, tzinfo=UTC)


def test_normalize_timestamp_returns_utc() -> None:
    moment = normalize_timestamp("2026-11-03T13:30:00+03:00")

    assert moment == datetime(2026, 11, 3, 10, 30, tzinfo=UTC)
    assert moment.tzinfo is UTC


@pytest.mark.parametrize("value", ["", "not-a-date", 12345, None])
def test_parse_timestamp_rejects_invalid_values(value) -> None:
    with pytest.raises(TimestampError):
        parse_timestamp(value)


def test_parse_timestamp_rejects_unknown_timezone() -> None:
    with pytest.raises(TimestampError, match="unknown timezone"):
        parse_timestamp("2026-11-03T10:30:00", assume_timezone="Mars/Olympus")


def test_to_utc_rejects_non_datetime() -> None:
    with pytest.raises(TimestampError):
        to_utc("2026-11-03T10:30:00Z")
