"""Data type conversion with explicit, field-aware errors.

Money is converted to :class:`~decimal.Decimal` rather than ``float`` so totals
and fares keep exact decimal precision; integral fields reject fractional input.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from pipeline_healer.pipeline.transforms.errors import TransformError

MONEY_PLACES = Decimal("0.01")


class ConversionError(TransformError):
    """Raised when a value cannot be converted to the requested type."""


def to_int(value: Any, *, field: str = "value") -> int:
    """Convert an integer-like value, rejecting bools and fractions."""
    if isinstance(value, bool):
        raise ConversionError(f"{field}: expected an integer, got a boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ConversionError(f"{field}: expected an integer, got {value!r}")
        return int(value)
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ConversionError(f"{field}: expected an integer, got {value!r}")
        return int(value)
    if isinstance(value, str):
        try:
            number = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ConversionError(f"{field}: expected an integer, got {value!r}") from exc
        if not number.is_finite() or number != number.to_integral_value():
            raise ConversionError(f"{field}: expected an integer, got {value!r}")
        return int(number)

    raise ConversionError(f"{field}: expected an integer, got {type(value).__name__}")


def to_decimal(
    value: Any,
    *,
    field: str = "value",
    places: int | None = None,
    minimum: Decimal | int | float | None = None,
) -> Decimal:
    """Convert a number-like value to a finite ``Decimal``.

    ``places`` quantizes the result (money uses two places); ``minimum`` rejects
    values below the given bound.
    """
    if isinstance(value, bool):
        raise ConversionError(f"{field}: expected a number, got a boolean")
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, float):
        number = Decimal(str(value))
    elif isinstance(value, str):
        try:
            number = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ConversionError(f"{field}: expected a number, got {value!r}") from exc
    else:
        raise ConversionError(f"{field}: expected a number, got {type(value).__name__}")

    if not number.is_finite():
        raise ConversionError(f"{field}: expected a finite number, got {value!r}")

    if places is not None:
        try:
            number = number.quantize(Decimal(1).scaleb(-places))
        except InvalidOperation as exc:
            raise ConversionError(f"{field}: cannot quantize {value!r}") from exc

    if minimum is not None and number < Decimal(str(minimum)):
        raise ConversionError(f"{field}: must be >= {minimum}, got {number}")

    return number


def to_money(value: Any, *, field: str = "amount") -> Decimal:
    """Convert an amount to a non-negative two-place decimal."""
    return to_decimal(value, field=field, places=2, minimum=0)
