"""Identifier formats for pipeline events.

These formats are part of the data contract (``docs/data-contracts.md``). Both
producers and tests build identifiers through these helpers so the format is
defined once.
"""

from __future__ import annotations

import re

EVENT_ID_PATTERN = re.compile(r"^evt_\d{6,}$")
RIDE_ID_PATTERN = re.compile(r"^ride_\d{5,}$")
ORDER_ID_PATTERN = re.compile(r"^order_\d{5,}$")
USER_ID_PATTERN = re.compile(r"^user_\d{4,}$")
DRIVER_ID_PATTERN = re.compile(r"^driver_\d{3,}$")
MERCHANT_ID_PATTERN = re.compile(r"^merchant_\d{3,}$")
BATCH_ID_PATTERN = re.compile(r"^batch_\d{3,}$")


def _positive(value: int, label: str) -> int:
    if value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def event_id(sequence: int) -> str:
    """Globally unique event identifier, e.g. ``evt_000123``."""
    return f"evt_{_positive(sequence, 'event sequence'):06d}"


def ride_id(sequence: int) -> str:
    """Ride identifier, e.g. ``ride_88213``."""
    return f"ride_{_positive(sequence, 'ride sequence'):05d}"


def order_id(sequence: int) -> str:
    """Order identifier, e.g. ``order_55210``."""
    return f"order_{_positive(sequence, 'order sequence'):05d}"


def user_id(number: int) -> str:
    """Stable user identifier, e.g. ``user_4412``."""
    return f"user_{_positive(number, 'user number'):04d}"


def driver_id(number: int) -> str:
    """Stable driver identifier, e.g. ``driver_917``."""
    return f"driver_{_positive(number, 'driver number'):03d}"


def merchant_id(number: int) -> str:
    """Stable merchant identifier, e.g. ``merchant_331``."""
    return f"merchant_{_positive(number, 'merchant number'):03d}"


def batch_id(sequence: int) -> str:
    """Pipeline batch identifier, e.g. ``batch_104``."""
    return f"batch_{_positive(sequence, 'batch sequence'):03d}"
