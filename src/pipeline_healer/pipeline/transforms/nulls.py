"""Null handling: recognize placeholder tokens and normalize them to ``None``."""

from __future__ import annotations

from typing import Any

from pipeline_healer.pipeline.transforms.normalization import normalize_text

NULL_TOKENS = frozenset({"", "null", "none", "nil", "n/a", "na", "-", "—", "غير متوفر"})


def is_null(value: Any) -> bool:
    """Return whether a value is ``None`` or a recognized null placeholder."""
    if value is None:
        return True
    if isinstance(value, str):
        return normalize_text(value).casefold() in NULL_TOKENS
    return False


def nullify(value: Any) -> Any:
    """Map null placeholders to ``None``; leave other values untouched."""
    return None if is_null(value) else value


def normalize_nulls(value: Any) -> Any:
    """Recursively replace null placeholders in dictionaries and lists."""
    if isinstance(value, dict):
        return {key: normalize_nulls(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_nulls(item) for item in value]
    return nullify(value)
