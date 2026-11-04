"""Field and text normalization for incoming records.

Handles vendor-shaped field names (camelCase, spaced, hyphenated), Unicode
normalization, and enum casing. Keys are normalized recursively; a collision
after normalization is an error rather than a silent overwrite.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from pipeline_healer.pipeline.transforms.errors import TransformError

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM = re.compile(r"[^0-9A-Za-z]+")


def normalize_field_name(name: str) -> str:
    """Convert a field name to lowercase snake_case."""
    if not isinstance(name, str) or not name.strip():
        raise TransformError(f"field name must be a non-empty string: {name!r}")

    spaced = _CAMEL_BOUNDARY.sub("_", name.strip())
    collapsed = _NON_ALNUM.sub("_", spaced).strip("_").lower()
    if not collapsed:
        raise TransformError(f"field name normalizes to empty: {name!r}")
    return collapsed


def normalize_keys(value: Any) -> Any:
    """Recursively normalize dictionary keys, raising on collisions."""
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TransformError(f"field names must be strings: {key!r}")
            name = normalize_field_name(key)
            if name in normalized:
                raise TransformError(f"duplicate field after normalization: {name!r}")
            normalized[name] = normalize_keys(item)
        return normalized
    if isinstance(value, list):
        return [normalize_keys(item) for item in value]
    return value


def normalize_text(value: str) -> str:
    """Normalize Unicode and collapse whitespace."""
    return " ".join(unicodedata.normalize("NFC", value).split())


def normalize_event_type(value: str) -> str:
    """Canonicalize an event type: lowercase snake_case."""
    if not isinstance(value, str) or not value.strip():
        raise TransformError(f"event type must be a non-empty string: {value!r}")
    return re.sub(r"[\s-]+", "_", value.strip().lower())


def normalize_currency(value: str) -> str:
    """Canonicalize an ISO 4217 currency code."""
    if not isinstance(value, str) or not value.strip():
        raise TransformError(f"currency must be a non-empty string: {value!r}")
    return value.strip().upper()
