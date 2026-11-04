"""Shared exceptions for the transformation stage."""

from __future__ import annotations


class TransformError(ValueError):
    """Base class for recoverable transformation failures."""
