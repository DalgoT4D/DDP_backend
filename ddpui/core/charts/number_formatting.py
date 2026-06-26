"""Shared number formatter — single source of truth for ``value → display string``.

Used by:
- ``echarts_config_generator._format_number`` (number-chart rendering)
- KPI render paths (alert email body via ``{{current_value}}`` token)
- Any future surface that needs a formatted numeric string server-side

Mirrors ``webapp_v2/lib/formatters.ts:formatNumber`` for the seven supported
formats, with the same decimal / locale / threshold semantics, so backend and
frontend produce byte-identical output for the same inputs. A cross-stack
parity fixture (see ``tests/fixtures/number_format_parity.json``) guards this.
"""

from __future__ import annotations

import math
from typing import Optional

from babel.numbers import format_decimal


_SUPPORTED_FORMATS = {
    "default",
    "percentage",
    "indian",
    "international",
    "european",
    "adaptive_indian",
    "adaptive_international",
}


def _babel_format_string(decimal_places: int) -> str:
    """Build a babel format pattern for ``N`` decimal places."""
    if decimal_places <= 0:
        return "#,##0"
    return "#,##0." + ("0" * decimal_places)


def _indian_format_string(decimal_places: int) -> str:
    """Indian lakh/crore grouping pattern for babel (``en_IN``)."""
    if decimal_places <= 0:
        return "#,##,##0"
    return "#,##,##0." + ("0" * decimal_places)


def _fixed(value: float, decimal_places: int) -> str:
    """Mirror JS ``toFixed(n)``."""
    return f"{value:.{decimal_places}f}"


def format_number_v2(
    value: Optional[float],
    format_type: str = "default",
    decimal_places: int = 0,
    prefix: str = "",
    suffix: str = "",
) -> str:
    """Format a numeric value for display.

    Returns ``"No data"`` for ``None`` / ``NaN`` — without wrapping prefix/suffix.
    Unrecognised ``format_type`` falls back to ``default`` (matches frontend).

    See ``webapp_v2/lib/formatters.ts:formatNumber`` for the JS counterpart.
    """
    # None / NaN guard — also covers infinity since math.isfinite catches both
    if value is None:
        return "No data"
    if isinstance(value, float) and not math.isfinite(value):
        return "No data"

    # Clamp decimal_places to the same [0, 10] range the frontend enforces
    decimal_places = max(0, min(10, decimal_places or 0))

    if format_type == "default":
        formatted = (
            _fixed(value, decimal_places)
            if decimal_places > 0
            else (str(int(value)) if value == int(value) else str(value))
        )

    elif format_type == "percentage":
        # Percentage semantics: value is a ratio (0.85 → 85.00%). Multiply by 100
        # then render with the requested decimals + '%'.
        formatted = _fixed(value * 100, decimal_places) + "%"

    elif format_type == "international":
        formatted = format_decimal(
            value, locale="en_US", format=_babel_format_string(decimal_places)
        )

    elif format_type == "indian":
        formatted = format_decimal(
            value, locale="en_IN", format=_indian_format_string(decimal_places)
        )

    elif format_type == "european":
        formatted = format_decimal(
            value, locale="de_DE", format=_babel_format_string(decimal_places)
        )

    elif format_type == "adaptive_international":
        formatted = _adaptive_international(value, decimal_places)

    elif format_type == "adaptive_indian":
        formatted = _adaptive_indian(value, decimal_places)

    else:
        # Unknown format → default (matches frontend's `default` branch behavior)
        formatted = (
            _fixed(value, decimal_places)
            if decimal_places > 0
            else (str(int(value)) if value == int(value) else str(value))
        )

    if prefix or suffix:
        return f"{prefix}{formatted}{suffix}"
    return formatted


def _adaptive_international(value: float, decimal_places: int) -> str:
    """K / M / B compact notation. Default 2 decimals if not specified."""
    decimals = decimal_places if decimal_places else 2
    abs_value = abs(value)
    sign = "-" if value < 0 else ""

    if abs_value >= 1_000_000_000:
        return f"{sign}{_fixed(abs_value / 1_000_000_000, decimals)}B"
    if abs_value >= 1_000_000:
        return f"{sign}{_fixed(abs_value / 1_000_000, decimals)}M"
    if abs_value >= 1_000:
        return f"{sign}{_fixed(abs_value / 1_000, decimals)}K"
    return _fixed(value, decimal_places) if decimal_places > 0 else str(value)


def _adaptive_indian(value: float, decimal_places: int) -> str:
    """K / L (Lakh) / Cr (Crore) compact notation. Default 2 decimals."""
    decimals = decimal_places if decimal_places else 2
    abs_value = abs(value)
    sign = "-" if value < 0 else ""

    if abs_value >= 10_000_000:
        return f"{sign}{_fixed(abs_value / 10_000_000, decimals)}Cr"
    if abs_value >= 100_000:
        return f"{sign}{_fixed(abs_value / 100_000, decimals)}L"
    if abs_value >= 1_000:
        return f"{sign}{_fixed(abs_value / 1_000, decimals)}K"
    return _fixed(value, decimal_places) if decimal_places > 0 else str(value)
