"""Tests for ``ddpui.core.charts.number_formatting.format_number_v2``.

These tests lock the backend formatter's output for every supported format type
and edge case. A separate cross-stack parity fixture
(``tests/fixtures/number_format_parity.json``) ensures the frontend
``formatNumber`` produces byte-identical output for the same inputs.
"""

from __future__ import annotations

import math

import pytest

from ddpui.core.charts.number_formatting import format_number_v2


# ── Null / NaN / Infinity ──────────────────────────────────────────────────


class TestNullAndNonFinite:
    @pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), float("-inf")])
    def test_returns_no_data(self, bad):
        assert format_number_v2(bad) == "No data"

    def test_no_data_ignores_prefix_suffix(self):
        # prefix/suffix should NOT wrap "No data" — that would render as e.g. "₹No data%"
        assert format_number_v2(None, "indian", 2, "₹", "%") == "No data"


# ── default ────────────────────────────────────────────────────────────────


class TestDefault:
    def test_integer_no_decimals(self):
        assert format_number_v2(42, "default", 0) == "42"

    def test_float_with_decimals(self):
        assert format_number_v2(3.14159, "default", 2) == "3.14"

    def test_zero_no_decimals(self):
        assert format_number_v2(0, "default", 0) == "0"

    def test_negative(self):
        assert format_number_v2(-123, "default", 0) == "-123"

    def test_unknown_format_falls_back_to_default(self):
        assert format_number_v2(42, "wild-and-crazy", 0) == "42"


# ── percentage ─────────────────────────────────────────────────────────────


class TestPercentage:
    def test_zero_decimals(self):
        assert format_number_v2(85, "percentage", 0) == "85%"

    def test_two_decimals(self):
        assert format_number_v2(85.5, "percentage", 2) == "85.50%"


# ── currency ───────────────────────────────────────────────────────────────


class TestCurrency:
    def test_no_decimals(self):
        assert format_number_v2(1234, "currency", 0) == "$1,234"

    def test_two_decimals(self):
        assert format_number_v2(1234.5, "currency", 2) == "$1,234.50"


# ── international (en_US grouping) ─────────────────────────────────────────


class TestInternational:
    def test_million_no_decimals(self):
        assert format_number_v2(1_000_000, "international", 0) == "1,000,000"

    def test_decimals(self):
        assert format_number_v2(1234567.89, "international", 2) == "1,234,567.89"


# ── indian (lakh / crore grouping) ─────────────────────────────────────────


class TestIndian:
    def test_six_digits(self):
        # 1,23,456 — Indian lakh grouping
        assert format_number_v2(123456, "indian", 0) == "1,23,456"

    def test_crore(self):
        # 1,23,45,678 — Indian crore grouping
        assert format_number_v2(12345678, "indian", 0) == "1,23,45,678"

    def test_decimals(self):
        assert format_number_v2(123456.789, "indian", 2) == "1,23,456.79"


# ── european (de_DE: dot-thousands, comma-decimal) ─────────────────────────


class TestEuropean:
    def test_thousand_no_decimals(self):
        assert format_number_v2(1234, "european", 0) == "1.234"

    def test_decimals(self):
        assert format_number_v2(1234.56, "european", 2) == "1.234,56"


# ── adaptive_international (K / M / B) ─────────────────────────────────────


class TestAdaptiveInternational:
    def test_under_thousand(self):
        # below 1k threshold — no compact suffix; default 2 decimals not applied
        # for the small-number path (matches frontend logic)
        assert format_number_v2(500, "adaptive_international", 0) == "500"

    def test_thousand(self):
        assert format_number_v2(1500, "adaptive_international", 2) == "1.50K"

    def test_million(self):
        assert format_number_v2(2_500_000, "adaptive_international", 2) == "2.50M"

    def test_billion(self):
        assert format_number_v2(1_200_000_000, "adaptive_international", 1) == "1.2B"

    def test_negative(self):
        assert format_number_v2(-2_500_000, "adaptive_international", 1) == "-2.5M"


# ── adaptive_indian (K / L / Cr) ───────────────────────────────────────────


class TestAdaptiveIndian:
    def test_thousand(self):
        assert format_number_v2(1500, "adaptive_indian", 2) == "1.50K"

    def test_lakh(self):
        # 1 lakh = 100,000
        assert format_number_v2(150_000, "adaptive_indian", 2) == "1.50L"

    def test_crore(self):
        # 1 crore = 10,000,000
        assert format_number_v2(12_500_000, "adaptive_indian", 2) == "1.25Cr"


# ── prefix / suffix wrapping ───────────────────────────────────────────────


class TestPrefixSuffix:
    def test_prefix_only(self):
        assert format_number_v2(1234, "indian", 0, prefix="₹") == "₹1,234"

    def test_suffix_only(self):
        assert format_number_v2(85, "default", 0, suffix=" people") == "85 people"

    def test_prefix_and_suffix(self):
        assert format_number_v2(1234, "indian", 0, prefix="₹", suffix=" total") == "₹1,234 total"

    def test_currency_prefix_combines_with_user_prefix(self):
        # currency adds its own "$"; user prefix wraps on the outside
        # (e.g. user prefix "USD " → "USD $1,234")
        assert format_number_v2(1234, "currency", 0, prefix="USD ") == "USD $1,234"


# ── decimal-place clamping (mirrors frontend MAX_DECIMAL_PLACES=10 / min 0) ─


class TestDecimalClamping:
    def test_negative_decimals_clamped_to_zero(self):
        # Negative decimal_places shouldn't crash. Clamped to 0; under the
        # `default` format that means "no rounding" — preserves the legacy
        # ``EChartsConfigGenerator._format_number`` behavior for back-compat
        # with existing number-chart consumers.
        assert format_number_v2(1234.5678, "default", -3) == "1234.5678"

    def test_over_max_decimals_clamped(self):
        # 25 → clamps to 10
        result = format_number_v2(1.123456789012345, "default", 25)
        # exactly 10 decimal places
        assert result.split(".")[1] == "1234567890"


# ── integration: matches existing _format_number behavior ──────────────────


class TestBackwardCompatWithEChartsGenerator:
    """The old ``EChartsConfigGenerator._format_number`` now delegates to
    ``format_number_v2``. These tests pin the output for the three formats it
    handled before (default / percentage / currency) so the delegation is
    behavior-preserving for existing chart consumers."""

    def test_default_int(self):
        assert format_number_v2(100, "default", 0) == "100"

    def test_default_float_with_decimals(self):
        assert format_number_v2(99.5, "default", 1) == "99.5"

    def test_percentage(self):
        assert format_number_v2(50.5, "percentage", 2) == "50.50%"

    def test_currency(self):
        assert format_number_v2(1000, "currency", 2) == "$1,000.00"
