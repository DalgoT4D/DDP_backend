"""Tests for _safe_division_expression — NULLIF wrapping for division-by-zero prevention"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import _safe_division_expression


class TestSafeDivisionExpression:
    def test_wraps_aggregate_divisor(self):
        expr = "(SUM(total_volunteers_assigned)/SUM(total_volunteers_in_school))*100"
        result = _safe_division_expression(expr)
        assert result == "(SUM(total_volunteers_assigned)/NULLIF(SUM(total_volunteers_in_school), 0))*100"

    def test_wraps_simple_column_divisor(self):
        result = _safe_division_expression("a/b")
        assert result == "a/NULLIF(b, 0)"

    def test_wraps_parenthesised_divisor(self):
        result = _safe_division_expression("(a + b) / (c + d)")
        assert result == "(a + b) / NULLIF((c + d), 0)"

    def test_wraps_numeric_divisor(self):
        result = _safe_division_expression("total/100")
        assert result == "total/NULLIF(100, 0)"

    def test_expression_without_division_unchanged(self):
        expr = "SUM(total_volunteers_assigned) * 100"
        assert _safe_division_expression(expr) == expr

    def test_nested_function_call_divisor(self):
        result = _safe_division_expression("SUM(a) / COALESCE(SUM(b), 1)")
        assert result == "SUM(a) / NULLIF(COALESCE(SUM(b), 1), 0)"

    def test_multiple_divisions(self):
        result = _safe_division_expression("a / b / c")
        assert result == "a / NULLIF(b, 0) / NULLIF(c, 0)"

    def test_preserves_whitespace_around_slash(self):
        result = _safe_division_expression("SUM(x) / SUM(y)")
        assert result == "SUM(x) / NULLIF(SUM(y), 0)"

    def test_no_whitespace_around_slash(self):
        result = _safe_division_expression("SUM(x)/SUM(y)")
        assert result == "SUM(x)/NULLIF(SUM(y), 0)"

    def test_empty_expression(self):
        assert _safe_division_expression("") == ""

    def test_only_addition(self):
        expr = "SUM(a) + SUM(b)"
        assert _safe_division_expression(expr) == expr

    def test_decimal_divisor(self):
        result = _safe_division_expression("total / 3.14")
        assert result == "total / NULLIF(3.14, 0)"

    def test_complex_real_world_expression(self):
        expr = "(SUM(completed_sessions) / SUM(total_sessions)) * 100"
        result = _safe_division_expression(expr)
        assert result == "(SUM(completed_sessions) / NULLIF(SUM(total_sessions), 0)) * 100"
