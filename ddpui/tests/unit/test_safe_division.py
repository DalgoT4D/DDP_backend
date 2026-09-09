"""Tests for ddpui.utils.sql_utils.safe_division_expression"""

import pytest

from ddpui.utils.sql_utils import safe_division_expression


class TestSafeDivisionExpression:
    """Wraps division denominators with NULLIF(..., 0)."""

    def test_simple_division(self):
        assert safe_division_expression("a/b") == "a/NULLIF(b, 0)"

    def test_aggregate_division(self):
        expr = "SUM(sessions_happened)/SUM(attended_sessions)"
        expected = "SUM(sessions_happened)/NULLIF(SUM(attended_sessions), 0)"
        assert safe_division_expression(expr) == expected

    def test_aggregate_division_with_multiplier(self):
        expr = "(SUM(sessions_happened)/SUM(attended_sessions))*100"
        expected = "(SUM(sessions_happened)/NULLIF(SUM(attended_sessions), 0))*100"
        assert safe_division_expression(expr) == expected

    def test_parenthesised_denominator(self):
        expr = "SUM(a)/(SUM(b)+SUM(c))"
        expected = "SUM(a)/NULLIF((SUM(b)+SUM(c)), 0)"
        assert safe_division_expression(expr) == expected

    def test_no_division(self):
        expr = "SUM(col_a) + SUM(col_b)"
        assert safe_division_expression(expr) == expr

    def test_count_distinct_division(self):
        expr = "SUM(col_a - col_b) / COUNT(DISTINCT id)"
        expected = "SUM(col_a - col_b) / NULLIF(COUNT(DISTINCT id), 0)"
        assert safe_division_expression(expr) == expected

    def test_numeric_denominator(self):
        expr = "SUM(x)/100"
        expected = "SUM(x)/NULLIF(100, 0)"
        assert safe_division_expression(expr) == expected

    def test_multiple_divisions(self):
        expr = "SUM(a)/SUM(b)/SUM(c)"
        expected = "SUM(a)/NULLIF(SUM(b), 0)/NULLIF(SUM(c), 0)"
        assert safe_division_expression(expr) == expected

    def test_empty_expression(self):
        assert safe_division_expression("") == ""

    def test_plain_aggregate(self):
        expr = "SUM(revenue)"
        assert safe_division_expression(expr) == expr

    def test_spaces_around_division(self):
        expr = "SUM(a) / SUM(b)"
        expected = "SUM(a) / NULLIF(SUM(b), 0)"
        assert safe_division_expression(expr) == expected

    def test_nested_function_denominator(self):
        expr = "SUM(a)/COALESCE(SUM(b), 1)"
        expected = "SUM(a)/NULLIF(COALESCE(SUM(b), 1), 0)"
        assert safe_division_expression(expr) == expected

    def test_qualified_column_name(self):
        expr = "SUM(t.col_a)/SUM(t.col_b)"
        expected = "SUM(t.col_a)/NULLIF(SUM(t.col_b), 0)"
        assert safe_division_expression(expr) == expected

    def test_already_wrapped_nullif(self):
        """NULLIF wrapping is idempotent in effect — double wrapping is harmless."""
        expr = "SUM(a)/NULLIF(SUM(b), 0)"
        expected = "SUM(a)/NULLIF(NULLIF(SUM(b), 0), 0)"
        assert safe_division_expression(expr) == expected

    def test_string_literal_with_slash(self):
        expr = "CONCAT('a/b', col)"
        assert safe_division_expression(expr) == expr

    def test_complex_real_world(self):
        expr = "(SUM(sessions_happened)/SUM(attended_sessions))*100"
        expected = "(SUM(sessions_happened)/NULLIF(SUM(attended_sessions), 0))*100"
        assert safe_division_expression(expr) == expected

    def test_avg_division(self):
        expr = "AVG(price)/COUNT(*)"
        expected = "AVG(price)/NULLIF(COUNT(*), 0)"
        assert safe_division_expression(expr) == expected

    def test_decimal_denominator(self):
        expr = "SUM(x)/3.14"
        expected = "SUM(x)/NULLIF(3.14, 0)"
        assert safe_division_expression(expr) == expected
