"""Tests for apply_chart_filters — timestamp day-range filter handling"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.charts_service import apply_chart_filters
from ddpui.core.datainsights.query_builder import AggQueryBuilder

pytestmark = pytest.mark.django_db


def make_filter(col, operator, value, data_type="varchar"):
    return {"column": col, "operator": operator, "value": value, "data_type": data_type}


def get_where_sql(filters):
    """Apply filters and return compiled WHERE clauses as strings."""
    qb = AggQueryBuilder()
    apply_chart_filters(qb, filters)
    return [
        str(clause.compile(compile_kwargs={"literal_binds": True})) for clause in qb.where_clauses
    ]


class TestApplyChartFilters:
    def test_equals_timestamp_generates_day_range(self):
        """timestamp equals must match full day using >= start AND < next day"""
        sql = get_where_sql([make_filter("created_at", "equals", "2026-06-15", "timestamp")])
        assert len(sql) == 1
        assert "2026-06-15" in sql[0]
        assert "2026-06-16" in sql[0]

    def test_not_equals_timestamp_excludes_full_day(self):
        """timestamp not_equals must exclude entire day using OR range"""
        sql = get_where_sql([make_filter("created_at", "not_equals", "2026-06-15", "timestamp")])
        assert len(sql) == 1
        assert "2026-06-15" in sql[0]
        assert "2026-06-16" in sql[0]

    def test_greater_than_timestamp_starts_from_next_day(self):
        """timestamp greater_than must start from next day to exclude the selected day"""
        sql = get_where_sql([make_filter("created_at", "greater_than", "2026-06-15", "timestamp")])
        assert len(sql) == 1
        assert "2026-06-16" in sql[0]

    def test_less_than_timestamp_no_shift_needed(self):
        """timestamp less_than works correctly — midnight is already the right boundary"""
        sql = get_where_sql([make_filter("created_at", "less_than", "2026-06-15", "timestamp")])
        assert len(sql) == 1
        assert "2026-06-15" in sql[0]
        assert "2026-06-16" not in sql[0]

    def test_greater_than_equal_timestamp_no_shift_needed(self):
        """timestamp greater_than_equal works correctly from start of selected day"""
        sql = get_where_sql(
            [make_filter("created_at", "greater_than_equal", "2026-06-15", "timestamp")]
        )
        assert len(sql) == 1
        assert "2026-06-15" in sql[0]
        assert "2026-06-16" not in sql[0]

    def test_less_than_equal_timestamp_includes_full_day(self):
        """timestamp less_than_equal must shift to next day to include entire selected day"""
        sql = get_where_sql(
            [make_filter("created_at", "less_than_equal", "2026-06-15", "timestamp")]
        )
        assert len(sql) == 1
        assert "2026-06-16" in sql[0]

    def test_non_timestamp_column_unaffected(self):
        """date-only column uses simple equality — no range logic applied"""
        sql = get_where_sql([make_filter("birth_date", "equals", "2026-06-15", "date")])
        assert len(sql) == 1
        assert "2026-06-16" not in sql[0]

    def test_multiple_equals_same_column_grouped(self):
        """multiple equals on same non-timestamp column merged into one OR clause"""
        filters = [
            make_filter("status", "equals", "active"),
            make_filter("status", "equals", "pending"),
        ]
        sql = get_where_sql(filters)
        assert len(sql) == 1
        assert "active" in sql[0]
        assert "pending" in sql[0]

    def test_timestamp_equals_not_grouped(self):
        """timestamp equals filters are never grouped — each gets its own range clause"""
        filters = [
            make_filter("created_at", "equals", "2026-06-15", "timestamp"),
            make_filter("created_at", "equals", "2026-06-16", "timestamp"),
        ]
        sql = get_where_sql(filters)
        assert len(sql) == 2

    def test_timestamptz_also_uses_range(self):
        """timestamptz and datetime columns also use day-range logic"""
        for dtype in ["timestamptz", "datetime", "timestamp with time zone"]:
            sql = get_where_sql([make_filter("created_at", "equals", "2026-06-15", dtype)])
            assert len(sql) == 1
            assert "2026-06-16" in sql[0], f"Failed for data_type={dtype}"

    def test_null_operators_unaffected(self):
        """is_null and is_not_null work the same for all column types"""
        for operator in ["is_null", "is_not_null"]:
            sql = get_where_sql([make_filter("created_at", operator, "", "timestamp")])
            assert len(sql) == 1


class TestNumericFilterCoercion:
    """Tests for numeric value coercion in apply_chart_filters."""

    def test_empty_string_on_integer_column_is_skipped(self):
        """Empty string value on an integer column must be silently dropped."""
        sql = get_where_sql(
            [make_filter("active_work_orders", "greater_than_equal", "", "integer")]
        )
        assert len(sql) == 0

    def test_empty_string_on_bigint_column_is_skipped(self):
        """Empty string value on a bigint column must be silently dropped."""
        sql = get_where_sql(
            [make_filter("total_count", "less_than", "", "bigint")]
        )
        assert len(sql) == 0

    def test_whitespace_only_on_numeric_column_is_skipped(self):
        """Whitespace-only value on a numeric column must be silently dropped."""
        sql = get_where_sql(
            [make_filter("amount", "equals", "   ", "numeric")]
        )
        assert len(sql) == 0

    def test_valid_integer_string_is_coerced(self):
        """String '42' on an integer column must be coerced to integer 42."""
        sql = get_where_sql(
            [make_filter("active_work_orders", "greater_than_equal", "42", "integer")]
        )
        assert len(sql) == 1
        assert "42" in sql[0]

    def test_valid_float_string_is_coerced(self):
        """String '3.14' on a float column must be coerced to float."""
        sql = get_where_sql(
            [make_filter("score", "greater_than", "3.14", "float")]
        )
        assert len(sql) == 1
        assert "3.14" in sql[0]

    def test_non_numeric_string_on_integer_column_is_skipped(self):
        """Non-numeric string on an integer column must be silently dropped."""
        sql = get_where_sql(
            [make_filter("count", "equals", "abc", "integer")]
        )
        assert len(sql) == 0

    def test_numeric_value_already_int_passes_through(self):
        """Integer value on an integer column passes through unchanged."""
        sql = get_where_sql(
            [make_filter("count", "equals", 42, "integer")]
        )
        assert len(sql) == 1
        assert "42" in sql[0]

    def test_empty_string_on_varchar_column_is_kept(self):
        """Empty string on a varchar column should NOT be skipped."""
        sql = get_where_sql(
            [make_filter("name", "equals", "", "varchar")]
        )
        assert len(sql) == 1

    def test_empty_string_equals_on_integer_grouped_is_skipped(self):
        """Empty string in a grouped equals on integer column is skipped."""
        sql = get_where_sql(
            [make_filter("count", "equals", "", "integer")]
        )
        assert len(sql) == 0

    def test_mixed_valid_and_empty_integer_filters(self):
        """Only the valid filter should produce a WHERE clause."""
        filters = [
            make_filter("count", "greater_than", "", "integer"),
            make_filter("count", "less_than", "100", "integer"),
        ]
        sql = get_where_sql(filters)
        assert len(sql) == 1
        assert "100" in sql[0]

    def test_empty_string_on_numeric_is_null_still_works(self):
        """is_null on a numeric column with empty value should still work."""
        sql = get_where_sql(
            [make_filter("count", "is_null", "", "integer")]
        )
        assert len(sql) == 1

    def test_all_numeric_types_reject_empty_string(self):
        """All numeric data types should reject empty string values."""
        for dtype in [
            "integer", "bigint", "smallint", "numeric", "decimal",
            "double", "real", "float", "double precision", "money",
        ]:
            sql = get_where_sql(
                [make_filter("col", "greater_than", "", dtype)]
            )
            assert len(sql) == 0, f"Empty string not rejected for data_type={dtype}"
