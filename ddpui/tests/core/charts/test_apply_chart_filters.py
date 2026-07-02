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

    # --- Empty-string on non-text columns ---

    def test_numeric_equals_empty_string_becomes_is_null(self):
        """numeric column with equals '' must become IS NULL"""
        sql = get_where_sql([make_filter("AI_Signal", "equals", "", "numeric")])
        assert len(sql) == 1
        assert "IS NULL" in sql[0].upper()

    def test_numeric_not_equals_empty_string_becomes_is_not_null(self):
        """numeric column with not_equals '' must become IS NOT NULL"""
        sql = get_where_sql([make_filter("AI_Signal", "not_equals", "", "numeric")])
        assert len(sql) == 1
        assert "IS NOT NULL" in sql[0].upper()

    def test_integer_equals_empty_string_becomes_is_null(self):
        """integer column with equals '' must become IS NULL"""
        sql = get_where_sql([make_filter("count_col", "equals", "", "integer")])
        assert len(sql) == 1
        assert "IS NULL" in sql[0].upper()

    def test_boolean_equals_empty_string_becomes_is_null(self):
        """boolean column with equals '' must become IS NULL"""
        sql = get_where_sql([make_filter("is_active", "equals", "", "boolean")])
        assert len(sql) == 1
        assert "IS NULL" in sql[0].upper()

    def test_date_equals_empty_string_becomes_is_null(self):
        """date column with equals '' must become IS NULL (not a valid date)"""
        sql = get_where_sql([make_filter("birth_date", "equals", "", "date")])
        assert len(sql) == 1
        assert "IS NULL" in sql[0].upper()

    def test_numeric_greater_than_empty_string_skipped(self):
        """numeric column with greater_than '' is meaningless and should be skipped"""
        sql = get_where_sql([make_filter("AI_Signal", "greater_than", "", "numeric")])
        assert len(sql) == 0

    def test_numeric_less_than_empty_string_skipped(self):
        """numeric column with less_than '' is meaningless and should be skipped"""
        sql = get_where_sql([make_filter("AI_Signal", "less_than", "", "numeric")])
        assert len(sql) == 0

    def test_text_equals_empty_string_preserved(self):
        """text/varchar columns should still allow equals '' (valid SQL)"""
        for dtype in ["varchar", "text", "char", "character varying"]:
            sql = get_where_sql([make_filter("name", "equals", "", dtype)])
            assert len(sql) == 1
            assert "IS NULL" not in sql[0].upper(), f"Failed for data_type={dtype}"

    def test_no_data_type_empty_string_preserved(self):
        """When data_type is missing, empty string value is passed through as-is"""
        sql = get_where_sql([{"column": "col", "operator": "equals", "value": ""}])
        assert len(sql) == 1
        assert "IS NULL" not in sql[0].upper()

    def test_bigint_equals_empty_string_becomes_is_null(self):
        """bigint column with equals '' must become IS NULL"""
        for dtype in ["bigint", "float", "double precision", "real", "decimal"]:
            sql = get_where_sql([make_filter("num_col", "equals", "", dtype)])
            assert len(sql) == 1
            assert "IS NULL" in sql[0].upper(), f"Failed for data_type={dtype}"
