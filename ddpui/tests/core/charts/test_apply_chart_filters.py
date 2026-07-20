"""Tests for apply_chart_filters — timestamp day-range filter handling and empty value skipping"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.charts_service import apply_chart_filters, apply_dashboard_filters
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


class TestEmptyValueFiltersSkipped:
    """Filters with empty string values must be silently skipped to avoid
    invalid SQL such as ``WHERE meeting_date = ''`` on date columns."""

    def test_chart_filter_empty_string_equals_skipped(self):
        """equals filter with empty string value produces no WHERE clause"""
        sql = get_where_sql([make_filter("meeting_date", "equals", "", "date")])
        assert len(sql) == 0

    def test_chart_filter_whitespace_only_equals_skipped(self):
        """equals filter with whitespace-only value is also skipped"""
        sql = get_where_sql([make_filter("meeting_date", "equals", "   ", "date")])
        assert len(sql) == 0

    def test_chart_filter_empty_string_timestamp_skipped(self):
        """timestamp column with empty string value is skipped"""
        sql = get_where_sql([make_filter("created_at", "equals", "", "timestamp")])
        assert len(sql) == 0

    def test_chart_filter_empty_string_comparison_operators_skipped(self):
        """comparison operators with empty string value are all skipped"""
        for op in ["not_equals", "greater_than", "less_than", "greater_than_equal", "less_than_equal"]:
            sql = get_where_sql([make_filter("meeting_date", op, "", "date")])
            assert len(sql) == 0, f"operator {op} should skip empty value"

    def test_chart_filter_empty_string_like_skipped(self):
        """like/contains with empty string value are skipped"""
        for op in ["like", "like_case_insensitive", "contains", "not_contains"]:
            sql = get_where_sql([make_filter("name", op, "", "varchar")])
            assert len(sql) == 0, f"operator {op} should skip empty value"

    def test_chart_filter_is_null_with_empty_value_still_works(self):
        """is_null / is_not_null must NOT be skipped even when value is empty"""
        for op in ["is_null", "is_not_null"]:
            sql = get_where_sql([make_filter("meeting_date", op, "", "date")])
            assert len(sql) == 1, f"operator {op} should not be skipped"

    def test_chart_filter_non_empty_value_still_applied(self):
        """non-empty value on date column still produces a WHERE clause"""
        sql = get_where_sql([make_filter("meeting_date", "equals", "2026-06-15", "date")])
        assert len(sql) == 1
        assert "2026-06-15" in sql[0]

    def test_dashboard_filter_empty_string_value_skipped(self):
        """dashboard filter with empty string value is skipped"""
        qb = AggQueryBuilder()
        apply_dashboard_filters(qb, [
            {"column": "meeting_date", "type": "value", "value": ""},
        ])
        assert len(qb.where_clauses) == 0

    def test_dashboard_filter_empty_string_datetime_skipped(self):
        """dashboard datetime filter with empty string value is skipped"""
        qb = AggQueryBuilder()
        apply_dashboard_filters(qb, [
            {"column": "meeting_date", "type": "datetime", "value": ""},
        ])
        assert len(qb.where_clauses) == 0

    def test_dashboard_filter_none_value_skipped(self):
        """dashboard filter with None value is still skipped (existing behavior)"""
        qb = AggQueryBuilder()
        apply_dashboard_filters(qb, [
            {"column": "meeting_date", "type": "datetime", "value": None},
        ])
        assert len(qb.where_clauses) == 0

    def test_dashboard_filter_list_with_empty_strings_cleaned(self):
        """dashboard value filter with list containing empty strings drops them"""
        qb = AggQueryBuilder()
        apply_dashboard_filters(qb, [
            {"column": "status", "type": "value", "value": ["active", "", "  "]},
        ])
        assert len(qb.where_clauses) == 1
        compiled = str(qb.where_clauses[0].compile(compile_kwargs={"literal_binds": True}))
        assert "active" in compiled

    def test_dashboard_filter_list_all_empty_strings_skipped(self):
        """dashboard value filter where all list items are empty produces no clause"""
        qb = AggQueryBuilder()
        apply_dashboard_filters(qb, [
            {"column": "status", "type": "value", "value": ["", "  "]},
        ])
        assert len(qb.where_clauses) == 0
