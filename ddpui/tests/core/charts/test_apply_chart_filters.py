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

    def test_empty_string_value_skipped(self):
        """Filters with empty string values are silently skipped"""
        for operator in [
            "greater_than",
            "less_than",
            "greater_than_equal",
            "less_than_equal",
            "equals",
            "not_equals",
            "like",
            "like_case_insensitive",
            "contains",
            "not_contains",
        ]:
            sql = get_where_sql([make_filter("date_col", operator, "", "date")])
            assert len(sql) == 0, f"operator={operator} should skip empty value"

    def test_whitespace_only_value_skipped(self):
        """Filters with whitespace-only values are silently skipped"""
        sql = get_where_sql([make_filter("date_col", "greater_than", "   ", "date")])
        assert len(sql) == 0

    def test_none_value_skipped(self):
        """Filters with None values are silently skipped"""
        sql = get_where_sql([make_filter("date_col", "greater_than", None, "date")])
        assert len(sql) == 0

    def test_empty_value_skipped_with_valid_filters_kept(self):
        """Empty-value filters are skipped but valid filters still apply"""
        filters = [
            make_filter("date_col", "greater_than", "", "date"),
            make_filter("status", "equals", "active"),
        ]
        sql = get_where_sql(filters)
        assert len(sql) == 1
        assert "active" in sql[0]

    def test_null_operators_still_work_with_empty_value(self):
        """is_null and is_not_null must not be skipped even with empty/None values"""
        for value in ["", None, "   "]:
            for operator in ["is_null", "is_not_null"]:
                sql = get_where_sql([make_filter("date_col", operator, value, "date")])
                assert len(sql) == 1, (
                    f"operator={operator} with value={value!r} should NOT be skipped"
                )
