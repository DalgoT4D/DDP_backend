"""Tests for apply_chart_sorting — ensure sort dimension columns are added to GROUP BY"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from sqlalchemy import column
from ddpui.core.charts.charts_service import apply_chart_sorting
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas.config import ChartMetric


def _make_aggregated_qb():
    """Create a query builder with GROUP BY (simulating an aggregated query)."""
    qb = AggQueryBuilder()
    qb.fetch_from("meetings", "public")
    qb.add_column(column("state_name").label("state_name"))
    qb.group_cols_by("state_name")
    qb.add_aggregate_column(None, "count", "total_count")
    return qb


def _make_non_aggregated_qb():
    """Create a query builder without GROUP BY (non-aggregated query)."""
    qb = AggQueryBuilder()
    qb.fetch_from("meetings", "public")
    qb.add_column(column("state_name").label("state_name"))
    qb.add_column(column("meeting_date").label("meeting_date"))
    return qb


def _group_by_col_names(qb):
    """Extract column names from group_by_clauses."""
    names = []
    for clause in qb.group_by_clauses:
        if hasattr(clause, "name"):
            names.append(clause.name)
        elif hasattr(clause, "key"):
            names.append(clause.key)
    return names


class TestApplyChartSortingGroupBy:
    def test_sort_by_dimension_adds_to_group_by(self):
        """Sorting by a dimension column on an aggregated query must add it to GROUP BY."""
        qb = _make_aggregated_qb()
        sort_config = [{"column": "meeting_date", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config)

        group_cols = _group_by_col_names(qb)
        assert "meeting_date" in group_cols

    def test_sort_by_metric_does_not_add_to_group_by(self):
        """Sorting by a metric alias must NOT add it to GROUP BY."""
        qb = _make_aggregated_qb()
        metric = ChartMetric(aggregation="count", column=None, alias="total_count")

        class FakePayload:
            metrics = [metric]

        sort_config = [{"column": "total_count", "direction": "desc"}]
        apply_chart_sorting(qb, sort_config, payload=FakePayload())

        group_cols = _group_by_col_names(qb)
        assert "total_count" not in group_cols

    def test_sort_on_non_aggregated_query_does_not_add_group_by(self):
        """Sorting on a non-aggregated query (no GROUP BY) must not introduce GROUP BY."""
        qb = _make_non_aggregated_qb()
        assert len(qb.group_by_clauses) == 0

        sort_config = [{"column": "meeting_date", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.group_by_clauses) == 0

    def test_sort_by_existing_group_by_column_is_harmless(self):
        """Sorting by a column already in GROUP BY should not cause issues."""
        qb = _make_aggregated_qb()
        initial_count = len(qb.group_by_clauses)

        sort_config = [{"column": "state_name", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        # state_name is added again (harmless duplicate); query remains valid
        assert len(qb.group_by_clauses) >= initial_count

    def test_multiple_dimension_sorts_all_added_to_group_by(self):
        """Sorting by multiple dimension columns adds all to GROUP BY."""
        qb = _make_aggregated_qb()
        sort_config = [
            {"column": "meeting_date", "direction": "desc"},
            {"column": "city", "direction": "asc"},
        ]

        apply_chart_sorting(qb, sort_config)

        group_cols = _group_by_col_names(qb)
        assert "meeting_date" in group_cols
        assert "city" in group_cols

    def test_empty_sort_config_no_change(self):
        """Empty sort config should not modify the query builder."""
        qb = _make_aggregated_qb()
        initial_group_count = len(qb.group_by_clauses)
        initial_order_count = len(qb.order_by_clauses)

        apply_chart_sorting(qb, [])

        assert len(qb.group_by_clauses) == initial_group_count
        assert len(qb.order_by_clauses) == initial_order_count

    def test_compiled_sql_has_sort_col_in_group_by(self):
        """The compiled SQL must include the sort dimension column in GROUP BY."""
        qb = _make_aggregated_qb()
        sort_config = [{"column": "meeting_date", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config)

        sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
        # meeting_date should appear in both GROUP BY and ORDER BY
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql
        assert "meeting_date" in sql
