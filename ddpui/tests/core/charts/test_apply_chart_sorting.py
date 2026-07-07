"""Tests for apply_chart_sorting — GROUP BY guard for non-aggregate sort columns"""

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


def _group_col_names(qb: AggQueryBuilder) -> set:
    """Return the set of column names present in the builder's GROUP BY clauses."""
    names = set()
    for gc in qb.group_by_clauses:
        if hasattr(gc, "name"):
            names.add(gc.name)
    return names


class TestApplyChartSorting:
    """Tests that apply_chart_sorting adds sort columns to GROUP BY when needed."""

    def test_sort_by_non_grouped_column_adds_to_group_by(self):
        """Sorting by a bare column not already in GROUP BY must add it."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")
        qb.add_aggregate_column("amount", "sum", "total_amount")

        sort_config = [{"column": "meeting_date", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert "meeting_date" in _group_col_names(qb)
        assert len(qb.order_by_clauses) == 1

    def test_sort_by_already_grouped_column_no_duplicate(self):
        """Sorting by a column already in GROUP BY must NOT duplicate it."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")
        original_group_count = len(qb.group_by_clauses)

        sort_config = [{"column": "state", "direction": "desc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.group_by_clauses) == original_group_count
        assert len(qb.order_by_clauses) == 1

    def test_sort_by_metric_alias_not_added_to_group_by(self):
        """Sorting by a metric alias must NOT add it to GROUP BY."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")

        metrics = [ChartMetric(column="amount", aggregation="sum", alias="total_amount")]

        class FakePayload:
            pass

        payload = FakePayload()
        payload.metrics = metrics

        sort_config = [{"column": "total_amount", "direction": "desc"}]
        apply_chart_sorting(qb, sort_config, payload)

        assert "total_amount" not in _group_col_names(qb)
        assert len(qb.order_by_clauses) == 1

    def test_no_group_by_sort_does_not_add_group_by(self):
        """When query has no GROUP BY, sorting must not introduce one."""
        qb = AggQueryBuilder()
        assert len(qb.group_by_clauses) == 0

        sort_config = [{"column": "meeting_date", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.group_by_clauses) == 0
        assert len(qb.order_by_clauses) == 1

    def test_multiple_sort_columns_all_added_to_group_by(self):
        """Multiple non-grouped sort columns must all be added to GROUP BY."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")

        sort_config = [
            {"column": "meeting_date", "direction": "asc"},
            {"column": "created_at", "direction": "desc"},
        ]
        apply_chart_sorting(qb, sort_config)

        group_names = _group_col_names(qb)
        assert "meeting_date" in group_names
        assert "created_at" in group_names
        assert len(qb.order_by_clauses) == 2

    def test_sort_column_added_to_select(self):
        """A non-grouped sort column should also be added to SELECT."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")
        original_col_count = len(qb.column_clauses)

        sort_config = [{"column": "meeting_date", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.column_clauses) == original_col_count + 1

    def test_already_grouped_sort_column_not_added_to_select(self):
        """A sort column already in GROUP BY should NOT add a duplicate SELECT."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")
        original_col_count = len(qb.column_clauses)

        sort_config = [{"column": "state", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.column_clauses) == original_col_count

    def test_empty_sort_config_is_noop(self):
        """An empty sort config must not modify the query builder."""
        qb = AggQueryBuilder()
        qb.group_cols_by("state")

        apply_chart_sorting(qb, [])

        assert len(qb.order_by_clauses) == 0

    def test_labeled_group_by_column_recognized(self):
        """Sort column matching a labeled GROUP BY entry must not duplicate."""
        qb = AggQueryBuilder()
        labeled_col = column("meeting_date").label("meeting_date")
        qb.group_cols_by(labeled_col)
        original_group_count = len(qb.group_by_clauses)

        sort_config = [{"column": "meeting_date", "direction": "asc"}]
        apply_chart_sorting(qb, sort_config)

        assert len(qb.group_by_clauses) == original_group_count
