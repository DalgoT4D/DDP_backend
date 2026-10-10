import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import Mock

import pytest

from ddpui.core.charts.charts_service import apply_chart_sorting
from ddpui.core.datainsights.query_builder import AggQueryBuilder


def _make_payload(metrics=None):
    payload = Mock()
    payload.metrics = metrics or []
    return payload


def _make_metric(alias, aggregation="sum", col="amount"):
    m = Mock()
    m.alias = alias
    m.aggregation = aggregation
    m.column = col
    return m


class TestApplyChartSorting:
    def test_sort_by_grouped_dimension_is_allowed(self):
        qb = AggQueryBuilder()
        qb.group_cols_by("city")
        apply_chart_sorting(qb, [{"column": "city", "direction": "asc"}])
        assert len(qb.order_by_clauses) == 1

    def test_sort_by_non_grouped_column_is_skipped(self):
        """The bug: sorting by a column not in GROUP BY caused a PostgreSQL GroupingError."""
        qb = AggQueryBuilder()
        qb.group_cols_by("city")
        apply_chart_sorting(qb, [{"column": "donor_name", "direction": "asc"}])
        assert len(qb.order_by_clauses) == 0

    def test_sort_by_metric_alias_is_allowed(self):
        qb = AggQueryBuilder()
        qb.group_cols_by("city")
        metric = _make_metric(alias="total_amount")
        payload = _make_payload(metrics=[metric])
        apply_chart_sorting(qb, [{"column": "total_amount", "direction": "desc"}], payload)
        assert len(qb.order_by_clauses) == 1

    def test_sort_without_group_by_allows_any_column(self):
        """Non-aggregate queries have no GROUP BY so any column is valid."""
        qb = AggQueryBuilder()
        apply_chart_sorting(qb, [{"column": "donor_name", "direction": "asc"}])
        assert len(qb.order_by_clauses) == 1

    def test_mixed_valid_and_invalid_sort_columns(self):
        qb = AggQueryBuilder()
        qb.group_cols_by("city", "state")
        metric = _make_metric(alias="total")
        payload = _make_payload(metrics=[metric])
        sort_config = [
            {"column": "city", "direction": "asc"},
            {"column": "donor_name", "direction": "desc"},  # not grouped
            {"column": "total", "direction": "desc"},  # metric
            {"column": "state", "direction": "asc"},
        ]
        apply_chart_sorting(qb, sort_config, payload)
        # city, total, state should be added; donor_name should be skipped
        assert len(qb.order_by_clauses) == 3

    def test_empty_sort_config(self):
        qb = AggQueryBuilder()
        result = apply_chart_sorting(qb, [])
        assert result is qb
        assert len(qb.order_by_clauses) == 0

    def test_none_sort_config(self):
        qb = AggQueryBuilder()
        result = apply_chart_sorting(qb, None)
        assert result is qb
        assert len(qb.order_by_clauses) == 0
