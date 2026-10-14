"""Tests for apply_chart_sorting — validates sort columns against GROUP BY dimensions."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.charts.charts_service import apply_chart_sorting
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas.data import ChartDataPayload
from ddpui.schemas.chart_schemas.config import ChartMetric

def make_payload(chart_type="bar", dimensions=None, dimension_col=None, metrics=None):
    kwargs = {
        "chart_type": chart_type,
        "schema_name": "public",
        "table_name": "test_table",
    }
    if dimensions is not None:
        kwargs["dimensions"] = dimensions
    if dimension_col is not None:
        kwargs["dimension_col"] = dimension_col
    if metrics is not None:
        kwargs["metrics"] = metrics
    return ChartDataPayload(**kwargs)


def make_metric(aggregation="sum", col="amount", alias="total_amount"):
    return ChartMetric(aggregation=aggregation, column=col, alias=alias)


class TestApplyChartSorting:
    def test_sort_by_valid_dimension_is_kept(self):
        """Sorting by a column in GROUP BY dimensions should work."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="region",
            metrics=[make_metric()],
        )
        sort_config = [{"column": "region", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1

    def test_sort_by_metric_alias_is_kept(self):
        """Sorting by a metric alias should work."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="region",
            metrics=[make_metric()],
        )
        sort_config = [{"column": "total_amount", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1

    def test_sort_by_non_grouped_column_is_skipped(self):
        """Sorting by a column not in GROUP BY or metrics should be skipped."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="region",
            metrics=[make_metric()],
        )
        sort_config = [{"column": "donor_name", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 0

    def test_non_aggregated_table_sort_is_allowed(self):
        """Non-aggregated table charts (no metrics) should allow sorting by any column."""
        qb = AggQueryBuilder()
        payload = make_payload(
            chart_type="table",
            dimensions=["region", "donor_name"],
        )
        sort_config = [{"column": "donor_name", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1

    def test_mixed_valid_and_invalid_sort_columns(self):
        """Only valid sort columns should be kept; invalid ones skipped."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="region",
            metrics=[make_metric()],
        )
        sort_config = [
            {"column": "region", "direction": "asc"},
            {"column": "donor_name", "direction": "desc"},
            {"column": "total_amount", "direction": "desc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)

        # region (dimension) and total_amount (metric alias) kept; donor_name skipped
        assert len(qb.order_by_clauses) == 2

    def test_empty_sort_config_is_noop(self):
        """Empty sort config should not add any order clauses."""
        qb = AggQueryBuilder()
        payload = make_payload(dimension_col="region", metrics=[make_metric()])

        apply_chart_sorting(qb, [], payload)

        assert len(qb.order_by_clauses) == 0

    def test_no_payload_allows_any_sort(self):
        """When payload is None, any sort column should be allowed."""
        qb = AggQueryBuilder()
        sort_config = [{"column": "anything", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload=None)

        assert len(qb.order_by_clauses) == 1

    def test_table_chart_with_multiple_dimensions_and_metrics(self):
        """Table chart with metrics should only allow sorting by dimension or metric alias."""
        qb = AggQueryBuilder()
        payload = make_payload(
            chart_type="table",
            dimensions=["region", "status"],
            metrics=[make_metric()],
        )
        sort_config = [
            {"column": "region", "direction": "asc"},
            {"column": "unrelated_col", "direction": "desc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)

        # region kept, unrelated_col skipped
        assert len(qb.order_by_clauses) == 1

    def test_extra_dimension_is_valid_sort_target(self):
        """extra_dimension should be recognized as a valid GROUP BY dimension."""
        qb = AggQueryBuilder()
        payload = make_payload(
            chart_type="bar",
            dimension_col="region",
            metrics=[make_metric()],
        )
        payload.extra_dimension = "status"
        sort_config = [{"column": "status", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1
