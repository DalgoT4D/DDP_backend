"""Tests for apply_chart_sorting — validation of sort columns against GROUP BY dimensions"""

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




def make_payload(chart_type="bar", dimension_col=None, dimensions=None, metrics=None):
    """Create a minimal ChartDataPayload for testing."""
    return ChartDataPayload(
        chart_type=chart_type,
        schema_name="public",
        table_name="test_table",
        dimension_col=dimension_col,
        dimensions=dimensions,
        metrics=metrics,
    )


def make_metric(column=None, aggregation="sum", alias=None, column_expression=None):
    """Create a ChartMetric for testing."""
    return ChartMetric(
        column=column,
        aggregation=aggregation,
        alias=alias,
        column_expression=column_expression,
    )


def get_order_columns(query_builder):
    """Extract ORDER BY column names from the query builder."""
    return [
        str(clause.compile(compile_kwargs={"literal_binds": True}))
        for clause in query_builder.order_by_clauses
    ]


class TestApplyChartSorting:
    def test_sort_by_valid_dimension_accepted(self):
        """Sorting by a column that IS in dimensions should be accepted."""
        qb = AggQueryBuilder()
        payload = make_payload(dimension_col="city", metrics=[make_metric("amount")])
        sort_config = [{"column": "city", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "city" in order_sql[0]

    def test_sort_by_invalid_column_rejected(self):
        """Sorting by a column NOT in dimensions or metrics should be skipped."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="city", metrics=[make_metric("amount", alias="total_amount")]
        )
        sort_config = [{"column": "pct_target_missed", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 0

    def test_sort_by_metric_alias_accepted(self):
        """Sorting by a metric alias should be accepted."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="city", metrics=[make_metric("amount", alias="total_amount")]
        )
        sort_config = [{"column": "total_amount", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "total_amount" in order_sql[0]

    def test_sort_by_count_metric_accepted(self):
        """Sorting by a count(*) metric alias should be accepted."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="city",
            metrics=[make_metric(column=None, aggregation="count", alias="row_count")],
        )
        sort_config = [{"column": "row_count", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "count_all_row_count" in order_sql[0]

    def test_mixed_valid_and_invalid_sort_columns(self):
        """Only valid sort columns should be kept; invalid ones skipped."""
        qb = AggQueryBuilder()
        payload = make_payload(
            dimension_col="city", metrics=[make_metric("amount", alias="total_amount")]
        )
        sort_config = [
            {"column": "city", "direction": "asc"},
            {"column": "pct_target_missed", "direction": "desc"},
            {"column": "total_amount", "direction": "desc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 2
        assert "city" in order_sql[0]
        assert "total_amount" in order_sql[1]

    def test_sort_with_extra_dimension_accepted(self):
        """Sorting by an extra_dimension column should be accepted."""
        qb = AggQueryBuilder()
        payload = make_payload(dimension_col="city", metrics=[make_metric("amount")])
        payload.extra_dimension = "region"
        sort_config = [{"column": "region", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "region" in order_sql[0]

    def test_sort_with_table_chart_dimensions(self):
        """Table chart dimensions list should be used for validation."""
        qb = AggQueryBuilder()
        payload = make_payload(
            chart_type="table",
            dimensions=["city", "region"],
            metrics=[make_metric("amount")],
        )
        sort_config = [
            {"column": "city", "direction": "asc"},
            {"column": "nonexistent_col", "direction": "desc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "city" in order_sql[0]

    def test_empty_sort_config_returns_unchanged(self):
        """Empty sort config should not modify query builder."""
        qb = AggQueryBuilder()
        payload = make_payload(dimension_col="city")

        result = apply_chart_sorting(qb, [], payload)
        assert result is qb
        assert len(qb.order_by_clauses) == 0

    def test_no_payload_allows_any_sort_column(self):
        """When payload is None, any sort column should pass through."""
        qb = AggQueryBuilder()
        sort_config = [{"column": "any_column", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, None)
        order_sql = get_order_columns(qb)

        assert len(order_sql) == 1
        assert "any_column" in order_sql[0]
