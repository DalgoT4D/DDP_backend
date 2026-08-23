"""Tests for apply_chart_sorting — validation that sort columns must be
valid dimensions or metric aliases to prevent PostgreSQL GroupingError."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.charts.charts_service import apply_chart_sorting
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas.config import ChartMetric
from ddpui.schemas.chart_schemas.data import ChartDataPayload


def _make_payload(
    dimension_col=None,
    extra_dimension=None,
    dimensions=None,
    metrics=None,
    chart_type="bar",
):
    return ChartDataPayload(
        chart_type=chart_type,
        schema_name="public",
        table_name="test_table",
        dimension_col=dimension_col,
        extra_dimension=extra_dimension,
        dimensions=dimensions,
        metrics=metrics,
    )


def _order_clause_strings(qb):
    """Compile each ORDER BY clause to a plain string for easy assertion."""
    return [str(clause.compile(compile_kwargs={"literal_binds": True})) for clause in qb.order_by_clauses]


class TestApplyChartSorting:
    def test_sort_by_valid_dimension_is_applied(self):
        """Sorting on a column that IS a dimension should be allowed."""
        qb = AggQueryBuilder()
        payload = _make_payload(dimension_col="state_name")
        sort_config = [{"column": "state_name", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        clauses = _order_clause_strings(qb)
        assert len(clauses) == 1
        assert "state_name" in clauses[0]

    def test_sort_by_invalid_column_is_skipped(self):
        """Sorting on a column NOT in dimensions or metrics must be skipped
        to avoid PostgreSQL GroupingError."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column="id", aggregation="count", alias="Total Count")],
        )
        sort_config = [{"column": "deleted_at", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 0

    def test_sort_by_metric_alias_is_applied(self):
        """Sorting by a metric alias should generate the correct SQL alias."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue")],
        )
        sort_config = [{"column": "Total Revenue", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)

        clauses = _order_clause_strings(qb)
        assert len(clauses) == 1
        assert "Total Revenue" in clauses[0]

    def test_sort_by_count_all_metric(self):
        """Sorting by a count(*) metric should use the count_all_ prefixed alias."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column=None, aggregation="count", alias="Row Count")],
        )
        sort_config = [{"column": "Row Count", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)

        clauses = _order_clause_strings(qb)
        assert len(clauses) == 1
        assert "count_all_Row Count" in clauses[0]

    def test_mixed_valid_and_invalid_sorts(self):
        """Only valid sort columns should be kept; invalid ones are dropped."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue")],
        )
        sort_config = [
            {"column": "deleted_at", "direction": "asc"},
            {"column": "state_name", "direction": "asc"},
            {"column": "Total Revenue", "direction": "desc"},
            {"column": "unknown_col", "direction": "desc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 2
        clauses = _order_clause_strings(qb)
        assert "state_name" in clauses[0]
        assert "Total Revenue" in clauses[1]

    def test_sort_by_metric_sql_alias_without_display_alias(self):
        """When a metric has no alias, the sort column may match the auto-generated
        SQL alias (e.g. sum_revenue). This is a valid metric alias."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column="revenue", aggregation="sum")],
        )
        # The auto-generated SQL alias is "sum_revenue"
        sort_config = [{"column": "sum_revenue", "direction": "desc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1
        clauses = _order_clause_strings(qb)
        assert "sum_revenue" in clauses[0]

    def test_sort_by_extra_dimension_is_valid(self):
        """extra_dimension is a valid dimension for non-table charts."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            extra_dimension="region",
        )
        sort_config = [{"column": "region", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1
        clauses = _order_clause_strings(qb)
        assert "region" in clauses[0]

    def test_empty_sort_config(self):
        """Empty sort config should not modify the query builder."""
        qb = AggQueryBuilder()
        payload = _make_payload(dimension_col="state_name")

        apply_chart_sorting(qb, [], payload)

        assert len(qb.order_by_clauses) == 0

    def test_sort_config_with_empty_column_name(self):
        """Sort items with empty column names should be skipped."""
        qb = AggQueryBuilder()
        payload = _make_payload(dimension_col="state_name")
        sort_config = [{"column": "", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 0

    def test_table_chart_dimensions_list(self):
        """Table chart dimensions (from the dimensions list) should be valid sort columns."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            chart_type="table",
            dimensions=["col_a", "col_b", "col_c"],
        )
        sort_config = [
            {"column": "col_b", "direction": "desc"},
            {"column": "not_in_dims", "direction": "asc"},
        ]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1
        clauses = _order_clause_strings(qb)
        assert "col_b" in clauses[0]

    def test_expression_metric_alias_is_valid(self):
        """Sorting by a column_expression metric alias should work."""
        qb = AggQueryBuilder()
        payload = _make_payload(
            dimension_col="state_name",
            metrics=[ChartMetric(column_expression="SUM(a)/SUM(b)", alias="ratio")],
        )
        sort_config = [{"column": "ratio", "direction": "asc"}]

        apply_chart_sorting(qb, sort_config, payload)

        assert len(qb.order_by_clauses) == 1
        clauses = _order_clause_strings(qb)
        assert "ratio" in clauses[0]
