"""Tests for metric alias de-duplication.

Verifies that build_multi_metric_query and build_pivot_table_query produce
unique SQL column aliases even when multiple metrics share the same alias or
when a metric alias collides with a dimension column name.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import MagicMock

import pytest

from ddpui.core.charts.charts_service import (
    build_chart_query,
    build_multi_metric_query,
    deduplicate_metric_aliases,
    metric_sql_alias,
)
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric


# ── Unit tests for deduplicate_metric_aliases ────────────────────────────────


class TestDeduplicateMetricAliases:
    """Pure-function tests for the de-duplication helper."""

    def test_no_collision_returns_base_aliases(self):
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Revenue"),
            ChartMetric(column="cost", aggregation="sum", alias="Cost"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases == ["Revenue", "Cost"]

    def test_duplicate_metric_aliases_get_suffixed(self):
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Total"),
            ChartMetric(column="cost", aggregation="sum", alias="Total"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases[0] == "Total"
        assert aliases[1] == "Total_2"

    def test_three_way_collision(self):
        metrics = [
            ChartMetric(column="a", aggregation="sum", alias="X"),
            ChartMetric(column="b", aggregation="avg", alias="X"),
            ChartMetric(column="c", aggregation="min", alias="X"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases == ["X", "X_2", "X_3"]

    def test_collision_with_dimension_name(self):
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="region"),
        ]
        aliases = deduplicate_metric_aliases(metrics, dimension_names=["region"])
        assert aliases == ["region_2"]

    def test_collision_with_dimension_and_other_metric(self):
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="region"),
            ChartMetric(column="cost", aggregation="sum", alias="region"),
        ]
        aliases = deduplicate_metric_aliases(metrics, dimension_names=["region"])
        assert aliases == ["region_2", "region_3"]

    def test_no_metrics_returns_empty(self):
        assert deduplicate_metric_aliases([]) == []

    def test_auto_generated_alias_collision(self):
        """Two metrics with the same column+aggregation but no explicit alias."""
        metrics = [
            ChartMetric(column="revenue", aggregation="sum"),
            ChartMetric(column="revenue", aggregation="sum"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases[0] == "sum_revenue"
        assert aliases[1] == "sum_revenue_2"

    def test_count_all_collision(self):
        metrics = [
            ChartMetric(column=None, aggregation="count", alias="Total"),
            ChartMetric(column=None, aggregation="count", alias="Total"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases[0] == "count_all_Total"
        assert aliases[1] == "count_all_Total_2"

    def test_expression_metric_collision(self):
        metrics = [
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
            ChartMetric(column_expression="SUM(c)/SUM(d)", alias="Ratio"),
        ]
        aliases = deduplicate_metric_aliases(metrics)
        assert aliases[0] == "Ratio"
        assert aliases[1] == "Ratio_2"


# ── Integration: SQL query has unique aliases ────────────────────────────────


class TestBuildMultiMetricQueryDuplicateAliases:
    """Verify the SQL query has distinct column aliases for duplicate metrics."""

    @staticmethod
    def _warehouse():
        wh = MagicMock()
        wh.wtype = "postgres"
        return wh

    def test_duplicate_metric_aliases_produce_unique_sql_columns(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(column="revenue", aggregation="sum", alias="Total"),
                ChartMetric(column="cost", aggregation="sum", alias="Total"),
            ],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("test_table", "public")
        build_multi_metric_query(payload, qb, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # Both aliases should appear, with the second one suffixed
        assert "Total" in compiled
        assert "Total_2" in compiled

    def test_metric_alias_collides_with_dimension(self):
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["region"],
            metrics=[
                ChartMetric(column="revenue", aggregation="sum", alias="region"),
            ],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("test_table", "public")
        build_multi_metric_query(payload, qb, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # The metric alias should be suffixed to avoid collision with the dimension
        assert "region_2" in compiled

    def test_build_chart_query_no_crash_with_duplicate_aliases(self):
        """End-to-end: build_chart_query should compile without error."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="state",
            metrics=[
                ChartMetric(column="x", aggregation="sum", alias="my_metric"),
                ChartMetric(column="y", aggregation="avg", alias="my_metric"),
                ChartMetric(column="z", aggregation="count", alias="my_metric"),
            ],
        )
        qb = build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # All three should get unique aliases
        assert "my_metric" in compiled
        assert "my_metric_2" in compiled
        assert "my_metric_3" in compiled


class TestPivotQueryDuplicateAliases:
    """Verify pivot queries also produce unique aliases."""

    @staticmethod
    def _warehouse():
        wh = MagicMock()
        wh.wtype = "postgres"
        return wh

    def test_pivot_duplicate_metric_aliases(self):
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="test_table",
            row_dimensions=["district"],
            column_dimensions=["program"],
            show_row_subtotals=True,
            metrics=[
                ChartMetric(column="id", aggregation="count", alias="Count"),
                ChartMetric(column="amount", aggregation="sum", alias="Count"),
            ],
        )
        qb = build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        assert "Count" in compiled
        assert "Count_2" in compiled
