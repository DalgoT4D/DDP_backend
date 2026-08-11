"""Test that paginated chart queries include metric columns, GROUP BY, and sorting.

Regression tests for DALGO-BACKEND-26C: paginated queries skipped all chart-type
logic (metrics, GROUP BY, sort), producing a bare SELECT * that failed with
UndefinedColumn when sorting by an aggregated metric column.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import MagicMock

from ddpui.core.charts import charts_service
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.models.org import OrgWarehouse


class TestPaginatedChartQuery:
    """Paginated chart queries must include metric columns and GROUP BY."""

    def _warehouse(self):
        wh = MagicMock(spec=OrgWarehouse)
        wh.wtype = "postgres"
        return wh

    def _compile(self, query_builder):
        return str(
            query_builder.build().compile(compile_kwargs={"literal_binds": True})
        )

    def test_bar_chart_paginated_includes_metric_column(self):
        """A bar chart with pagination must still define the metric column."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
            extra_config={
                "pagination": {"enabled": True, "page_size": 50},
            },
        )
        qb = charts_service.build_chart_query(payload, self._warehouse())
        sql = self._compile(qb).lower()

        assert "count" in sql, "COUNT aggregate missing from paginated bar query"
        assert "group by" in sql, "GROUP BY missing from paginated bar query"
        assert "paginated_data" in sql, "inner LIMIT subquery alias missing"

    def test_bar_chart_paginated_with_sort_on_metric(self):
        """Sorting by a metric column must work when pagination is enabled."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
            extra_config={
                "pagination": {"enabled": True, "page_size": 50},
                "sort": [{"column": "Total Count", "direction": "desc"}],
            },
        )
        qb = charts_service.build_chart_query(payload, self._warehouse())
        sql = self._compile(qb).lower()

        assert "count_all_total count" in sql, (
            "Metric alias missing; ORDER BY would fail with UndefinedColumn"
        )
        assert "order by" in sql, "ORDER BY missing from paginated query"

    def test_pie_chart_paginated_includes_metric_and_group_by(self):
        """A pie chart with pagination must include metric and GROUP BY."""
        payload = ChartDataPayload(
            chart_type="pie",
            schema_name="public",
            table_name="test_table",
            dimension_col="category",
            metrics=[
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
            extra_config={
                "pagination": {"enabled": True, "page_size": 20},
            },
        )
        qb = charts_service.build_chart_query(payload, self._warehouse())
        sql = self._compile(qb).lower()

        assert "sum" in sql, "SUM aggregate missing from paginated pie query"
        assert "revenue" in sql, "revenue column missing from paginated pie query"
        assert "group by" in sql, "GROUP BY missing from paginated pie query"
        assert "paginated_data" in sql, "inner LIMIT subquery alias missing"

    def test_number_chart_paginated_includes_metric(self):
        """A number chart with pagination must include the metric."""
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="public",
            table_name="test_table",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total"),
            ],
            extra_config={
                "pagination": {"enabled": True, "page_size": 100},
            },
        )
        qb = charts_service.build_chart_query(payload, self._warehouse())
        sql = self._compile(qb).lower()

        assert "count" in sql, "COUNT aggregate missing from paginated number query"
        assert "paginated_data" in sql, "inner LIMIT subquery alias missing"

    def test_non_paginated_bar_chart_unchanged(self):
        """Bar chart without pagination should still work identically."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
        )
        qb = charts_service.build_chart_query(payload, self._warehouse())
        sql = self._compile(qb).lower()

        assert "count" in sql
        assert "group by" in sql
        assert "paginated_data" not in sql, "non-paginated path should not wrap in subquery"
