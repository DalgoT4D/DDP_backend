"""Regression test for DALGO-BACKEND-26C.

When pagination was enabled (limit is not None), the chart-type-specific logic
(dimensions, metrics, GROUP BY) was inside the `else` branch of the pagination
check and never ran.  ORDER BY still ran unconditionally, referencing aliases
that didn't exist in SELECT → ProgrammingError.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import MagicMock

from ddpui.core.charts import charts_service
from ddpui.models.org import OrgWarehouse
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric


class TestPaginatedChartQuery:
    """Charts with pagination enabled must still include dimensions/metrics/GROUP BY."""

    @staticmethod
    def _warehouse():
        wh = MagicMock(spec=OrgWarehouse)
        wh.wtype = "postgres"
        return wh

    def test_bar_chart_with_pagination_and_sort(self):
        """Bar chart + COUNT metric + sort + pagination must compile without error."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            dimension_col="status",
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
            extra_config={
                "pagination": {"enabled": True, "page_size": 50},
                "sort": [{"column": "count_all_Total Count", "order": "desc"}],
            },
        )

        qb = charts_service.build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # Dimension must be in SELECT
        assert "status" in compiled.lower()
        # Metric alias must be in SELECT
        assert "count_all_total count" in compiled.lower() or "count" in compiled.lower()
        # GROUP BY must be present
        assert "GROUP BY" in compiled.upper()
        # ORDER BY must be present
        assert "ORDER BY" in compiled.upper()

    def test_bar_chart_with_pagination_has_subquery(self):
        """When pagination is on, the inner LIMIT/OFFSET subquery should exist."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            dimension_col="status",
            metrics=[ChartMetric(aggregation="sum", column="amount", alias="Revenue")],
            extra_config={
                "pagination": {"enabled": True, "page_size": 25},
            },
        )

        qb = charts_service.build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        # The paginated subquery should include LIMIT
        assert "LIMIT" in compiled.upper()
        # Dimension and metric should still be present in the outer query
        assert "status" in compiled.lower()
        assert "sum" in compiled.lower()
        assert "GROUP BY" in compiled.upper()

    def test_pie_chart_with_pagination(self):
        """Pie chart with pagination must still include dimension and metric."""
        payload = ChartDataPayload(
            chart_type="pie",
            schema_name="public",
            table_name="orders",
            dimension_col="category",
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total")],
            extra_config={
                "pagination": {"enabled": True, "page_size": 50},
            },
        )

        qb = charts_service.build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        assert "category" in compiled.lower()
        assert "count" in compiled.lower()
        assert "GROUP BY" in compiled.upper()

    def test_number_chart_with_pagination(self):
        """Number chart with pagination must still include the aggregate metric."""
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="public",
            table_name="orders",
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total")],
            extra_config={
                "pagination": {"enabled": True, "page_size": 100},
            },
        )

        qb = charts_service.build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        assert "count" in compiled.lower()

    def test_line_chart_with_pagination_and_sort(self):
        """Line chart + pagination + sort must compile without error."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="sales",
            dimension_col="month",
            metrics=[ChartMetric(aggregation="sum", column="revenue", alias="Revenue")],
            extra_config={
                "pagination": {"enabled": True, "page_size": 30},
                "sort": [{"column": "sum_revenue", "order": "asc"}],
            },
        )

        qb = charts_service.build_chart_query(payload, self._warehouse())
        compiled = str(qb.build().compile(compile_kwargs={"literal_binds": True}))

        assert "month" in compiled.lower()
        assert "sum" in compiled.lower()
        assert "revenue" in compiled.lower()
        assert "GROUP BY" in compiled.upper()
        assert "ORDER BY" in compiled.upper()
