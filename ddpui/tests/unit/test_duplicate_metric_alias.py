"""Regression tests for DALGO-BACKEND-269: duplicate metric aliases.

Duplicate metrics with identical aliases must not produce SQL with ambiguous
column names. SQLAlchemy raises InvalidRequestError when dict(row) encounters
duplicate column names in the result set.
"""

from unittest.mock import MagicMock

from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric
from ddpui.core.charts.charts_service import build_chart_query


class TestDuplicateMetricAliasDeduplication:
    """build_multi_metric_query and build_pivot_table_query must skip metrics
    whose SQL alias has already been emitted, preventing ambiguous column names."""

    @staticmethod
    def _warehouse(wtype="postgres"):
        wh = MagicMock()
        wh.wtype = wtype
        return wh

    @staticmethod
    def _compile(query_builder):
        return str(query_builder.build().compile(compile_kwargs={"literal_binds": True}))

    def test_duplicate_count_all_metrics_deduplicated(self):
        """Two COUNT(*) metrics with the same alias produce only one SQL column."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
        )
        compiled = self._compile(build_chart_query(payload, self._warehouse()))

        assert compiled.count("count_all_Total Count") == 1

    def test_duplicate_simple_metrics_deduplicated(self):
        """Two SUM(revenue) metrics with the same alias produce only one SQL column."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["region"],
            metrics=[
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )
        select_section = self._compile(build_chart_query(payload, self._warehouse())).split("FROM")[0]

        assert select_section.count('"Revenue"') == 1

    def test_duplicate_expression_metrics_deduplicated(self):
        """Two expression metrics with the same alias produce only one SQL column."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
                ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
            ],
        )
        select_section = self._compile(build_chart_query(payload, self._warehouse())).split("FROM")[0]

        assert select_section.count("Ratio") == 1

    def test_three_identical_metrics_produce_one_column(self):
        """Three identical COUNT(*) metrics should still produce only one SQL column."""
        payload = ChartDataPayload(
            chart_type="line",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
        )
        compiled = self._compile(build_chart_query(payload, self._warehouse()))

        assert compiled.count("count_all_Total Count") == 1

    def test_distinct_metrics_not_deduplicated(self):
        """Metrics with different aliases should all appear in the SQL."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )
        compiled = self._compile(build_chart_query(payload, self._warehouse()))

        assert "count_all_Total Count" in compiled
        assert "Revenue" in compiled

    def test_pivot_duplicate_metrics_deduplicated(self):
        """Pivot table queries should also deduplicate duplicate metric aliases."""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="test_table",
            row_dimensions=["region"],
            column_dimensions=["state"],
            show_row_subtotals=True,
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
            ],
        )
        compiled = self._compile(build_chart_query(payload, self._warehouse()))

        assert compiled.count("count_all_Total Count") == 1

    def test_pivot_distinct_metrics_preserved(self):
        """Pivot table with distinct metrics should keep all of them."""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="test_table",
            row_dimensions=["region"],
            column_dimensions=["state"],
            show_row_subtotals=True,
            metrics=[
                ChartMetric(aggregation="count", column="id", alias="Count"),
                ChartMetric(aggregation="sum", column="amount", alias="Total"),
            ],
        )
        compiled = self._compile(build_chart_query(payload, self._warehouse()))

        assert "Count" in compiled
        assert "Total" in compiled
