"""Tests for duplicate metric alias validation in build_chart_query.

Verifies that build_chart_query raises ValueError when two metrics
produce the same SQL alias, preventing SQLAlchemy ambiguous-column errors.
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

class TestDuplicateMetricAliasValidation:
    """Tests for _validate_unique_metric_aliases and its integration in build_chart_query."""

    def _mock_warehouse(self):
        mock = MagicMock(spec=OrgWarehouse)
        mock.wtype = "bigquery"
        return mock

    def test_duplicate_expression_metric_aliases_rejected(self):
        """Two expression metrics with the same alias must raise ValueError."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(
                    column_expression="SUM(a) / SUM(b)",
                    alias="% observed classrooms with teachers_applying_conceptual_cfu_count",
                ),
                ChartMetric(
                    column_expression="SUM(c) / SUM(d)",
                    alias="% observed classrooms with teachers_applying_conceptual_cfu_count",
                ),
            ],
        )

        with pytest.raises(ValueError, match="Duplicate metric alias"):
            charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_duplicate_simple_metric_aliases_rejected(self):
        """Two simple metrics with the same explicit alias must raise ValueError."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test_table",
            dimensions=["region"],
            metrics=[
                ChartMetric(aggregation="sum", column="revenue", alias="Total"),
                ChartMetric(aggregation="avg", column="revenue", alias="Total"),
            ],
        )

        with pytest.raises(ValueError, match="Duplicate metric alias"):
            charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_duplicate_computed_aliases_rejected(self):
        """Two metrics with no explicit alias that compute the same alias must raise ValueError."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="sum", column="revenue"),
                ChartMetric(aggregation="sum", column="revenue"),
            ],
        )

        with pytest.raises(ValueError, match="Duplicate metric alias"):
            charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_unique_aliases_accepted(self):
        """Metrics with distinct aliases must not raise."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(
                    column_expression="SUM(a) / SUM(b)",
                    alias="metric_a",
                ),
                ChartMetric(
                    column_expression="SUM(c) / SUM(d)",
                    alias="metric_b",
                ),
            ],
        )

        # Should not raise
        charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_single_metric_skips_validation(self):
        """A single metric must not trigger duplicate validation."""
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="public",
            table_name="test_table",
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total"),
            ],
        )

        # Should not raise
        charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_duplicate_aliases_in_pivot_table_rejected(self):
        """Pivot table queries with duplicate metric aliases must raise ValueError."""
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="public",
            table_name="test_table",
            row_dimensions=["region"],
            metrics=[
                ChartMetric(aggregation="sum", column="revenue", alias="Total"),
                ChartMetric(aggregation="avg", column="revenue", alias="Total"),
            ],
        )

        with pytest.raises(ValueError, match="Duplicate metric alias"):
            charts_service.build_chart_query(payload, self._mock_warehouse())

    def test_error_message_includes_alias_and_positions(self):
        """The error message must include the duplicate alias and the metric positions."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="test_table",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="sum", column="a", alias="my_metric"),
                ChartMetric(aggregation="avg", column="b", alias="other"),
                ChartMetric(aggregation="max", column="c", alias="my_metric"),
            ],
        )

        with pytest.raises(ValueError, match=r"positions 0 and 2"):
            charts_service.build_chart_query(payload, self._mock_warehouse())
