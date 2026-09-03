"""Tests for validate_payload_columns"""

import os
from unittest.mock import Mock

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import validate_payload_columns
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric


def _make_warehouse(column_names: list[str]) -> Mock:
    """Return a mock Warehouse whose get_table_columns returns the given names."""
    warehouse = Mock()
    warehouse.get_table_columns.return_value = [
        {"name": name, "data_type": "TEXT", "translated_type": "str", "nullable": True}
        for name in column_names
    ]
    return warehouse


class TestValidatePayloadColumns:
    def test_valid_dimension_col(self):
        warehouse = _make_warehouse(["chapter_id", "state", "value"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter_id",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        # Should not raise
        validate_payload_columns(warehouse, payload)

    def test_missing_dimension_col_raises(self):
        warehouse = _make_warehouse(["chapter_id", "state", "value"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="chapter"):
            validate_payload_columns(warehouse, payload)

    def test_missing_metric_column_raises(self):
        warehouse = _make_warehouse(["chapter_id", "state"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter_id",
            metrics=[ChartMetric(column="old_metric", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="old_metric"):
            validate_payload_columns(warehouse, payload)

    def test_expression_metric_skipped(self):
        warehouse = _make_warehouse(["chapter_id"])
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="analytics",
            table_name="dashboard",
            metrics=[ChartMetric(column_expression="SUM(a)/SUM(b)", alias="ratio")],
        )
        validate_payload_columns(warehouse, payload)

    def test_count_star_metric_skipped(self):
        warehouse = _make_warehouse(["chapter_id"])
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="analytics",
            table_name="dashboard",
            metrics=[ChartMetric(aggregation="count", alias="total")],
        )
        validate_payload_columns(warehouse, payload)

    def test_missing_dimensions_list_entry(self):
        warehouse = _make_warehouse(["chapter_id", "state", "value"])
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="analytics",
            table_name="dashboard",
            dimensions=["chapter_id", "chapter", "state"],
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="chapter"):
            validate_payload_columns(warehouse, payload)

    def test_missing_extra_dimension_raises(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter_id",
            extra_dimension="old_col",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="old_col"):
            validate_payload_columns(warehouse, payload)

    def test_missing_row_dimension_raises(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="analytics",
            table_name="dashboard",
            row_dimensions=["gone_col"],
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="gone_col"):
            validate_payload_columns(warehouse, payload)

    def test_missing_column_dimension_raises(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="pivot_table",
            schema_name="analytics",
            table_name="dashboard",
            row_dimensions=["chapter_id"],
            column_dimensions=["gone_col"],
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="gone_col"):
            validate_payload_columns(warehouse, payload)

    def test_missing_geographic_column_raises(self):
        warehouse = _make_warehouse(["state_name", "value"])
        payload = ChartDataPayload(
            chart_type="map",
            schema_name="analytics",
            table_name="dashboard",
            geographic_column="old_state",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="old_state"):
            validate_payload_columns(warehouse, payload)

    def test_missing_filter_column_raises(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter_id",
            metrics=[ChartMetric(column="value", aggregation="sum")],
            extra_config={
                "filters": [
                    {"column": "gone_filter", "operator": "equals", "value": "x"}
                ]
            },
        )
        with pytest.raises(ValueError, match="gone_filter"):
            validate_payload_columns(warehouse, payload)

    def test_get_table_columns_failure_raises(self):
        warehouse = Mock()
        warehouse.get_table_columns.side_effect = Exception("connection error")
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter_id",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="Could not read columns"):
            validate_payload_columns(warehouse, payload)

    def test_empty_dimensions_skipped(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="analytics",
            table_name="dashboard",
            dimensions=["chapter_id", "", "  "],
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        validate_payload_columns(warehouse, payload)

    def test_no_columns_referenced_passes(self):
        warehouse = _make_warehouse(["chapter_id", "value"])
        payload = ChartDataPayload(
            chart_type="number",
            schema_name="analytics",
            table_name="dashboard",
            metrics=[ChartMetric(aggregation="count", alias="total")],
        )
        validate_payload_columns(warehouse, payload)

    def test_error_message_includes_available_columns(self):
        warehouse = _make_warehouse(["chapter_id", "state", "value"])
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="analytics",
            table_name="dashboard",
            dimension_col="chapter",
            metrics=[ChartMetric(column="value", aggregation="sum")],
        )
        with pytest.raises(ValueError, match="chapter_id"):
            validate_payload_columns(warehouse, payload)
