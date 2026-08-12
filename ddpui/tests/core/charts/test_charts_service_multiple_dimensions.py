"""Unit Tests for Multiple Dimensions Support in Charts

Tests the core logic for handling multiple dimensions:
1. normalize_dimensions function
2. build_multi_metric_query function  
3. transform_data_for_chart function
4. Column mapping in get_chart_data_table_preview
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import MagicMock, patch
from ddpui.core.charts import charts_service
from ddpui.core.charts.maps_service import transform_data_for_map
from ddpui.core.charts.charts_service import build_chart_data_payload
from ddpui.schemas.chart_schemas import (
    ChartConfig,
    ChartDataPayload,
    ChartMetric,
    TransformDataForChart,
)
from ddpui.models.org import OrgWarehouse

pytestmark = pytest.mark.django_db


class TestNormalizeDimensions:
    """Tests for normalize_dimensions function"""

    def test_table_chart_with_dimensions_array(self):
        """Test normalize_dimensions with dimensions array for table chart"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "region", "country"],
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 3
        assert "KEY" in result
        assert "region" in result
        assert "country" in result

    def test_table_chart_without_dimensions_array(self):
        """Test normalize_dimensions falls back to dimension_col/extra_dimension for table chart"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=None,
            dimension_col="KEY",
            extra_dimension="region",
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 2
        assert "KEY" in result
        assert "region" in result

    def test_table_chart_fallback_dimension_col_only(self):
        """Test normalize_dimensions falls back to dimension_col alone when extra_dimension is not set"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=None,
            dimension_col="KEY",
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 1
        assert "KEY" in result

    def test_table_chart_fallback_extra_dimension_only(self):
        """Test normalize_dimensions falls back to extra_dimension alone when dimension_col is not set"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=None,
            extra_dimension="region",
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 1
        assert "region" in result

    def test_table_chart_empty_dimensions_list_fallback(self):
        """Test normalize_dimensions falls back when dimensions is an empty list"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=[],
            dimension_col="KEY",
            extra_dimension="region",
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 2
        assert "KEY" in result
        assert "region" in result

    def test_table_chart_whitespace_dimensions_fallback(self):
        """Test normalize_dimensions falls back when dimensions has only whitespace entries"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["", "   "],
            dimension_col="KEY",
            extra_dimension="region",
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 2
        assert "KEY" in result
        assert "region" in result

    def test_table_chart_filters_empty_dimensions(self):
        """Test normalize_dimensions filters out empty strings"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "", "region", "   ", "country"],
        )

        result = charts_service.normalize_dimensions(payload)

        assert len(result) == 3
        assert "" not in result
        assert "   " not in result
        assert "KEY" in result
        assert "region" in result
        assert "country" in result


class TestBuildMultiMetricQuery:
    """Tests for build_multi_metric_query function"""

    @patch("ddpui.core.charts.charts_service.AggQueryBuilder")
    def test_build_query_with_multiple_dimensions(self, mock_query_builder_class):
        """Test build_multi_metric_query adds all dimensions to query"""
        mock_query_builder = MagicMock()
        mock_query_builder_class.return_value = mock_query_builder

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None),
                ChartMetric(aggregation="sum", column="revenue"),
            ],
        )

        mock_warehouse = MagicMock()
        mock_warehouse.wtype = "postgres"

        result = charts_service.build_multi_metric_query(
            payload, mock_query_builder, mock_warehouse
        )

        # Verify add_column was called for each dimension
        assert mock_query_builder.add_column.call_count == 3
        # Verify group_cols_by was called for each dimension
        assert mock_query_builder.group_cols_by.call_count == 3
        # Verify add_aggregate_column was called for each metric
        assert mock_query_builder.add_aggregate_column.call_count == 2

    @patch("ddpui.core.charts.charts_service.AggQueryBuilder")
    def test_build_query_dimensions_only_no_metrics(self, mock_query_builder_class):
        """Test build_multi_metric_query with dimensions only (no metrics)"""
        mock_query_builder = MagicMock()
        mock_query_builder_class.return_value = mock_query_builder

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "region"],
            metrics=None,
        )

        mock_warehouse = MagicMock()
        mock_warehouse.wtype = "postgres"

        # Should not raise error for table charts without metrics
        result = charts_service.build_multi_metric_query(
            payload, mock_query_builder, mock_warehouse
        )

        # Verify dimensions were added
        assert mock_query_builder.add_column.call_count == 2
        assert mock_query_builder.group_cols_by.call_count == 2


class TestTransformDataForChart:
    """Tests for transform_data_for_chart function with multiple dimensions"""

    def test_transform_table_with_multiple_dimensions(self):
        """Test transform_data_for_chart includes all dimensions in columns"""
        results = [
            {"KEY": "key1", "region": "North", "country": "USA", "Total Count": 100},
            {"KEY": "key2", "region": "South", "country": "Canada", "Total Count": 200},
        ]

        payload = TransformDataForChart(
            chart_type="table",
            dimensions=["KEY", "region", "country"],
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
        )

        result = charts_service.transform_data_for_chart(results, payload)

        # Table charts return data directly with columns and tableData keys
        assert "columns" in result
        columns = result["columns"]
        # Should have all 3 dimensions
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Should have metric column
        assert len(columns) >= 4  # 3 dimensions + 1 metric

    def test_transform_table_dimensions_only(self):
        """Test transform_data_for_chart with dimensions only"""
        results = [
            {"KEY": "key1", "region": "North", "country": "USA"},
            {"KEY": "key2", "region": "South", "country": "Canada"},
        ]

        payload = TransformDataForChart(
            chart_type="table",
            dimensions=["KEY", "region", "country"],
            metrics=None,
        )

        result = charts_service.transform_data_for_chart(results, payload)

        # Table charts return data directly with columns and tableData keys
        assert "columns" in result
        columns = result["columns"]
        # Should have exactly 3 dimensions
        assert len(columns) == 3
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns

    def test_transform_table_three_dimensions_two_metrics(self):
        """Test transform_data_for_chart with 3 dimensions and 2 metrics"""
        results = [
            {
                "KEY": "key1",
                "region": "North",
                "country": "USA",
                "Total Count": 100,
                "Revenue": 5000.50,
            },
        ]

        payload = TransformDataForChart(
            chart_type="table",
            dimensions=["KEY", "region", "country"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )

        result = charts_service.transform_data_for_chart(results, payload)

        # Table charts return data directly with columns and tableData keys
        assert "columns" in result
        columns = result["columns"]
        # Should have all 3 dimensions
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Should have both metrics
        assert len(columns) >= 5  # 3 dimensions + 2 metrics

    def test_transform_table_two_dimensions_four_metrics(self):
        """Test transform_data_for_chart with 2 dimensions and 4 metrics"""
        results = [
            {
                "KEY": "key1",
                "region": "North",
                "Total Count": 100,
                "Revenue": 5000.50,
                "Average Price": 50.00,
                "Max Price": 100.00,
            },
        ]

        payload = TransformDataForChart(
            chart_type="table",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
                ChartMetric(aggregation="max", column="price", alias="Max Price"),
            ],
        )

        result = charts_service.transform_data_for_chart(results, payload)

        # Table charts return data directly with columns and tableData keys
        assert "columns" in result
        columns = result["columns"]
        # Should have all 2 dimensions
        assert "KEY" in columns
        assert "region" in columns
        # Should have all 4 metrics
        assert len(columns) == 6  # 2 dimensions + 4 metrics
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns
        assert "Max Price" in columns

    def test_transform_table_four_dimensions_three_metrics(self):
        """Test transform_data_for_chart with 4 dimensions and 3 metrics"""
        results = [
            {
                "KEY": "key1",
                "region": "North",
                "country": "USA",
                "state": "CA",
                "Total Count": 100,
                "Revenue": 5000.50,
                "Average Price": 50.00,
            },
        ]

        payload = TransformDataForChart(
            chart_type="table",
            dimensions=["KEY", "region", "country", "state"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
            ],
        )

        result = charts_service.transform_data_for_chart(results, payload)

        # Table charts return data directly with columns and tableData keys
        assert "columns" in result
        columns = result["columns"]
        # Should have all 4 dimensions
        assert len(columns) == 7  # 4 dimensions + 3 metrics
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        assert "state" in columns
        # Should have all 3 metrics
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns


class TestGetChartDataTablePreview:
    """Tests for get_chart_data_table_preview column mapping"""

    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.execute_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_preview_column_mapping_multiple_dimensions(
        self, mock_warehouse, mock_execute, mock_build_query
    ):
        """Test that column mapping includes all dimensions"""
        mock_warehouse_obj = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        mock_query_builder = MagicMock()
        mock_build_query.return_value = mock_query_builder

        mock_execute.return_value = [
            {"KEY": "key1", "region": "North", "country": "USA", "Total Count": 100},
        ]

        mock_org_warehouse = MagicMock(spec=OrgWarehouse)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "region", "country"],
            metrics=[ChartMetric(aggregation="count", column=None, alias="Total Count")],
        )

        result = charts_service.get_chart_data_table_preview(mock_org_warehouse, payload, 0, 10)

        assert "columns" in result
        columns = result["columns"]
        # Should have all 3 dimensions
        assert "KEY" in columns
        assert "region" in columns
        assert "country" in columns
        # Should have metric column
        assert len(columns) >= 4  # 3 dimensions + 1 metric

    @patch("ddpui.core.charts.charts_service.build_chart_query")
    @patch("ddpui.core.charts.charts_service.execute_query")
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_preview_two_dimensions_three_metrics(
        self, mock_warehouse, mock_execute, mock_build_query
    ):
        """Test column mapping with 2 dimensions and 3 metrics"""
        mock_warehouse_obj = MagicMock()
        mock_warehouse.return_value = mock_warehouse_obj

        mock_query_builder = MagicMock()
        mock_build_query.return_value = mock_query_builder

        mock_execute.return_value = [
            {
                "KEY": "key1",
                "region": "North",
                "Total Count": 100,
                "Revenue": 5000.50,
                "Average Price": 50.00,
            },
        ]

        mock_org_warehouse = MagicMock(spec=OrgWarehouse)

        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="test",
            dimensions=["KEY", "region"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
                ChartMetric(aggregation="avg", column="price", alias="Average Price"),
            ],
        )

        result = charts_service.get_chart_data_table_preview(mock_org_warehouse, payload, 0, 10)

        assert "columns" in result
        columns = result["columns"]
        # Should have all 2 dimensions
        assert "KEY" in columns
        assert "region" in columns
        # Should have all 3 metrics
        assert len(columns) == 5  # 2 dimensions + 3 metrics
        assert "Total Count" in columns
        assert "Revenue" in columns
        assert "Average Price" in columns


class TestTransformDataForMap:
    """Map transform must support calculated (column_expression) metrics like every other
    aggregated chart type.

    Regression: transform_data_for_map used dict access (.get / metric['aggregation']) that
    crashed on ChartMetric objects and on expression metrics (aggregation is None).
    """

    GEOJSON = {"features": [{"properties": {"name": "Karnataka"}}]}

    @staticmethod
    def _results(alias, value):
        return [{"state_name": "Karnataka", alias: value}]

    def test_calculated_metric_object(self):
        """ChartMetric object with column_expression must not crash; value read via alias."""
        metric = ChartMetric(
            column=None,
            aggregation=None,
            column_expression="sum(district_population)",
            alias="sum(district_population)",
        )
        out = transform_data_for_map(
            self._results("sum(district_population)", 312500999),
            self.GEOJSON,
            "state_name",
            None,
            {},
            [metric],
            0,
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "sum(district_population)"

    def test_calculated_metric_with_alias(self):
        """Expression metric whose alias differs from the expression text."""
        metric = ChartMetric(
            column=None,
            aggregation=None,
            column_expression="avg(district_population)",
            alias="avg pop",
        )
        out = transform_data_for_map(
            self._results("avg pop", 42.0), self.GEOJSON, "state_name", None, {}, [metric], 0
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "avg pop"

    def test_simple_count_metric_still_works(self):
        """Guard: the existing simple-metric path is unchanged."""
        metric = ChartMetric(column=None, aggregation="count", alias="Total Count")
        out = transform_data_for_map(
            self._results("count_all_Total Count", 15),
            self.GEOJSON,
            "state_name",
            None,
            {},
            [metric],
            0,
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "Total Count"
