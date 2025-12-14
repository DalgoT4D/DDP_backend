"""Test cases for Chart schemas (Pydantic/Ninja validation)

These tests verify:
1. Required vs optional fields
2. Default values
3. Type validation
4. ORM mode conversion (from_orm)
5. Schema serialization
"""

import pytest
from datetime import datetime
from pydantic import ValidationError
from unittest.mock import MagicMock

from ddpui.schemas.chart_schema import (
    ChartMetric,
    ChartCreate,
    ChartUpdate,
    ChartResponse,
    ChartDataPayload,
    ChartDataResponse,
    DataPreviewResponse,
    ExecuteChartQuery,
    TransformDataForChart,
    GeoJSONListResponse,
    GeoJSONDetailResponse,
    GeoJSONUpload,
)


# ================================================================================
# Test ChartMetric Schema
# ================================================================================


class TestChartMetricSchema:
    """Tests for ChartMetric schema"""

    def test_chart_metric_valid_with_column(self):
        """Test valid metric with column name"""
        metric = ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue")

        assert metric.column == "revenue"
        assert metric.aggregation == "sum"
        assert metric.alias == "Total Revenue"

    def test_chart_metric_valid_without_column(self):
        """Test valid metric without column (for COUNT(*))"""
        metric = ChartMetric(column=None, aggregation="count", alias="Row Count")

        assert metric.column is None
        assert metric.aggregation == "count"

    def test_chart_metric_valid_without_alias(self):
        """Test valid metric without alias (optional)"""
        metric = ChartMetric(column="sales", aggregation="avg")

        assert metric.column == "sales"
        assert metric.aggregation == "avg"
        assert metric.alias is None

    def test_chart_metric_missing_aggregation(self):
        """Test that aggregation is required"""
        with pytest.raises(ValidationError) as exc_info:
            ChartMetric(column="revenue")

        errors = exc_info.value.errors()
        assert any(err["loc"] == ("aggregation",) for err in errors)

    def test_chart_metric_all_aggregation_types(self):
        """Test various aggregation types are accepted"""
        aggregations = ["sum", "avg", "count", "min", "max", "count_distinct"]

        for agg in aggregations:
            metric = ChartMetric(column="value", aggregation=agg)
            assert metric.aggregation == agg

    def test_chart_metric_from_dict(self):
        """Test creating metric from dictionary"""
        data = {"column": "amount", "aggregation": "sum", "alias": "Total"}
        metric = ChartMetric(**data)

        assert metric.column == "amount"
        assert metric.aggregation == "sum"


# ================================================================================
# Test ChartCreate Schema
# ================================================================================


class TestChartCreateSchema:
    """Tests for ChartCreate schema"""

    def test_chart_create_valid_minimal(self):
        """Test valid chart creation with minimal required fields"""
        chart = ChartCreate(
            title="My Chart",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={},
        )

        assert chart.title == "My Chart"
        assert chart.chart_type == "bar"
        assert chart.computation_type == "raw"
        assert chart.description is None

    def test_chart_create_valid_full(self):
        """Test valid chart creation with all fields"""
        chart = ChartCreate(
            title="Sales Dashboard",
            description="Monthly sales overview",
            chart_type="line",
            computation_type="aggregated",
            schema_name="analytics",
            table_name="monthly_sales",
            extra_config={
                "x_axis_column": "month",
                "metrics": [{"column": "revenue", "aggregation": "sum"}],
            },
        )

        assert chart.title == "Sales Dashboard"
        assert chart.description == "Monthly sales overview"
        assert chart.extra_config["x_axis_column"] == "month"

    def test_chart_create_missing_title(self):
        """Test that title is required"""
        with pytest.raises(ValidationError) as exc_info:
            ChartCreate(
                chart_type="bar",
                computation_type="raw",
                schema_name="public",
                table_name="users",
                extra_config={},
            )

        errors = exc_info.value.errors()
        assert any(err["loc"] == ("title",) for err in errors)

    def test_chart_create_missing_chart_type(self):
        """Test that chart_type is required"""
        with pytest.raises(ValidationError) as exc_info:
            ChartCreate(
                title="My Chart",
                computation_type="raw",
                schema_name="public",
                table_name="users",
                extra_config={},
            )

        errors = exc_info.value.errors()
        assert any(err["loc"] == ("chart_type",) for err in errors)

    def test_chart_create_missing_extra_config(self):
        """Test that extra_config is required"""
        with pytest.raises(ValidationError) as exc_info:
            ChartCreate(
                title="My Chart",
                chart_type="bar",
                computation_type="raw",
                schema_name="public",
                table_name="users",
            )

        errors = exc_info.value.errors()
        assert any(err["loc"] == ("extra_config",) for err in errors)

    def test_chart_create_all_chart_types(self):
        """Test chart creation with various chart types"""
        chart_types = ["bar", "pie", "line", "number", "map"]

        for chart_type in chart_types:
            chart = ChartCreate(
                title=f"{chart_type} Chart",
                chart_type=chart_type,
                computation_type="raw",
                schema_name="public",
                table_name="data",
                extra_config={},
            )
            assert chart.chart_type == chart_type

    def test_chart_create_all_computation_types(self):
        """Test chart creation with various computation types"""
        computation_types = ["raw", "aggregated"]

        for comp_type in computation_types:
            chart = ChartCreate(
                title=f"{comp_type} Chart",
                chart_type="bar",
                computation_type=comp_type,
                schema_name="public",
                table_name="data",
                extra_config={},
            )
            assert chart.computation_type == comp_type


# ================================================================================
# Test ChartUpdate Schema
# ================================================================================


class TestChartUpdateSchema:
    """Tests for ChartUpdate schema (partial updates)"""

    def test_chart_update_all_fields(self):
        """Test updating all fields"""
        update = ChartUpdate(
            title="Updated Title",
            description="Updated Description",
            chart_type="pie",
            computation_type="aggregated",
            schema_name="new_schema",
            table_name="new_table",
            extra_config={"new_key": "new_value"},
        )

        assert update.title == "Updated Title"
        assert update.chart_type == "pie"

    def test_chart_update_partial_title_only(self):
        """Test partial update with only title"""
        update = ChartUpdate(title="New Title")

        assert update.title == "New Title"
        assert update.description is None
        assert update.chart_type is None

    def test_chart_update_empty(self):
        """Test update with no fields (all optional)"""
        update = ChartUpdate()

        assert update.title is None
        assert update.chart_type is None
        assert update.extra_config is None


# ================================================================================
# Test ChartResponse Schema with ORM Mode
# ================================================================================


class TestChartResponseSchema:
    """Tests for ChartResponse schema including ORM mode"""

    def test_chart_response_valid(self):
        """Test valid chart response"""
        now = datetime.now()
        response = ChartResponse(
            id=1,
            title="Test Chart",
            description="Test Description",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={"key": "value"},
            created_at=now,
            updated_at=now,
        )

        assert response.id == 1
        assert response.title == "Test Chart"
        assert response.chart_type == "bar"

    def test_chart_response_null_description(self):
        """Test chart response with null description"""
        now = datetime.now()
        response = ChartResponse(
            id=1,
            title="Test Chart",
            description=None,
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_at=now,
            updated_at=now,
        )

        assert response.description is None

    def test_chart_response_orm_mode_enabled(self):
        """Test that ORM mode is enabled in Config"""
        assert hasattr(ChartResponse, "Config")
        assert ChartResponse.Config.orm_mode is True

    def test_chart_response_from_orm(self):
        """Test creating ChartResponse from ORM-like object using from_orm"""
        # Create a mock ORM object
        mock_chart = MagicMock()
        mock_chart.id = 42
        mock_chart.title = "ORM Chart"
        mock_chart.description = "From ORM"
        mock_chart.chart_type = "pie"
        mock_chart.computation_type = "aggregated"
        mock_chart.schema_name = "public"
        mock_chart.table_name = "sales"
        mock_chart.extra_config = {"dimension": "category"}
        mock_chart.created_at = datetime(2024, 1, 1, 12, 0, 0)
        mock_chart.updated_at = datetime(2024, 1, 2, 12, 0, 0)

        # Use from_orm to create response
        response = ChartResponse.from_orm(mock_chart)

        assert response.id == 42
        assert response.title == "ORM Chart"
        assert response.chart_type == "pie"
        assert response.extra_config == {"dimension": "category"}
        assert response.created_at.year == 2024

    def test_chart_response_from_orm_with_none_description(self):
        """Test from_orm handles None description correctly"""
        mock_chart = MagicMock()
        mock_chart.id = 1
        mock_chart.title = "Test"
        mock_chart.description = None
        mock_chart.chart_type = "bar"
        mock_chart.computation_type = "raw"
        mock_chart.schema_name = "public"
        mock_chart.table_name = "test"
        mock_chart.extra_config = {}
        mock_chart.created_at = datetime.now()
        mock_chart.updated_at = datetime.now()

        response = ChartResponse.from_orm(mock_chart)

        assert response.description is None


# ================================================================================
# Test ChartDataPayload Schema
# ================================================================================


class TestChartDataPayloadSchema:
    """Tests for ChartDataPayload schema"""

    def test_payload_valid_minimal(self):
        """Test minimal valid payload"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
        )

        assert payload.chart_type == "bar"
        assert payload.offset == 0
        assert payload.limit == 100

    def test_payload_with_raw_data_fields(self):
        """Test payload with raw data fields"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="sales",
            x_axis="date",
            y_axis="amount",
        )

        assert payload.x_axis == "date"
        assert payload.y_axis == "amount"

    def test_payload_with_aggregated_data_fields(self):
        """Test payload with aggregated data fields"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="aggregated",
            schema_name="public",
            table_name="sales",
            dimension_col="category",
            metrics=[ChartMetric(column="revenue", aggregation="sum")],
        )

        assert payload.dimension_col == "category"
        assert len(payload.metrics) == 1

    def test_payload_with_map_fields(self):
        """Test payload with map-specific fields"""
        payload = ChartDataPayload(
            chart_type="map",
            computation_type="aggregated",
            schema_name="public",
            table_name="regional_sales",
            geographic_column="region",
            value_column="sales",
            selected_geojson_id=1,
        )

        assert payload.geographic_column == "region"
        assert payload.selected_geojson_id == 1

    def test_payload_default_pagination(self):
        """Test default pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
        )

        assert payload.offset == 0
        assert payload.limit == 100

    def test_payload_custom_pagination(self):
        """Test custom pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            offset=50,
            limit=25,
        )

        assert payload.offset == 50
        assert payload.limit == 25

    def test_payload_with_dashboard_filters(self):
        """Test payload with dashboard filters"""
        payload = ChartDataPayload(
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            dashboard_filters=[
                {"column": "status", "value": "active"},
            ],
        )

        assert len(payload.dashboard_filters) == 1


# ================================================================================
# Test ChartDataResponse Schema
# ================================================================================


class TestChartDataResponseSchema:
    """Tests for ChartDataResponse schema"""

    def test_chart_data_response_valid(self):
        """Test valid chart data response"""
        response = ChartDataResponse(
            data={"labels": ["A", "B"], "values": [10, 20]},
            echarts_config={"xAxis": {"type": "category"}, "series": []},
        )

        assert response.data["labels"] == ["A", "B"]
        assert "xAxis" in response.echarts_config

    def test_chart_data_response_empty_data(self):
        """Test response with empty data"""
        response = ChartDataResponse(
            data={},
            echarts_config={},
        )

        assert response.data == {}
        assert response.echarts_config == {}


# ================================================================================
# Test GeoJSON Schemas
# ================================================================================


class TestGeoJSONSchemas:
    """Tests for GeoJSON related schemas"""

    def test_geojson_upload_valid(self):
        """Test valid GeoJSON upload"""
        upload = GeoJSONUpload(
            region_id=1,
            name="custom_regions",
            description="Custom regions for our org",
            properties_key="region_name",
            geojson_data={"type": "FeatureCollection", "features": []},
        )

        assert upload.region_id == 1
        assert upload.name == "custom_regions"

    def test_geojson_upload_without_description(self):
        """Test GeoJSON upload without optional description"""
        upload = GeoJSONUpload(
            region_id=1,
            name="test",
            properties_key="name",
            geojson_data={},
        )

        assert upload.description is None

    def test_geojson_list_response(self):
        """Test GeoJSON list response"""
        response = GeoJSONListResponse(
            id=1,
            name="india_states",
            display_name="India States",
            is_default=True,
            layer_name="states",
            properties_key="name",
        )

        assert response.id == 1
        assert response.is_default is True


# ================================================================================
# Test Schema Serialization
# ================================================================================


class TestSchemaSerialization:
    """Tests for schema serialization (.dict())"""

    def test_chart_create_to_dict(self):
        """Test ChartCreate can be converted to dict"""
        chart = ChartCreate(
            title="Test",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={"key": "value"},
        )

        data = chart.dict()

        assert isinstance(data, dict)
        assert data["title"] == "Test"
        assert data["extra_config"] == {"key": "value"}

    def test_chart_update_to_dict_excludes_none(self):
        """Test ChartUpdate dict excludes None values when specified"""
        update = ChartUpdate(title="New Title")

        data = update.dict(exclude_none=True)

        assert "title" in data
        assert "description" not in data
        assert "chart_type" not in data

    def test_chart_response_to_dict(self):
        """Test ChartResponse can be converted to dict"""
        now = datetime.now()
        response = ChartResponse(
            id=1,
            title="Test",
            description=None,
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="test",
            extra_config={},
            created_at=now,
            updated_at=now,
        )

        data = response.dict()

        assert data["id"] == 1
        assert data["title"] == "Test"
        assert data["description"] is None


# ================================================================================
# Test Edge Cases
# ================================================================================


class TestEdgeCases:
    """Tests for edge cases and special values"""

    def test_chart_create_with_unicode_title(self):
        """Test chart creation with unicode characters"""
        chart = ChartCreate(
            title="日本語チャート 📊",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={},
        )

        assert chart.title == "日本語チャート 📊"

    def test_chart_create_with_special_characters(self):
        """Test chart creation with special characters in title"""
        chart = ChartCreate(
            title="Chart with 'quotes' and \"double quotes\"",
            chart_type="bar",
            computation_type="raw",
            schema_name="my_schema",
            table_name="my_table_v2",
            extra_config={},
        )

        assert "quotes" in chart.title

    def test_extra_config_with_nested_arrays(self):
        """Test extra_config with deeply nested structures"""
        extra_config = {
            "filters": [
                {
                    "conditions": [
                        {"field": "a", "op": "eq", "value": 1},
                        {"field": "b", "op": "gt", "value": 2},
                    ]
                }
            ]
        }

        chart = ChartCreate(
            title="Nested",
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config=extra_config,
        )

        assert chart.extra_config["filters"][0]["conditions"][0]["field"] == "a"

    def test_chart_response_id_coerced_from_string(self):
        """Test that id can be coerced from string to int"""
        now = datetime.now()
        response = ChartResponse(
            id="123",  # String will be coerced to int
            title="Test",
            description=None,
            chart_type="bar",
            computation_type="raw",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_at=now,
            updated_at=now,
        )

        assert response.id == 123
        assert isinstance(response.id, int)
