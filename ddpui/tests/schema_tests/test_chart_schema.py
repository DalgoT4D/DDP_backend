"""Schema Tests for Chart schemas

Tests schema-specific functionality NOT tested by API tests:
1. ChartMetric nested schema validation
2. ChartDataPayload specific fields (map, metrics, pagination)
3. GeoJSON schemas
4. Schema serialization (.dict())
5. Edge cases (unicode, type coercion)
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
    GeoJSONListResponse,
    GeoJSONUpload,
)


# ================================================================================
# Test ChartMetric Schema (nested schema - NOT tested by API tests)
# ================================================================================


class TestChartMetricSchema:
    """Tests for ChartMetric nested schema"""

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

    def test_chart_metric_missing_aggregation(self):
        """Test that aggregation is required"""
        with pytest.raises(ValidationError) as exc_info:
            ChartMetric(column="revenue")

        errors = exc_info.value.errors()
        assert any(err["loc"] == ("aggregation",) for err in errors)

    def test_chart_metric_all_aggregation_types(self):
        """Test various aggregation types"""
        aggregations = ["sum", "avg", "count", "min", "max", "count_distinct"]

        for agg in aggregations:
            metric = ChartMetric(column="value", aggregation=agg)
            assert metric.aggregation == agg


# ================================================================================
# Test ChartDataPayload specific fields (NOT fully tested by API tests)
# ================================================================================


class TestChartDataPayloadSchema:
    """Tests for ChartDataPayload specific fields"""

    def test_payload_default_pagination(self):
        """Test default pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
        )

        assert payload.offset == 0
        assert payload.limit == 100

    def test_payload_custom_pagination(self):
        """Test custom pagination values"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
            offset=50,
            limit=25,
        )

        assert payload.offset == 50
        assert payload.limit == 25

    def test_payload_with_map_fields(self):
        """Test payload with map-specific fields"""
        payload = ChartDataPayload(
            chart_type="map",
            schema_name="public",
            table_name="regional_sales",
            geographic_column="region",
            value_column="sales",
            selected_geojson_id=1,
        )

        assert payload.geographic_column == "region"
        assert payload.value_column == "sales"
        assert payload.selected_geojson_id == 1

    def test_payload_with_metrics(self):
        """Test payload with metrics array"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="sales",
            dimension_col="category",
            metrics=[ChartMetric(column="revenue", aggregation="sum")],
        )

        assert payload.dimension_col == "category"
        assert len(payload.metrics) == 1
        assert payload.metrics[0].aggregation == "sum"

    def test_payload_with_dashboard_filters(self):
        """Test payload with dashboard filters"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="users",
            dashboard_filters=[{"column": "status", "value": "active"}],
        )

        assert len(payload.dashboard_filters) == 1


# ================================================================================
# Test GeoJSON Schemas (NOT tested by API tests)
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
        assert upload.description == "Custom regions for our org"

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
# Test Schema Serialization (NOT tested by API tests)
# ================================================================================


class TestSchemaSerialization:
    """Tests for schema serialization (.dict())"""

    def test_chart_create_to_dict(self):
        """Test ChartCreate can be converted to dict"""
        chart = ChartCreate(
            title="Test",
            chart_type="bar",
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

    def test_chart_data_response_to_dict(self):
        """Test ChartDataResponse can be converted to dict"""
        response = ChartDataResponse(
            data={"labels": ["A", "B"], "values": [10, 20]},
            echarts_config={"type": "bar"},
        )

        data = response.dict()

        assert data["data"]["labels"] == ["A", "B"]
        assert data["echarts_config"]["type"] == "bar"


# ================================================================================
# Test Edge Cases (NOT tested by API tests)
# ================================================================================


class TestEdgeCases:
    """Tests for edge cases and special values"""

    def test_chart_create_with_unicode_title(self):
        """Test chart creation with unicode characters"""
        chart = ChartCreate(
            title="日本語チャート 📊",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
        )

        assert chart.title == "日本語チャート 📊"

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
            schema_name="public",
            table_name="users",
            extra_config={},
            created_at=now,
            updated_at=now,
        )

        assert response.id == 123
        assert isinstance(response.id, int)

    def test_chart_metric_with_empty_alias(self):
        """Test metric with empty string alias"""
        metric = ChartMetric(column="revenue", aggregation="sum", alias="")

        assert metric.alias == ""
