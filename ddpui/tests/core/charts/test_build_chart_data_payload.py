"""Tests for build_chart_data_payload helper function"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.charts_service import build_chart_data_payload
from ddpui.schemas.chart_schema import ChartConfig

pytestmark = pytest.mark.django_db


class TestBuildChartDataPayload:
    """Tests for build_chart_data_payload helper"""

    def test_basic_bar_chart(self):
        """Builds payload from a bar chart config"""
        config = ChartConfig(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            title="Sales Chart",
            extra_config={
                "x_axis_column": "month",
                "y_axis_column": "revenue",
                "dimension_column": "region",
                "customizations": {"color": "blue"},
            },
        )
        payload = build_chart_data_payload(config)

        assert payload.chart_type == "bar"
        assert payload.schema_name == "public"
        assert payload.table_name == "orders"
        assert payload.x_axis == "month"
        assert payload.y_axis == "revenue"
        assert payload.dimension_col == "region"
        assert payload.customizations["title"] == "Sales Chart"
        assert payload.customizations["color"] == "blue"
        assert payload.offset == 0
        assert payload.limit == 100

    def test_title_injected_into_customizations(self):
        """Title from config is injected into customizations"""
        config = ChartConfig(
            chart_type="line",
            schema_name="public",
            table_name="orders",
            title="My Chart",
            extra_config={"customizations": {"subtitle": "2025"}},
        )
        payload = build_chart_data_payload(config)

        assert payload.customizations["title"] == "My Chart"
        assert payload.customizations["subtitle"] == "2025"

    def test_no_title_no_injection(self):
        """When title is None, customizations are not modified"""
        config = ChartConfig(
            chart_type="pie",
            schema_name="public",
            table_name="orders",
            extra_config={"customizations": {"color": "red"}},
        )
        payload = build_chart_data_payload(config)

        assert "title" not in payload.customizations
        assert payload.customizations["color"] == "red"

    def test_none_extra_config(self):
        """Handles None extra_config gracefully"""
        config = ChartConfig(
            chart_type="number",
            schema_name="public",
            table_name="orders",
            extra_config=None,
        )
        payload = build_chart_data_payload(config)

        assert payload.x_axis is None
        assert payload.metrics is None
        assert payload.extra_config == {}

    def test_map_fields(self):
        """Map-specific fields are extracted from extra_config"""
        config = ChartConfig(
            chart_type="map",
            schema_name="public",
            table_name="regions",
            extra_config={
                "geographic_column": "state",
                "value_column": "population",
                "selected_geojson_id": 42,
            },
        )
        payload = build_chart_data_payload(config)

        assert payload.geographic_column == "state"
        assert payload.value_column == "population"
        assert payload.selected_geojson_id == 42

    def test_metrics_passed_through(self):
        """Metrics from extra_config are included in payload"""
        config = ChartConfig(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config={
                "metrics": [
                    {"column": "revenue", "aggregation": "sum", "alias": "Total Revenue"},
                    {"column": None, "aggregation": "count", "alias": "Count"},
                ],
            },
        )
        payload = build_chart_data_payload(config)

        assert payload.metrics is not None
        assert len(payload.metrics) == 2

    def test_dashboard_filters_passed_through(self):
        """Resolved dashboard filters are set on the payload"""
        config = ChartConfig(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config={"x_axis_column": "month"},
        )
        filters = [
            {
                "filter_id": "1",
                "column": "status",
                "type": "value",
                "value": "active",
                "settings": {},
            }
        ]
        payload = build_chart_data_payload(config, resolved_dashboard_filters=filters)

        assert payload.dashboard_filters == filters

    def test_dimensions_normalized_for_table(self):
        """Table chart dimensions are normalized via normalize_dimensions"""
        config = ChartConfig(
            chart_type="table",
            schema_name="public",
            table_name="orders",
            extra_config={
                "dimension_columns": ["region", "product"],
            },
        )
        payload = build_chart_data_payload(config)

        assert payload.dimensions == ["region", "product"]

    def test_dimensions_normalized_for_bar(self):
        """Bar chart dimensions use dimension_col and extra_dimension"""
        config = ChartConfig(
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config={
                "dimension_column": "region",
                "extra_dimension_column": "category",
            },
        )
        payload = build_chart_data_payload(config)

        assert payload.dimensions == ["region", "category"]

    def test_frozen_report_config_works(self):
        """Works with a frozen chart config dict (same shape as ChartConfig fields)"""
        frozen = {
            "id": 1,
            "title": "Frozen Chart",
            "chart_type": "line",
            "schema_name": "analytics",
            "table_name": "metrics",
            "extra_config": {
                "x_axis_column": "date",
                "dimension_column": "source",
                "metrics": [{"column": "visits", "aggregation": "sum", "alias": "Visits"}],
                "filters": [
                    {"column": "date", "operator": "greater_than_equal", "value": "2025-01-01"}
                ],
            },
        }
        config = ChartConfig(**frozen)
        payload = build_chart_data_payload(config)

        assert payload.chart_type == "line"
        assert payload.schema_name == "analytics"
        assert payload.x_axis == "date"
        assert payload.customizations["title"] == "Frozen Chart"
        assert payload.extra_config["filters"][0]["value"] == "2025-01-01"
