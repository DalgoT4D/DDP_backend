"""Test cases for chart validator"""

import pytest
from ddpui.core.charts.chart_validator import ChartValidator, ChartValidationError


class TestChartValidator:
    """Test chart configuration validation"""

    def test_valid_bar_chart_raw_data(self):
        """Test valid bar chart with raw data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="raw",
            extra_config={
                "x_axis_column": "date",
                "y_axis_column": "sales",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is True
        assert error is None

    def test_valid_bar_chart_aggregated_data(self):
        """Test valid bar chart with aggregated data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="aggregated",
            extra_config={
                "dimension_column": "product",
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is True
        assert error is None

    def test_invalid_bar_chart_missing_x_axis(self):
        """Test invalid bar chart missing x-axis for raw data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="raw",
            extra_config={
                "y_axis_column": "sales",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Bar chart with raw data requires X-axis column" in error

    def test_invalid_bar_chart_mixing_raw_aggregated(self):
        """Test invalid bar chart mixing raw and aggregated fields"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="raw",
            extra_config={
                "x_axis_column": "date",
                "y_axis_column": "sales",
                "dimension_column": "product",  # Should not be present for raw
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "should not have dimension" in error

    def test_valid_pie_chart_raw_data(self):
        """Test valid pie chart with raw data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="pie",
            computation_type="raw",
            extra_config={
                "x_axis_column": "category",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is True
        assert error is None

    def test_valid_pie_chart_aggregated_data(self):
        """Test valid pie chart with aggregated data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="pie",
            computation_type="aggregated",
            extra_config={
                "dimension_column": "region",
                "aggregate_column": "sales",
                "aggregate_function": "avg",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is True
        assert error is None

    def test_valid_line_chart_raw_data(self):
        """Test valid line chart with raw data"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="line",
            computation_type="raw",
            extra_config={
                "x_axis_column": "timestamp",
                "y_axis_column": "temperature",
            },
            schema_name="public",
            table_name="sensor_data",
        )
        assert is_valid is True
        assert error is None

    def test_valid_number_chart(self):
        """Test valid number chart (always aggregated)"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="number",
            computation_type="aggregated",
            extra_config={
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
                "customizations": {
                    "subtitle": "Total Revenue YTD",
                    "numberFormat": "currency",
                    "decimalPlaces": 2,
                },
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is True
        assert error is None

    def test_invalid_number_chart_raw_data(self):
        """Test invalid number chart with raw computation type"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="number",
            computation_type="raw",
            extra_config={
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Number charts only support aggregated data" in error

    def test_invalid_number_chart_missing_aggregate(self):
        """Test invalid number chart missing aggregate column"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="number",
            computation_type="aggregated",
            extra_config={
                "aggregate_function": "sum",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Number chart requires aggregate column" in error

    def test_invalid_number_format(self):
        """Test invalid number format for number chart"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="number",
            computation_type="aggregated",
            extra_config={
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
                "customizations": {
                    "numberFormat": "invalid_format",
                },
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Invalid number format" in error

    def test_invalid_decimal_places(self):
        """Test invalid decimal places for number chart"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="number",
            computation_type="aggregated",
            extra_config={
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
                "customizations": {
                    "decimalPlaces": 15,  # Max is 10
                },
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Decimal places must be between 0 and 10" in error

    def test_invalid_chart_type(self):
        """Test invalid chart type"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="invalid_type",
            computation_type="raw",
            extra_config={},
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Invalid chart type" in error

    def test_invalid_computation_type(self):
        """Test invalid computation type"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="invalid_type",
            extra_config={},
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Invalid computation type" in error

    def test_invalid_aggregate_function(self):
        """Test invalid aggregate function"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="aggregated",
            extra_config={
                "dimension_column": "product",
                "aggregate_column": "revenue",
                "aggregate_function": "invalid_func",
            },
            schema_name="public",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Invalid aggregate function" in error

    def test_missing_schema_name(self):
        """Test missing schema name"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="raw",
            extra_config={
                "x_axis_column": "date",
                "y_axis_column": "sales",
            },
            schema_name="",
            table_name="sales_data",
        )
        assert is_valid is False
        assert "Schema name is required" in error

    def test_missing_table_name(self):
        """Test missing table name"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="bar",
            computation_type="raw",
            extra_config={
                "x_axis_column": "date",
                "y_axis_column": "sales",
            },
            schema_name="public",
            table_name="",
        )
        assert is_valid is False
        assert "Table name is required" in error

    def test_map_chart_not_implemented(self):
        """Test map chart (not yet implemented)"""
        is_valid, error = ChartValidator.validate_chart_config(
            chart_type="map",
            computation_type="raw",
            extra_config={},
            schema_name="public",
            table_name="geo_data",
        )
        assert is_valid is False
        assert "Map charts are not yet implemented" in error

    def test_validate_for_update_partial(self):
        """Test validation for partial updates"""
        # Update only customizations - should be valid
        is_valid, error = ChartValidator.validate_for_update(
            existing_chart_type="number",
            new_chart_type=None,
            new_computation_type=None,
            extra_config={
                "customizations": {
                    "numberFormat": "percentage",
                },
            },
            schema_name="public",
            table_name="metrics",
        )
        assert is_valid is True
        assert error is None

    def test_validate_for_update_full(self):
        """Test validation for full updates when chart type changes"""
        # Changing chart type requires full validation
        is_valid, error = ChartValidator.validate_for_update(
            existing_chart_type="bar",
            new_chart_type="number",
            new_computation_type="aggregated",
            extra_config={
                "aggregate_column": "revenue",
                "aggregate_function": "sum",
            },
            schema_name="public",
            table_name="sales",
        )
        assert is_valid is True
        assert error is None

    def test_validate_for_update_invalid(self):
        """Test validation for invalid update"""
        # Invalid aggregate function in update
        is_valid, error = ChartValidator.validate_for_update(
            existing_chart_type="bar",
            new_chart_type=None,
            new_computation_type=None,
            extra_config={
                "aggregate_function": "invalid_func",
            },
            schema_name="public",
            table_name="sales",
        )
        assert is_valid is False
        assert "Invalid aggregate function" in error
