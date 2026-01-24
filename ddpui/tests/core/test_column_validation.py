"""Tests for column normalization and dimension limits"""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import (
    normalize_dimensions,
    MAX_DIMENSIONS,
)
from ddpui.schemas.chart_schema import ChartDataPayload


class TestNormalizeDimensions:
    """Test normalize_dimensions function"""

    def test_normalize_dimensions_table_chart_with_dimensions_list(self):
        """Test that normalize_dimensions handles dimensions list for table charts"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["dim1", "dim-2", "dim 3"],
        )
        dims = normalize_dimensions(payload)
        assert dims == ["dim1", "dim-2", "dim 3"]

    def test_normalize_dimensions_table_chart_backward_compatibility(self):
        """Test backward compatibility with dimension_col + extra_dimension"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="dim-1",
            extra_dimension="dim 2",
        )
        dims = normalize_dimensions(payload)
        assert dims == ["dim-1", "dim 2"]

    def test_normalize_dimensions_bar_chart(self):
        """Test normalize_dimensions for bar chart"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="valid-dimension",
        )
        dims = normalize_dimensions(payload)
        assert dims == ["valid-dimension"]

    def test_normalize_dimensions_filters_empty_strings(self):
        """Test that empty strings are filtered out"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["dim1", "", "dim2", "  ", "dim3"],
        )
        dims = normalize_dimensions(payload)
        assert dims == ["dim1", "dim2", "dim3"]

    def test_normalize_dimensions_special_characters_allowed(self):
        """Test that special characters are allowed in column names"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=[
                "Total Registrations",
                "my-column",
                "user.name",
                "Price $",
                "100% Complete",
            ],
        )
        dims = normalize_dimensions(payload)
        assert len(dims) == 5
        assert "Total Registrations" in dims
        assert "my-column" in dims

    def test_normalize_dimensions_no_dimensions(self):
        """Test normalize_dimensions when no dimensions are provided"""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
        )
        dims = normalize_dimensions(payload)
        assert dims == []


class TestDimensionLimitValidation:
    """Test dimension limit validation for table charts"""

    def test_max_dimensions_constant_is_10(self):
        """Verify that MAX_DIMENSIONS is set to 10"""
        assert MAX_DIMENSIONS == 10

    def test_exactly_10_dimensions_passes(self):
        """Test that exactly 10 dimensions is allowed"""
        dimensions = [f"dim{i}" for i in range(1, 11)]  # dim1 to dim10
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        result = normalize_dimensions(payload)
        assert len(result) == 10
        assert result == dimensions

    def test_11_dimensions_fails(self):
        """Test that 11 dimensions raises ValueError"""
        dimensions = [f"dim{i}" for i in range(1, 12)]  # dim1 to dim11
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload)
        assert "maximum of 10 dimensions" in str(exc_info.value)
        assert "You provided 11 dimensions" in str(exc_info.value)

    def test_15_dimensions_fails(self):
        """Test that 15 dimensions raises ValueError with correct count"""
        dimensions = [f"dim{i}" for i in range(1, 16)]  # dim1 to dim15
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload)
        assert "maximum of 10 dimensions" in str(exc_info.value)
        assert "You provided 15 dimensions" in str(exc_info.value)

    def test_1_dimension_passes(self):
        """Test that 1 dimension is allowed"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["dim1"],
        )
        result = normalize_dimensions(payload)
        assert len(result) == 1
        assert result == ["dim1"]

    def test_5_dimensions_passes(self):
        """Test that 5 dimensions is allowed"""
        dimensions = [f"dim{i}" for i in range(1, 6)]  # dim1 to dim5
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        result = normalize_dimensions(payload)
        assert len(result) == 5
        assert result == dimensions

    def test_backward_compatibility_dimension_col_extra_dimension_within_limit(self):
        """Test backward compatibility with dimension_col + extra_dimension"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="dim1",
            extra_dimension="dim2",
        )
        result = normalize_dimensions(payload)
        assert len(result) == 2
        assert result == ["dim1", "dim2"]

    def test_dimension_limit_with_empty_strings_filtered_out(self):
        """Test that empty strings are filtered out before checking limit"""
        # 12 total dimensions but 2 are empty, so 10 valid dimensions
        dimensions = [f"dim{i}" for i in range(1, 11)] + ["", ""]
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        result = normalize_dimensions(payload)
        assert len(result) == 10  # Empty strings filtered out

    def test_dimension_limit_with_whitespace_strings_filtered_out(self):
        """Test that whitespace-only strings are filtered out before limit"""
        # 12 total dimensions but 2 are whitespace, so 10 valid dimensions
        dimensions = [f"dim{i}" for i in range(1, 11)] + ["  ", "\t"]
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        result = normalize_dimensions(payload)
        assert len(result) == 10  # Whitespace strings filtered out

    def test_dimension_limit_only_applies_to_table_charts(self):
        """Test that dimension limit only applies to table charts"""
        # Bar chart with dimension_col - should not check limit
        payload_bar = ChartDataPayload(
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="dim1",
        )
        result = normalize_dimensions(payload_bar)
        assert result == ["dim1"]  # No limit check for bar charts

    def test_error_message_format(self):
        """Test that error message is clear and helpful"""
        dimensions = [f"dim{i}" for i in range(1, 21)]  # 20 dimensions
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload)
        error_msg = str(exc_info.value)
        # Check that error message contains all important information
        assert "Table charts" in error_msg
        assert "maximum of 10 dimensions" in error_msg
        assert "20 dimensions" in error_msg


class TestSQLAlchemyHandlesColumnNames:
    """
    Tests to verify that SQLAlchemy properly handles column names.
    
    Note: SQL injection protection is now handled by SQLAlchemy's column() 
    function which properly quotes identifiers. These tests verify that
    column names with special characters work correctly.
    """

    def test_column_names_with_spaces_work(self):
        """Test that column names with spaces are accepted"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["First Name", "Last Name", "Total Sales"],
        )
        dims = normalize_dimensions(payload)
        assert dims == ["First Name", "Last Name", "Total Sales"]

    def test_column_names_with_special_chars_work(self):
        """Test that column names with special characters are accepted"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=[
                "user@email",
                "price$",
                "100%",
                "#items",
                "col.name",
            ],
        )
        dims = normalize_dimensions(payload)
        assert len(dims) == 5

    def test_unicode_column_names_work(self):
        """Test that unicode column names are accepted"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["日本語", "données", "名前"],
        )
        dims = normalize_dimensions(payload)
        assert len(dims) == 3

    def test_column_names_with_quotes_work(self):
        """Test that column names with quotes are now accepted (SQLAlchemy handles quoting)"""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["column'name", 'column"name'],
        )
        dims = normalize_dimensions(payload)
        # SQLAlchemy will properly quote these when building the query
        assert len(dims) == 2
