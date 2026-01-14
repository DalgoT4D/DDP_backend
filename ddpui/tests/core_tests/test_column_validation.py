"""Tests for column name validation to prevent SQL injection"""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import (
    validate_column_name,
    validate_dimension_names,
    normalize_dimensions,
    MAX_DIMENSIONS,
)
from ddpui.schemas.chart_schema import ChartDataPayload, ChartMetric


class TestColumnValidation:
    """Test column name validation for security"""

    def test_validate_column_name_valid_names(self):
        """Test that valid column names pass validation"""
        valid_names = [
            "column_name",
            "ColumnName",
            "column123",
            "_private_column",
            "col_123_name",
            "a",
            "_",
            "column_name_with_many_underscores",
        ]
        for name in valid_names:
            assert validate_column_name(name) is True, f"Expected '{name}' to be valid"

    def test_validate_column_name_invalid_names(self):
        """Test that invalid column names fail validation"""
        invalid_names = [
            "column-name",  # Hyphen not allowed
            "column name",  # Space not allowed
            "column.name",  # Dot not allowed
            "123column",  # Cannot start with number
            "column;DROP TABLE",  # SQL injection attempt
            "column' OR '1'='1",  # SQL injection attempt
            "column/*comment*/",  # SQL comment injection
            "",  # Empty string
            "   ",  # Just whitespace
            "column\nname",  # Newline
            "column\tname",  # Tab
            "column@name",  # Special character
            "column$name",  # Special character
            "column%name",  # Special character
        ]
        for name in invalid_names:
            assert validate_column_name(name) is False, f"Expected '{name}' to be invalid"

    def test_validate_dimension_names_valid(self):
        """Test validation of valid dimension names list"""
        valid_dims = ["dim1", "dim2", "dimension_name"]
        is_valid, error = validate_dimension_names(valid_dims)
        assert is_valid is True
        assert error is None

    def test_validate_dimension_names_invalid(self):
        """Test validation of invalid dimension names list"""
        invalid_dims = ["dim1", "dim-2", "dimension_name"]
        is_valid, error = validate_dimension_names(invalid_dims)
        assert is_valid is False
        assert error is not None
        assert "dim-2" in error

    def test_validate_dimension_names_empty_list(self):
        """Test validation of empty dimension names list"""
        is_valid, error = validate_dimension_names([])
        assert is_valid is True
        assert error is None

    def test_normalize_dimensions_validates_table_chart(self):
        """Test that normalize_dimensions validates dimension names for table charts"""
        # Valid dimensions
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["dim1", "dim2"],
        )
        dims = normalize_dimensions(payload)
        assert dims == ["dim1", "dim2"]

        # Invalid dimensions - should raise ValueError
        payload_invalid = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=["dim1", "dim-2"],  # Invalid: contains hyphen
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload_invalid)
        assert "Invalid column name" in str(exc_info.value)
        assert "dim-2" in str(exc_info.value)

    def test_normalize_dimensions_validates_dimension_col(self):
        """Test that normalize_dimensions validates dimension_col"""
        # Valid dimension_col
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="valid_dimension",
        )
        dims = normalize_dimensions(payload)
        assert dims == ["valid_dimension"]

        # Invalid dimension_col - should raise ValueError
        payload_invalid = ChartDataPayload(
            chart_type="bar",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="invalid-dimension",  # Invalid: contains hyphen
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload_invalid)
        assert "Invalid column name" in str(exc_info.value)
        assert "invalid-dimension" in str(exc_info.value)

    def test_sql_injection_attempts_blocked(self):
        """Test that common SQL injection attempts are blocked"""
        sql_injection_attempts = [
            "col'; DROP TABLE users; --",
            "col' OR '1'='1",
            "col; DELETE FROM table",
            "col UNION SELECT password FROM users",
            "col' AND 1=1 --",
            "col/**/OR/**/1=1",
        ]
        for attempt in sql_injection_attempts:
            assert (
                validate_column_name(attempt) is False
            ), f"SQL injection attempt '{attempt}' should be blocked"

    def test_normalize_dimensions_backward_compatibility(self):
        """Test that backward compatibility with dimension_col + extra_dimension still validates"""
        # Valid backward compatible usage
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="dim1",
            extra_dimension="dim2",
        )
        dims = normalize_dimensions(payload)
        assert dims == ["dim1", "dim2"]

        # Invalid backward compatible usage - should raise ValueError
        payload_invalid = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimension_col="dim1",
            extra_dimension="dim-2",  # Invalid: contains hyphen
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload_invalid)
        assert "Invalid column name" in str(exc_info.value)
        assert "dim-2" in str(exc_info.value)


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
        """Test backward compatibility with dimension_col + extra_dimension (2 dimensions total)"""
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
        """Test that whitespace-only strings are filtered out before checking limit"""
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

    def test_dimension_limit_checked_before_column_validation(self):
        """Test that dimension limit is checked before column name validation"""
        # 11 dimensions, last one has invalid name
        # Should fail on count first, not on invalid column name
        dimensions = [f"dim{i}" for i in range(1, 11)] + ["invalid-dim"]
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="test_schema",
            table_name="test_table",
            dimensions=dimensions,
        )
        with pytest.raises(ValueError) as exc_info:
            normalize_dimensions(payload)
        # Should fail on dimension count, not column name validation
        assert "maximum of 10 dimensions" in str(exc_info.value)

    def test_dimension_limit_only_applies_to_table_charts(self):
        """Test that dimension limit only applies to table charts, not other chart types"""
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
