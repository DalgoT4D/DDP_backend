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
