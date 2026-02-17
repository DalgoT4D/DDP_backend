"""Tests for column normalization and dimension limits"""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import (
    normalize_dimensions,
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
