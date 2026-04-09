"""Tests for chart validation module"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.chart_validator import ChartValidator, ChartValidationError


class TestValidateBasicFields:
    """Tests for _validate_basic_fields"""

    def test_missing_chart_type(self):
        with pytest.raises(ChartValidationError, match="Chart type is required"):
            ChartValidator._validate_basic_fields("", "public", "table")

    def test_none_chart_type(self):
        with pytest.raises(ChartValidationError, match="Chart type is required"):
            ChartValidator._validate_basic_fields(None, "public", "table")

    def test_invalid_chart_type(self):
        with pytest.raises(ChartValidationError, match="Invalid chart type"):
            ChartValidator._validate_basic_fields("scatter", "public", "table")

    def test_missing_schema_name(self):
        with pytest.raises(ChartValidationError, match="Schema name is required"):
            ChartValidator._validate_basic_fields("bar", "", "table")

    def test_missing_table_name(self):
        with pytest.raises(ChartValidationError, match="Table name is required"):
            ChartValidator._validate_basic_fields("bar", "public", "")

    def test_valid_fields(self):
        # Should not raise
        ChartValidator._validate_basic_fields("bar", "public", "my_table")

    def test_all_valid_chart_types(self):
        for chart_type in ["bar", "pie", "line", "number", "map", "table"]:
            ChartValidator._validate_basic_fields(chart_type, "public", "t")


class TestValidateAggregateFunction:
    """Tests for _validate_aggregate_function"""

    def test_valid_functions(self):
        for func in ["sum", "avg", "count", "min", "max", "count_distinct"]:
            ChartValidator._validate_aggregate_function(func)

    def test_invalid_function(self):
        with pytest.raises(ChartValidationError, match="Invalid aggregate function"):
            ChartValidator._validate_aggregate_function("median")

    def test_none_function_allowed(self):
        # None should pass (optional)
        ChartValidator._validate_aggregate_function(None)

    def test_empty_string_allowed(self):
        # Empty string is falsy, so treated as None
        ChartValidator._validate_aggregate_function("")


class TestValidateMetrics:
    """Tests for _validate_metrics"""

    def test_empty_metrics(self):
        with pytest.raises(ChartValidationError, match="requires at least one metric"):
            ChartValidator._validate_metrics([], "bar")

    def test_none_metrics(self):
        with pytest.raises(ChartValidationError, match="requires at least one metric"):
            ChartValidator._validate_metrics(None, "bar")

    def test_multiple_not_allowed(self):
        metrics = [
            {"aggregation": "sum", "column": "a"},
            {"aggregation": "avg", "column": "b"},
        ]
        with pytest.raises(ChartValidationError, match="support only one metric"):
            ChartValidator._validate_metrics(metrics, "pie", allow_multiple=False)

    def test_non_dict_metric(self):
        with pytest.raises(ChartValidationError, match="must be a dictionary"):
            ChartValidator._validate_metrics(["not_a_dict"], "bar")

    def test_missing_aggregation(self):
        with pytest.raises(ChartValidationError, match="requires aggregation"):
            ChartValidator._validate_metrics([{"column": "a"}], "bar")

    def test_missing_column_for_non_count(self):
        with pytest.raises(ChartValidationError, match="requires column"):
            ChartValidator._validate_metrics([{"aggregation": "sum"}], "bar")

    def test_count_without_column_ok(self):
        # count doesn't require a column
        ChartValidator._validate_metrics([{"aggregation": "count", "column": None}], "bar")

    def test_valid_metric(self):
        ChartValidator._validate_metrics([{"aggregation": "sum", "column": "revenue"}], "bar")


class TestValidateBarChart:
    """Tests for _validate_bar_chart"""

    def test_missing_dimension(self):
        with pytest.raises(ChartValidationError, match="requires dimension"):
            ChartValidator._validate_bar_chart(None, [{"aggregation": "sum", "column": "a"}])

    def test_valid_bar_chart(self):
        ChartValidator._validate_bar_chart("category", [{"aggregation": "sum", "column": "a"}])

    def test_multiple_metrics_allowed(self):
        ChartValidator._validate_bar_chart(
            "category",
            [
                {"aggregation": "sum", "column": "a"},
                {"aggregation": "avg", "column": "b"},
            ],
        )


class TestValidatePieChart:
    """Tests for _validate_pie_chart"""

    def test_missing_dimension(self):
        with pytest.raises(ChartValidationError, match="requires dimension"):
            ChartValidator._validate_pie_chart(None, [{"aggregation": "count", "column": None}])

    def test_multiple_metrics_not_allowed(self):
        with pytest.raises(ChartValidationError, match="support only one metric"):
            ChartValidator._validate_pie_chart(
                "category",
                [
                    {"aggregation": "sum", "column": "a"},
                    {"aggregation": "avg", "column": "b"},
                ],
            )

    def test_valid_pie_chart(self):
        ChartValidator._validate_pie_chart(
            "category", [{"aggregation": "sum", "column": "revenue"}]
        )


class TestValidateLineChart:
    """Tests for _validate_line_chart"""

    def test_missing_dimension(self):
        with pytest.raises(ChartValidationError, match="requires dimension"):
            ChartValidator._validate_line_chart(None, [{"aggregation": "sum", "column": "a"}])

    def test_valid_line_chart(self):
        ChartValidator._validate_line_chart("date", [{"aggregation": "avg", "column": "price"}])


class TestValidateNumberChart:
    """Tests for _validate_number_chart"""

    def test_valid_number_chart(self):
        ChartValidator._validate_number_chart([{"aggregation": "count", "column": None}])

    def test_invalid_number_format(self):
        with pytest.raises(ChartValidationError, match="Invalid number format"):
            ChartValidator._validate_number_chart(
                [{"aggregation": "count", "column": None}],
                {"numberFormat": "bogus"},
            )

    def test_valid_number_formats(self):
        for fmt in [
            "default",
            "percentage",
            "currency",
            "indian",
            "international",
            "european",
            "adaptive_international",
            "adaptive_indian",
        ]:
            ChartValidator._validate_number_chart(
                [{"aggregation": "count", "column": None}],
                {"numberFormat": fmt},
            )

    def test_negative_decimal_places(self):
        with pytest.raises(ChartValidationError, match="between 0 and 10"):
            ChartValidator._validate_number_chart(
                [{"aggregation": "count", "column": None}],
                {"decimalPlaces": -1},
            )

    def test_too_many_decimal_places(self):
        with pytest.raises(ChartValidationError, match="between 0 and 10"):
            ChartValidator._validate_number_chart(
                [{"aggregation": "count", "column": None}],
                {"decimalPlaces": 11},
            )

    def test_invalid_decimal_places_type(self):
        with pytest.raises(ChartValidationError, match="valid number"):
            ChartValidator._validate_number_chart(
                [{"aggregation": "count", "column": None}],
                {"decimalPlaces": "abc"},
            )

    def test_valid_decimal_places(self):
        ChartValidator._validate_number_chart(
            [{"aggregation": "count", "column": None}],
            {"decimalPlaces": 5},
        )

    def test_no_customizations(self):
        ChartValidator._validate_number_chart([{"aggregation": "sum", "column": "total"}])


class TestValidateMapChart:
    """Tests for _validate_map_chart"""

    def test_missing_geographic_column(self):
        with pytest.raises(ChartValidationError, match="requires geographic column"):
            ChartValidator._validate_map_chart(None, 1, [{"aggregation": "sum", "column": "a"}])

    def test_missing_geojson_id(self):
        with pytest.raises(ChartValidationError, match="requires selected GeoJSON"):
            ChartValidator._validate_map_chart(
                "state", None, [{"aggregation": "sum", "column": "a"}]
            )

    def test_invalid_color_scheme(self):
        with pytest.raises(ChartValidationError, match="Invalid color scheme"):
            ChartValidator._validate_map_chart(
                "state",
                1,
                [{"aggregation": "sum", "column": "a"}],
                {"colorScheme": "Rainbow"},
            )

    def test_valid_color_schemes(self):
        for scheme in ["Blues", "Reds", "Greens", "Purples", "Oranges", "Greys"]:
            ChartValidator._validate_map_chart(
                "state",
                1,
                [{"aggregation": "sum", "column": "a"}],
                {"colorScheme": scheme},
            )

    def test_valid_map_chart(self):
        ChartValidator._validate_map_chart("state", 1, [{"aggregation": "sum", "column": "a"}])

    def test_multiple_metrics_not_allowed(self):
        with pytest.raises(ChartValidationError, match="support only one metric"):
            ChartValidator._validate_map_chart(
                "state",
                1,
                [
                    {"aggregation": "sum", "column": "a"},
                    {"aggregation": "avg", "column": "b"},
                ],
            )


class TestValidateChartConfig:
    """Tests for the main validate_chart_config method"""

    def test_valid_bar_chart(self):
        is_valid, error = ChartValidator.validate_chart_config(
            "bar",
            {
                "dimension_column": "category",
                "metrics": [{"aggregation": "sum", "column": "revenue"}],
            },
            "public",
            "sales",
        )
        assert is_valid is True
        assert error is None

    def test_valid_number_chart(self):
        is_valid, error = ChartValidator.validate_chart_config(
            "number",
            {"metrics": [{"aggregation": "count", "column": None}]},
            "public",
            "sales",
        )
        assert is_valid is True

    def test_valid_table_chart(self):
        is_valid, error = ChartValidator.validate_chart_config("table", {}, "public", "sales")
        assert is_valid is True

    def test_invalid_returns_error(self):
        is_valid, error = ChartValidator.validate_chart_config(
            "bar", {"metrics": []}, "public", "sales"
        )
        assert is_valid is False
        assert error is not None

    def test_unexpected_exception_handled(self):
        """Exceptions other than ChartValidationError are caught"""
        is_valid, error = ChartValidator.validate_chart_config(
            "bar",
            None,  # Will cause AttributeError on .get()
            "public",
            "sales",
        )
        assert is_valid is False
        assert "Validation error" in error

    def test_valid_map_chart(self):
        is_valid, error = ChartValidator.validate_chart_config(
            "map",
            {
                "geographic_column": "state",
                "selected_geojson_id": 1,
                "metrics": [{"aggregation": "sum", "column": "value"}],
            },
            "public",
            "sales",
        )
        assert is_valid is True


class TestValidateForUpdate:
    """Tests for validate_for_update"""

    def test_new_chart_type_triggers_full_validation(self):
        is_valid, error = ChartValidator.validate_for_update(
            "bar",
            "pie",
            {
                "dimension_column": "cat",
                "metrics": [{"aggregation": "sum", "column": "val"}],
            },
            "public",
            "t",
        )
        assert is_valid is True

    def test_no_chart_type_change_lenient(self):
        is_valid, error = ChartValidator.validate_for_update(
            "bar", None, {"customizations": {}}, "public", "t"
        )
        assert is_valid is True

    def test_invalid_number_format_in_update(self):
        is_valid, error = ChartValidator.validate_for_update(
            "number",
            None,
            {"customizations": {"numberFormat": "bogus"}},
            "public",
            "t",
        )
        assert is_valid is False
        assert "Invalid number format" in error

    def test_valid_number_format_in_update(self):
        is_valid, error = ChartValidator.validate_for_update(
            "number",
            None,
            {"customizations": {"numberFormat": "percentage"}},
            "public",
            "t",
        )
        assert is_valid is True

    def test_no_customizations_in_update(self):
        is_valid, error = ChartValidator.validate_for_update("bar", None, {}, "public", "t")
        assert is_valid is True
