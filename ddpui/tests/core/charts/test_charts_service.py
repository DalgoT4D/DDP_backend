"""Tests for charts_service module - pure functions and query building"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from ddpui.core.charts import charts_service
from ddpui.schemas.chart_schema import ChartDataPayload, ChartMetric, TransformDataForChart


# ============================================================
# apply_time_grain
# ============================================================


class TestApplyTimeGrain:
    def test_no_time_grain_returns_column(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, None)
        assert result is col

    def test_empty_time_grain_returns_column(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "")
        assert result is col

    def test_postgres_uses_date_trunc(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "month", "postgres")
        # Result should be a func.date_trunc call
        assert result is not col

    def test_postgresql_alias(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "year", "postgresql")
        assert result is not col

    def test_bigquery_year(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "year", "bigquery")
        assert result is not col

    def test_bigquery_month(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "month", "bigquery")
        assert result is not col

    def test_bigquery_day(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "day", "bigquery")
        assert result is not col

    def test_bigquery_hour(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "hour", "bigquery")
        assert result is not col

    def test_bigquery_minute(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "minute", "bigquery")
        assert result is not col

    def test_bigquery_second(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "second", "bigquery")
        assert result is not col

    def test_unknown_warehouse_defaults_to_postgres(self):
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "day", "snowflake")
        assert result is not col

    def test_bigquery_unknown_grain_returns_column(self):
        """BigQuery with unrecognized grain falls through to return column_expr"""
        col = MagicMock()
        result = charts_service.apply_time_grain(col, "quarter", "bigquery")
        assert result is col


# ============================================================
# format_time_grain_label
# ============================================================


class TestFormatTimeGrainLabel:
    def test_none_value(self):
        assert charts_service.format_time_grain_label(None, "year") == "Unknown"

    def test_string_with_T(self):
        result = charts_service.format_time_grain_label("2024-01-15T10:30:00", "day")
        assert "Jan 15, 2024" in result

    def test_string_with_space(self):
        result = charts_service.format_time_grain_label("2024-01-15 10:30:00", "month")
        assert "Jan 2024" in result

    def test_date_string_only(self):
        result = charts_service.format_time_grain_label("2024-01-15", "year")
        assert result == "2024"

    def test_unparseable_string(self):
        result = charts_service.format_time_grain_label("not-a-date", "year")
        assert result == "not-a-date"

    def test_date_object(self):
        d = date(2024, 3, 15)
        result = charts_service.format_time_grain_label(d, "month")
        assert result == "Mar 2024"

    def test_datetime_object(self):
        dt = datetime(2024, 3, 15, 14, 30, 45)
        assert charts_service.format_time_grain_label(dt, "year") == "2024"
        assert charts_service.format_time_grain_label(dt, "month") == "Mar 2024"
        assert "Mar 15, 2024" in charts_service.format_time_grain_label(dt, "day")
        assert "14:00" in charts_service.format_time_grain_label(dt, "hour")
        assert "14:30" in charts_service.format_time_grain_label(dt, "minute")
        assert "14:30:45" in charts_service.format_time_grain_label(dt, "second")

    def test_unknown_grain_returns_string(self):
        dt = datetime(2024, 1, 1)
        result = charts_service.format_time_grain_label(dt, "quarter")
        assert result == str(dt)

    def test_non_datetime_type_returns_string(self):
        result = charts_service.format_time_grain_label(12345, "year")
        assert result == "12345"

    def test_string_with_z_suffix(self):
        result = charts_service.format_time_grain_label("2024-01-15T10:30:00Z", "day")
        assert "Jan 15, 2024" in result


# ============================================================
# get_pagination_params
# ============================================================


class TestGetPaginationParams:
    def test_table_chart_returns_none(self):
        payload = ChartDataPayload(chart_type="table", schema_name="public", table_name="t")
        limit, offset = charts_service.get_pagination_params(payload)
        assert limit is None
        assert offset is None

    def test_no_extra_config_returns_none(self):
        payload = ChartDataPayload(chart_type="bar", schema_name="public", table_name="t")
        limit, offset = charts_service.get_pagination_params(payload)
        assert limit is None
        assert offset is None

    def test_pagination_disabled_returns_none(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            extra_config={"pagination": {"enabled": False}},
        )
        limit, offset = charts_service.get_pagination_params(payload)
        assert limit is None

    def test_pagination_enabled_returns_values(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            extra_config={"pagination": {"enabled": True, "page_size": 25}},
        )
        limit, offset = charts_service.get_pagination_params(payload)
        assert limit == 25
        assert offset == 0

    def test_pagination_default_page_size(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            extra_config={"pagination": {"enabled": True}},
        )
        limit, offset = charts_service.get_pagination_params(payload)
        assert limit == 50


# ============================================================
# handle_null_value / safe_get_value / convert_value
# ============================================================


class TestNullHandling:
    def test_handle_null_value_none(self):
        assert charts_service.handle_null_value(None) == "Unknown"

    def test_handle_null_value_custom_label(self):
        assert charts_service.handle_null_value(None, "N/A") == "N/A"

    def test_handle_null_value_not_none(self):
        assert charts_service.handle_null_value("hello") == "hello"

    def test_safe_get_value_existing_key(self):
        assert charts_service.safe_get_value({"a": 10}, "a") == 10

    def test_safe_get_value_missing_key(self):
        assert charts_service.safe_get_value({"a": 10}, "b") == "Unknown"

    def test_safe_get_value_null_value(self):
        assert charts_service.safe_get_value({"a": None}, "a") == "Unknown"

    def test_safe_get_value_custom_null_label(self):
        assert charts_service.safe_get_value({"a": None}, "a", "N/A") == "N/A"


class TestConvertValue:
    def test_none_preserve(self):
        assert charts_service.convert_value(None, preserve_none=True) is None

    def test_none_no_preserve(self):
        assert charts_service.convert_value(None) == "Unknown"

    def test_datetime_value(self):
        dt = datetime(2024, 1, 15, 10, 30)
        result = charts_service.convert_value(dt)
        assert "2024-01-15" in result

    def test_date_value(self):
        d = date(2024, 1, 15)
        result = charts_service.convert_value(d)
        assert result == "2024-01-15"

    def test_decimal_value(self):
        result = charts_service.convert_value(Decimal("3.14"))
        assert result == 3.14
        assert isinstance(result, float)

    def test_regular_value(self):
        assert charts_service.convert_value("hello") == "hello"
        assert charts_service.convert_value(42) == 42


# ============================================================
# normalize_dimensions
# ============================================================


class TestNormalizeDimensions:
    def test_table_with_dimensions_list(self):
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
            dimensions=["col1", "col2"],
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == ["col1", "col2"]

    def test_table_filters_empty_strings(self):
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
            dimensions=["col1", "", "  ", "col2"],
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == ["col1", "col2"]

    def test_table_no_dimensions(self):
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == []

    def test_bar_with_dimension_col(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            dimension_col="category",
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == ["category"]

    def test_bar_with_extra_dimension(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            dimension_col="category",
            extra_dimension="region",
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == ["category", "region"]

    def test_bar_no_dimensions(self):
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
        )
        result = charts_service.normalize_dimensions(payload)
        assert result == []


# ============================================================
# transform_data_for_chart - bar
# ============================================================


class TestTransformDataForBarChart:
    def test_bar_no_metrics_returns_empty(self):
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            metrics=[],
        )
        result = charts_service.transform_data_for_chart([], payload)
        assert result == {}

    def test_bar_single_metric(self):
        results = [
            {"cat": "A", "sum_val": 10},
            {"cat": "B", "sum_val": 20},
        ]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["xAxisData"] == ["A", "B"]
        assert len(data["series"]) == 1
        assert data["series"][0]["data"] == [10, 20]

    def test_bar_multiple_metrics(self):
        results = [
            {"cat": "A", "sum_val": 10, "avg_val": 5},
            {"cat": "B", "sum_val": 20, "avg_val": 15},
        ]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            metrics=[
                ChartMetric(aggregation="sum", column="val"),
                ChartMetric(aggregation="avg", column="val"),
            ],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert len(data["series"]) == 2

    def test_bar_with_extra_dimension_single_metric(self):
        results = [
            {"cat": "A", "region": "East", "sum_val": 10},
            {"cat": "A", "region": "West", "sum_val": 15},
            {"cat": "B", "region": "East", "sum_val": 20},
        ]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            extra_dimension="region",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert "series" in data
        assert "legend" in data
        assert len(data["legend"]) == 2  # East, West

    def test_bar_with_extra_dimension_multiple_metrics(self):
        results = [
            {"cat": "A", "region": "East", "sum_val": 10, "avg_val": 5},
        ]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            extra_dimension="region",
            metrics=[
                ChartMetric(aggregation="sum", column="val"),
                ChartMetric(aggregation="avg", column="val"),
            ],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        # With extra_dimension and multiple metrics, each combo gets a series
        assert len(data["series"]) >= 1

    def test_bar_with_time_grain(self):
        results = [
            {"month": datetime(2024, 1, 1), "sum_val": 100},
            {"month": datetime(2024, 2, 1), "sum_val": 200},
        ]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="month",
            metrics=[ChartMetric(aggregation="sum", column="val")],
            time_grain="month",
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert "Jan 2024" in data["xAxisData"]

    def test_bar_count_all_alias(self):
        results = [{"cat": "A", "count_all": 5}]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="count", column=None)],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["series"][0]["data"] == [5]
        assert "Total Count" in data["legend"]

    def test_bar_count_with_alias(self):
        results = [{"cat": "A", "count_all_my_count": 5}]
        payload = TransformDataForChart(
            chart_type="bar",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="count", column=None, alias="my_count")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["series"][0]["data"] == [5]


# ============================================================
# transform_data_for_chart - pie
# ============================================================


class TestTransformDataForPieChart:
    def test_pie_no_metrics_returns_empty(self):
        payload = TransformDataForChart(
            chart_type="pie",
            dimension_col="cat",
            metrics=[],
        )
        result = charts_service.transform_data_for_chart([], payload)
        assert result == {}

    def test_pie_basic(self):
        results = [
            {"cat": "A", "sum_val": 10},
            {"cat": "B", "sum_val": 20},
        ]
        payload = TransformDataForChart(
            chart_type="pie",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert len(data["pieData"]) == 2
        assert data["pieData"][0]["name"] == "A"

    def test_pie_with_extra_dimension(self):
        results = [
            {"cat": "A", "region": "East", "sum_val": 10},
        ]
        payload = TransformDataForChart(
            chart_type="pie",
            dimension_col="cat",
            extra_dimension="region",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert "A - East" in data["pieData"][0]["name"]

    def test_pie_max_slices(self):
        results = [{"cat": f"C{i}", "sum_val": i} for i in range(10)]
        payload = TransformDataForChart(
            chart_type="pie",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="sum", column="val")],
            customizations={"maxSlices": 3},
        )
        data = charts_service.transform_data_for_chart(results, payload)
        # Should have 3 top slices + "Other"
        assert len(data["pieData"]) == 4
        assert data["pieData"][-1]["name"] == "Other"

    def test_pie_count_all(self):
        results = [{"cat": "A", "count_all": 5}]
        payload = TransformDataForChart(
            chart_type="pie",
            dimension_col="cat",
            metrics=[ChartMetric(aggregation="count", column=None)],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["pieData"][0]["value"] == 5


# ============================================================
# transform_data_for_chart - line
# ============================================================


class TestTransformDataForLineChart:
    def test_line_no_metrics_returns_empty(self):
        payload = TransformDataForChart(
            chart_type="line",
            dimension_col="date",
            metrics=[],
        )
        result = charts_service.transform_data_for_chart([], payload)
        assert result == {}

    def test_line_basic(self):
        results = [
            {"date": "Jan", "sum_val": 10},
            {"date": "Feb", "sum_val": 20},
        ]
        payload = TransformDataForChart(
            chart_type="line",
            dimension_col="date",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["xAxisData"] == ["Jan", "Feb"]
        assert len(data["series"]) == 1

    def test_line_with_extra_dimension(self):
        results = [
            {"date": "Jan", "region": "East", "sum_val": 10},
            {"date": "Jan", "region": "West", "sum_val": 15},
        ]
        payload = TransformDataForChart(
            chart_type="line",
            dimension_col="date",
            extra_dimension="region",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert len(data["legend"]) == 2


# ============================================================
# transform_data_for_chart - number
# ============================================================


class TestTransformDataForNumberChart:
    def test_number_basic(self):
        results = [{"sum_val": 42}]
        payload = TransformDataForChart(
            chart_type="number",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["value"] == 42

    def test_number_empty_results(self):
        payload = TransformDataForChart(
            chart_type="number",
            metrics=[ChartMetric(aggregation="sum", column="val")],
        )
        data = charts_service.transform_data_for_chart([], payload)
        assert data["value"] is None
        assert data["is_null"] is True

    def test_number_count_all(self):
        results = [{"count_all": 100}]
        payload = TransformDataForChart(
            chart_type="number",
            metrics=[ChartMetric(aggregation="count", column=None)],
        )
        data = charts_service.transform_data_for_chart(results, payload)
        assert data["value"] == 100
