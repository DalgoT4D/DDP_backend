"""Tests for validate_metric_column_types – ensures numeric-only aggregations
(AVG, SUM) are rejected when applied to non-numeric columns."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import MagicMock, patch

from ddpui.core.charts.charts_service import validate_metric_column_types
from ddpui.core.datainsights.insights.insight_interface import TranslateColDataType
from ddpui.schemas.chart_schemas import ChartMetric


def _make_org_warehouse():
    return MagicMock()


def _table_columns(*specs):
    """Build a list of column dicts from (name, translated_type) tuples."""
    return [
        {"name": name, "data_type": "mock", "translated_type": ttype, "nullable": True}
        for name, ttype in specs
    ]


# ── Happy-path: numeric columns with numeric aggregations ──────────────────


class TestValidateMetricColumnTypesNumeric:
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_avg_on_numeric_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("amount", TranslateColDataType.NUMERIC),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="amount", aggregation="avg", alias="avg_amount")

        # Should not raise
        validate_metric_column_types(
            [metric], "public", "orders", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_sum_on_numeric_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("price", TranslateColDataType.NUMERIC),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="price", aggregation="sum", alias="total_price")

        validate_metric_column_types(
            [metric], "public", "products", _make_org_warehouse()
        )


# ── Error-path: non-numeric columns with numeric-only aggregations ─────────


class TestValidateMetricColumnTypesNonNumeric:
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_avg_on_text_column_raises(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("Log book record", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(
            column="Log book record", aggregation="avg", alias="avg_log"
        )

        with pytest.raises(ValueError, match="AVG requires a numeric column"):
            validate_metric_column_types(
                [metric], "public", "task_view_agg", _make_org_warehouse()
            )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_sum_on_text_column_raises(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="name", aggregation="sum", alias="sum_name")

        with pytest.raises(ValueError, match="SUM requires a numeric column"):
            validate_metric_column_types(
                [metric], "public", "users", _make_org_warehouse()
            )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_avg_on_datetime_column_raises(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("created_at", TranslateColDataType.DATETIME),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(
            column="created_at", aggregation="avg", alias="avg_date"
        )

        with pytest.raises(ValueError, match="AVG requires a numeric column"):
            validate_metric_column_types(
                [metric], "public", "events", _make_org_warehouse()
            )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_sum_on_boolean_column_raises(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("is_active", TranslateColDataType.BOOL),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(
            column="is_active", aggregation="sum", alias="sum_active"
        )

        with pytest.raises(ValueError, match="SUM requires a numeric column"):
            validate_metric_column_types(
                [metric], "public", "users", _make_org_warehouse()
            )


# ── Non-numeric aggregations on any column type should pass ────────────────


class TestValidateMetricColumnTypesNonNumericAggregations:
    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_count_on_text_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="name", aggregation="count", alias="count_name")

        validate_metric_column_types(
            [metric], "public", "users", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_min_on_text_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="name", aggregation="min", alias="min_name")

        validate_metric_column_types(
            [metric], "public", "users", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_max_on_text_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(column="name", aggregation="max", alias="max_name")

        validate_metric_column_types(
            [metric], "public", "users", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_count_distinct_on_text_column_passes(self, mock_get_client):
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(
            column="name", aggregation="count_distinct", alias="distinct_names"
        )

        validate_metric_column_types(
            [metric], "public", "users", _make_org_warehouse()
        )


# ── Edge cases ─────────────────────────────────────────────────────────────


class TestValidateMetricColumnTypesEdgeCases:
    def test_no_metrics_does_nothing(self):
        """Empty or None metrics list should not raise."""
        validate_metric_column_types(None, "public", "t", _make_org_warehouse())
        validate_metric_column_types([], "public", "t", _make_org_warehouse())

    def test_no_org_warehouse_does_nothing(self):
        metric = ChartMetric(column="x", aggregation="avg", alias="a")
        validate_metric_column_types([metric], "public", "t", None)

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_column_expression_skips_validation(self, mock_get_client):
        """Metrics using raw column_expression should bypass column type checks."""
        metric = ChartMetric(
            column_expression="CAST(col AS NUMERIC)", alias="expr"
        )

        # get_warehouse_client should not even be called
        validate_metric_column_types(
            [metric], "public", "t", _make_org_warehouse()
        )
        mock_get_client.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_count_with_no_column_skips_validation(self, mock_get_client):
        """COUNT(*) metrics (column=None) should not trigger validation."""
        metric = ChartMetric(column=None, aggregation="count", alias="total")

        validate_metric_column_types(
            [metric], "public", "t", _make_org_warehouse()
        )
        mock_get_client.assert_not_called()

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_column_not_in_metadata_passes(self, mock_get_client):
        """If column isn't found in metadata (e.g. computed column), skip validation."""
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("other_col", TranslateColDataType.NUMERIC),
        )
        mock_get_client.return_value = client

        metric = ChartMetric(
            column="missing_col", aggregation="avg", alias="avg_missing"
        )

        # Should not raise — unknown column is not blocked
        validate_metric_column_types(
            [metric], "public", "t", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_warehouse_error_does_not_block(self, mock_get_client):
        """If warehouse metadata lookup fails, validation is skipped gracefully."""
        mock_get_client.side_effect = Exception("connection error")

        metric = ChartMetric(column="x", aggregation="avg", alias="a")

        # Should not raise
        validate_metric_column_types(
            [metric], "public", "t", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_mixed_metrics_validates_only_numeric_aggs(self, mock_get_client):
        """Only metrics with avg/sum should be checked; count/min/max pass freely."""
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
            ("amount", TranslateColDataType.NUMERIC),
        )
        mock_get_client.return_value = client

        metrics = [
            ChartMetric(column="name", aggregation="count", alias="count_name"),
            ChartMetric(column="amount", aggregation="avg", alias="avg_amount"),
        ]

        # Should not raise — count on text is fine, avg on numeric is fine
        validate_metric_column_types(
            metrics, "public", "orders", _make_org_warehouse()
        )

    @patch("ddpui.core.charts.charts_service.get_warehouse_client")
    def test_mixed_metrics_one_bad_raises(self, mock_get_client):
        """If any metric has a bad column-aggregation pairing, raise."""
        client = MagicMock()
        client.get_table_columns.return_value = _table_columns(
            ("name", TranslateColDataType.STRING),
            ("amount", TranslateColDataType.NUMERIC),
        )
        mock_get_client.return_value = client

        metrics = [
            ChartMetric(column="amount", aggregation="sum", alias="total"),
            ChartMetric(column="name", aggregation="avg", alias="bad_avg"),
        ]

        with pytest.raises(ValueError, match="AVG requires a numeric column"):
            validate_metric_column_types(
                metrics, "public", "orders", _make_org_warehouse()
            )
