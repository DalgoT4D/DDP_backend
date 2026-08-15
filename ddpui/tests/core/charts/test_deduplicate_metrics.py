"""Tests for _deduplicate_metrics helper function"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.charts.charts_service import _deduplicate_metrics
from ddpui.schemas.chart_schemas import ChartMetric


class TestDeduplicateMetrics:
    """Tests for _deduplicate_metrics helper"""

    def test_empty_list(self):
        """Empty list is returned unchanged"""
        assert _deduplicate_metrics([]) == []

    def test_single_metric(self):
        """Single metric is returned unchanged"""
        metrics = [ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue")]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 1
        assert result[0].alias == "Total Revenue"

    def test_no_duplicates(self):
        """Distinct metrics are all kept"""
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue"),
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 2

    def test_exact_duplicates_removed(self):
        """Identical metrics are collapsed to one"""
        metrics = [
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 1
        assert result[0].alias == "Total Count"

    def test_three_duplicates_collapsed(self):
        """Three identical metrics are collapsed to one"""
        metric = ChartMetric(column="revenue", aggregation="sum", alias="Revenue")
        result = _deduplicate_metrics([metric, metric, metric])
        assert len(result) == 1

    def test_preserves_order_first_kept(self):
        """First occurrence is kept, subsequent duplicates dropped"""
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue"),
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
            ChartMetric(column="revenue", aggregation="sum", alias="Total Revenue"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 2
        assert result[0].alias == "Total Revenue"
        assert result[1].alias == "Total Count"

    def test_same_alias_different_aggregation(self):
        """Metrics with same alias but different aggregation produce same SQL alias — deduplicated"""
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Revenue"),
            ChartMetric(column="revenue", aggregation="avg", alias="Revenue"),
        ]
        result = _deduplicate_metrics(metrics)
        # Both produce alias "Revenue", so second is a duplicate
        assert len(result) == 1

    def test_count_all_duplicates(self):
        """COUNT(*) metrics with same alias are deduplicated"""
        metrics = [
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
            ChartMetric(column=None, aggregation="count", alias="Total Count"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 1

    def test_expression_metrics_deduplicated(self):
        """Expression metrics with same alias are deduplicated"""
        metrics = [
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 1

    def test_expression_metrics_different_alias_kept(self):
        """Expression metrics with different aliases are kept"""
        metrics = [
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio A"),
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio B"),
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 2

    def test_mixed_metrics_with_duplicates(self):
        """Mix of expression, count, and regular metrics with some duplicates"""
        metrics = [
            ChartMetric(column="revenue", aggregation="sum", alias="Revenue"),
            ChartMetric(column=None, aggregation="count", alias="Count"),
            ChartMetric(column="revenue", aggregation="sum", alias="Revenue"),  # dup
            ChartMetric(column_expression="SUM(a)/SUM(b)", alias="Ratio"),
            ChartMetric(column=None, aggregation="count", alias="Count"),  # dup
        ]
        result = _deduplicate_metrics(metrics)
        assert len(result) == 3
        assert result[0].alias == "Revenue"
        assert result[1].alias == "Count"
        assert result[2].alias == "Ratio"
