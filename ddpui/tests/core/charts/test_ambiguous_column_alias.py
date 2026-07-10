"""Tests for duplicate / ambiguous column-alias prevention.

Verifies that build_multi_metric_query and build_chart_query
produce unique column labels even when the same column name
appears multiple times in dimensions or metrics, which would
otherwise cause SQLAlchemy's "Ambiguous column name" error.
"""

import os
import re

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import MagicMock

from ddpui.core.charts import charts_service
from ddpui.core.charts.charts_service import _make_unique_alias
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.schemas.chart_schemas import ChartDataPayload, ChartMetric


@pytest.fixture
def mock_warehouse():
    wh = MagicMock()
    wh.wtype = "postgres"
    return wh


def _compile(qb):
    return str(qb.build().compile(compile_kwargs={"literal_binds": True}))


# ---------------------------------------------------------------------------
# _make_unique_alias
# ---------------------------------------------------------------------------


class TestMakeUniqueAlias:
    def test_first_use_returns_unchanged(self):
        used = set()
        assert _make_unique_alias("col", used) == "col"
        assert "col" in used

    def test_second_use_appends_1(self):
        used = {"col"}
        assert _make_unique_alias("col", used) == "col_1"
        assert "col_1" in used

    def test_third_use_appends_2(self):
        used = {"col", "col_1"}
        assert _make_unique_alias("col", used) == "col_2"

    def test_different_alias_not_affected(self):
        used = {"col"}
        assert _make_unique_alias("other", used) == "other"


# ---------------------------------------------------------------------------
# Duplicate dimension columns
# ---------------------------------------------------------------------------


class TestDuplicateDimensions:
    def test_duplicate_dimension_in_aggregated_query(self, mock_warehouse):
        """Same column twice in dimensions → only one SELECT label."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
            dimensions=["advanced_prof", "advanced_prof", "region"],
            metrics=[ChartMetric(aggregation="count", column=None)],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("t", "public")
        charts_service.build_multi_metric_query(payload, qb, mock_warehouse)
        sql = _compile(qb)
        assert sql.count("AS advanced_prof") == 1

    def test_duplicate_dimension_in_non_aggregated_table(self, mock_warehouse):
        """Non-aggregated table with duplicate dims → only one column."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
            dimensions=["advanced_prof", "region", "advanced_prof"],
            metrics=None,
        )
        qb = charts_service.build_chart_query(payload, mock_warehouse)
        sql = _compile(qb)
        assert sql.count("AS advanced_prof") == 1


# ---------------------------------------------------------------------------
# Metric alias colliding with dimension label
# ---------------------------------------------------------------------------


class TestMetricAliasDimensionCollision:
    def test_metric_alias_equals_dimension_name(self, mock_warehouse):
        """Metric with explicit alias matching a dimension name is suffixed."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            dimension_col="advanced_prof",
            metrics=[ChartMetric(aggregation="sum", column="score", alias="advanced_prof")],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("t", "public")
        charts_service.build_multi_metric_query(payload, qb, mock_warehouse)
        sql = _compile(qb)

        # Dimension keeps original label
        assert "AS advanced_prof" in sql
        # Metric gets suffixed label
        assert "advanced_prof_1" in sql
        # Metric object was updated so downstream code stays consistent
        assert payload.metrics[0].alias == "advanced_prof_1"

    def test_auto_alias_no_collision(self, mock_warehouse):
        """Auto-generated alias (agg_col) doesn't collide with dimension."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            dimension_col="advanced_prof",
            metrics=[ChartMetric(aggregation="sum", column="advanced_prof")],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("t", "public")
        charts_service.build_multi_metric_query(payload, qb, mock_warehouse)
        sql = _compile(qb)
        assert "sum_advanced_prof" in sql


# ---------------------------------------------------------------------------
# Duplicate metric aliases
# ---------------------------------------------------------------------------


class TestDuplicateMetricAliases:
    def test_two_metrics_same_alias(self, mock_warehouse):
        """Two metrics with identical alias → second gets _1."""
        payload = ChartDataPayload(
            chart_type="bar",
            schema_name="public",
            table_name="t",
            dimension_col="region",
            metrics=[
                ChartMetric(aggregation="sum", column="score", alias="Total"),
                ChartMetric(aggregation="avg", column="score", alias="Total"),
            ],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("t", "public")
        charts_service.build_multi_metric_query(payload, qb, mock_warehouse)

        assert payload.metrics[0].alias == "Total"
        assert payload.metrics[1].alias == "Total_1"


# ---------------------------------------------------------------------------
# Backward-compatibility: normal (no-collision) cases unchanged
# ---------------------------------------------------------------------------


class TestNoCollisionUnchanged:
    def test_distinct_dimensions_and_metrics(self, mock_warehouse):
        """Normal usage: distinct dims and metrics pass through untouched."""
        payload = ChartDataPayload(
            chart_type="table",
            schema_name="public",
            table_name="t",
            dimensions=["col_a", "col_b"],
            metrics=[
                ChartMetric(aggregation="count", column=None, alias="Total Count"),
                ChartMetric(aggregation="sum", column="revenue", alias="Revenue"),
            ],
        )
        qb = AggQueryBuilder()
        qb.fetch_from("t", "public")
        charts_service.build_multi_metric_query(payload, qb, mock_warehouse)
        sql = _compile(qb)

        assert "col_a" in sql
        assert "col_b" in sql
        assert "Total Count" in sql
        assert "Revenue" in sql
        # Aliases unchanged
        assert payload.metrics[0].alias == "Total Count"
        assert payload.metrics[1].alias == "Revenue"
