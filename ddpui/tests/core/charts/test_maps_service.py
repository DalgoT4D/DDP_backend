"""Tests for map data transformation, esp. calculated (column_expression) metrics.

Regression: transform_data_for_map used dict access (.get / metric['aggregation']) that crashed
on ChartMetric objects and on expression metrics (aggregation is None). Maps must support
calculated metrics like every other aggregated chart type.
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.maps_service import transform_data_for_map
from ddpui.schemas.chart_schemas import ChartMetric

pytestmark = pytest.mark.django_db

GEOJSON = {"features": [{"properties": {"name": "Karnataka"}}]}


def _results(alias, value):
    return [{"state_name": "Karnataka", alias: value}]


class TestMapCalculatedMetrics:
    def test_calculated_metric_object(self):
        """ChartMetric object with column_expression (aggregation=None) must not crash and
        must read the value back via the metric alias."""
        metric = ChartMetric(
            column=None,
            aggregation=None,
            column_expression="sum(district_population)",
            alias="sum(district_population)",
        )
        out = transform_data_for_map(
            _results("sum(district_population)", 312500999),
            GEOJSON,
            "state_name",
            None,
            {},
            [metric],
            0,
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "sum(district_population)"

    def test_calculated_metric_resolved_dict(self):
        """Saved calculated metrics arrive resolved as dicts — also supported."""
        metric = {
            "column": None,
            "aggregation": None,
            "column_expression": "avg(district_population)",
            "alias": "avg pop",
        }
        out = transform_data_for_map(
            _results("avg pop", 42.0), GEOJSON, "state_name", None, {}, [metric], 0
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "avg pop"

    def test_simple_count_metric_still_works(self):
        """Guard: the existing simple-metric path is unchanged."""
        metric = ChartMetric(column=None, aggregation="count", alias="Total Count")
        out = transform_data_for_map(
            _results("count_all_Total Count", 15), GEOJSON, "state_name", None, {}, [metric], 0
        )
        assert out["matched_regions"] == 1
        assert out["available_metrics"][0]["display_name"] == "Total Count"
