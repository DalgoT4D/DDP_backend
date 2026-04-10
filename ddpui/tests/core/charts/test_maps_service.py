"""Tests for maps service module"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import MagicMock, patch
from ddpui.core.charts.maps_service import (
    normalize_region_name,
    transform_data_for_map,
)

pytestmark = pytest.mark.django_db


class TestNormalizeRegionName:
    def test_basic_normalization(self):
        assert normalize_region_name("  Maharashtra ") == "maharashtra"

    def test_none_returns_empty(self):
        assert normalize_region_name(None) == ""

    def test_empty_string(self):
        assert normalize_region_name("") == ""

    def test_number_input(self):
        assert normalize_region_name(123) == "123"

    def test_mixed_case(self):
        assert normalize_region_name("New Delhi") == "new delhi"


class TestTransformDataForMap:
    def _make_geojson(self, region_names):
        return {
            "type": "FeatureCollection",
            "features": [{"properties": {"name": name}, "geometry": {}} for name in region_names],
        }

    def test_basic_transform(self):
        results = [
            {"state": "Maharashtra", "sum_val": 100},
            {"state": "Karnataka", "sum_val": 200},
        ]
        geojson = self._make_geojson(["Maharashtra", "Karnataka", "Tamil Nadu"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert data["matched_regions"] == 2
        assert data["total_regions"] == 3
        assert data["min_value"] == 100
        assert data["max_value"] == 200

    def test_case_insensitive_matching(self):
        results = [{"state": "maharashtra", "sum_val": 50}]
        geojson = self._make_geojson(["Maharashtra"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert data["matched_regions"] == 1

    def test_no_matches(self):
        results = [{"state": "Unknown", "sum_val": 50}]
        geojson = self._make_geojson(["Maharashtra"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert data["matched_regions"] == 0
        assert data["min_value"] == 0
        assert data["max_value"] == 100  # default when no values

    def test_count_all_metric(self):
        results = [{"state": "A", "count_all": 10}]
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "count", "column": None}],
        )
        assert data["data"][0]["value"] == 10

    def test_count_all_with_alias(self):
        results = [{"state": "A", "count_all_total": 10}]
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "count", "column": None, "alias": "total"}],
        )
        assert data["data"][0]["value"] == 10

    def test_metric_with_alias(self):
        results = [{"state": "A", "my_sum": 42}]
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val", "alias": "my_sum"}],
        )
        assert data["data"][0]["value"] == 42

    def test_available_metrics_info(self):
        results = [{"state": "A", "sum_val": 10}]
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert len(data["available_metrics"]) == 1
        assert data["available_metrics"][0]["is_selected"] is True

    def test_selected_metric_index(self):
        results = [{"state": "A", "sum_a": 10, "avg_b": 20}]
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            results,
            geojson,
            "state",
            metrics=[
                {"aggregation": "sum", "column": "a"},
                {"aggregation": "avg", "column": "b"},
            ],
            selected_metric_index=1,
        )
        assert data["selected_metric_index"] == 1
        assert data["available_metrics"][1]["is_selected"] is True

    def test_empty_results(self):
        geojson = self._make_geojson(["A", "B"])
        data = transform_data_for_map(
            [],
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert data["matched_regions"] == 0
        assert len(data["data"]) == 2

    def test_geojson_data_passed_through(self):
        geojson = self._make_geojson(["A"])
        data = transform_data_for_map(
            [{"state": "A", "sum_val": 1}],
            geojson,
            "state",
            metrics=[{"aggregation": "sum", "column": "val"}],
        )
        assert data["geojson"] == geojson
