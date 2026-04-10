"""Tests for ECharts configuration generator"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator


class TestGenerateNumberConfig:
    """Tests for number/KPI chart config generation"""

    def test_basic_number_config(self):
        data = {"value": 42}
        config = EChartsConfigGenerator.generate_number_config(data)
        assert config["series"][0]["type"] == "gauge"
        assert config["series"][0]["detail"]["formatter"] == "42"

    def test_null_value_shows_no_data(self):
        data = {"value": None, "is_null": True}
        config = EChartsConfigGenerator.generate_number_config(data)
        assert config["series"][0]["detail"]["formatter"] == "No data"

    def test_is_null_flag_shows_no_data(self):
        data = {"value": 100, "is_null": True}
        config = EChartsConfigGenerator.generate_number_config(data)
        assert config["series"][0]["detail"]["formatter"] == "No data"

    def test_custom_subtitle(self):
        data = {"value": 10}
        config = EChartsConfigGenerator.generate_number_config(data, {"subtitle": "Total Users"})
        assert config["series"][0]["data"][0]["name"] == "Total Users"

    def test_number_format_percentage(self):
        data = {"value": 75.5}
        config = EChartsConfigGenerator.generate_number_config(
            data, {"numberFormat": "percentage", "decimalPlaces": 1}
        )
        assert "75.5%" in config["series"][0]["detail"]["formatter"]

    def test_number_format_currency(self):
        data = {"value": 1234.56}
        config = EChartsConfigGenerator.generate_number_config(
            data, {"numberFormat": "currency", "decimalPlaces": 2}
        )
        assert "$" in config["series"][0]["detail"]["formatter"]

    def test_number_size_small(self):
        data = {"value": 1}
        config = EChartsConfigGenerator.generate_number_config(data, {"numberSize": "small"})
        assert config["series"][0]["detail"]["fontSize"] == 32

    def test_number_size_large(self):
        data = {"value": 1}
        config = EChartsConfigGenerator.generate_number_config(data, {"numberSize": "large"})
        assert config["series"][0]["detail"]["fontSize"] == 64

    def test_number_size_medium_default(self):
        data = {"value": 1}
        config = EChartsConfigGenerator.generate_number_config(data)
        assert config["series"][0]["detail"]["fontSize"] == 48

    def test_prefix_and_suffix(self):
        data = {"value": 100}
        config = EChartsConfigGenerator.generate_number_config(
            data, {"numberPrefix": "$", "numberSuffix": "M"}
        )
        assert config["series"][0]["detail"]["formatter"] == "$100M"

    def test_title_customization(self):
        data = {"value": 5}
        config = EChartsConfigGenerator.generate_number_config(data, {"title": "Revenue"})
        assert config["title"]["text"] == "Revenue"

    def test_decimal_places_default(self):
        data = {"value": 10.0}
        config = EChartsConfigGenerator.generate_number_config(
            data, {"numberFormat": "default", "decimalPlaces": 0}
        )
        # value 10.0 == int(10.0), so should show "10"
        assert config["series"][0]["detail"]["formatter"] == "10"

    def test_decimal_places_nonzero(self):
        data = {"value": 10.0}
        config = EChartsConfigGenerator.generate_number_config(
            data, {"numberFormat": "default", "decimalPlaces": 2}
        )
        assert "10.00" in config["series"][0]["detail"]["formatter"]


class TestFormatNumber:
    """Tests for the _format_number static method"""

    def test_none_value(self):
        result = EChartsConfigGenerator._format_number(None, "default", 0)
        assert result == "No data"

    def test_percentage_format(self):
        result = EChartsConfigGenerator._format_number(75.123, "percentage", 2)
        assert result == "75.12%"

    def test_currency_format(self):
        result = EChartsConfigGenerator._format_number(1234.5, "currency", 2)
        assert result == "$1,234.50"

    def test_default_format_integer(self):
        result = EChartsConfigGenerator._format_number(42.0, "default", 0)
        assert result == "42"

    def test_default_format_float(self):
        result = EChartsConfigGenerator._format_number(42.5, "default", 0)
        assert result == "42.5"

    def test_default_with_decimals(self):
        result = EChartsConfigGenerator._format_number(42.123, "default", 2)
        assert result == "42.12"

    def test_with_prefix_suffix(self):
        result = EChartsConfigGenerator._format_number(100, "default", 0, "$", "K")
        assert result == "$100K"


class TestGenerateBarConfig:
    """Tests for bar chart config generation"""

    def test_basic_vertical_bar(self):
        data = {
            "xAxisData": ["A", "B", "C"],
            "series": [{"name": "Sales", "data": [10, 20, 30]}],
            "legend": ["Sales"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data)
        assert config["xAxis"]["type"] == "category"
        assert config["yAxis"]["type"] == "value"
        assert len(config["series"]) == 1
        assert config["series"][0]["type"] == "bar"

    def test_horizontal_bar(self):
        data = {
            "yAxisData": ["A", "B"],
            "series": [{"name": "S1", "data": [1, 2]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"orientation": "horizontal"})
        assert config["xAxis"]["type"] == "value"
        assert config["yAxis"]["type"] == "category"

    def test_stacked_bar(self):
        data = {
            "xAxisData": ["A"],
            "series": [
                {"name": "S1", "data": [1]},
                {"name": "S2", "data": [2]},
            ],
            "legend": ["S1", "S2"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"stacked": True})
        for s in config["series"]:
            assert s["stack"] == "total"

    def test_data_labels_shown(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"showDataLabels": True})
        assert config["series"][0]["label"]["show"] is True

    def test_tooltip_disabled(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"showTooltip": False})
        assert "tooltip" not in config

    def test_tooltip_enabled_by_default(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data)
        assert "tooltip" in config

    def test_axis_titles(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(
            data, {"xAxisTitle": "Category", "yAxisTitle": "Value"}
        )
        assert config["xAxis"]["name"] == "Category"
        assert config["yAxis"]["name"] == "Value"

    def test_axis_rotation(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"xAxisLabelRotation": "45"})
        assert config["xAxis"]["axisLabel"]["rotate"] == -45

    def test_vertical_rotation(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(
            data, {"xAxisLabelRotation": "vertical"}
        )
        assert config["xAxis"]["axisLabel"]["rotate"] == -90

    def test_horizontal_bar_label_position_mapping(self):
        """For horizontal bars, 'top' maps to 'right', 'bottom' to 'left'"""
        data = {
            "yAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(
            data,
            {
                "orientation": "horizontal",
                "showDataLabels": True,
                "dataLabelPosition": "top",
            },
        )
        assert config["series"][0]["label"]["position"] == "right"

        config2 = EChartsConfigGenerator.generate_bar_config(
            data,
            {
                "orientation": "horizontal",
                "showDataLabels": True,
                "dataLabelPosition": "bottom",
            },
        )
        assert config2["series"][0]["label"]["position"] == "left"

    def test_legend_hidden(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_bar_config(data, {"showLegend": False})
        assert config["legend"]["show"] is False


class TestGeneratePieConfig:
    """Tests for pie chart config generation"""

    def test_basic_donut(self):
        data = {
            "pieData": [{"name": "A", "value": 10}, {"name": "B", "value": 20}],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(data)
        assert config["series"][0]["type"] == "pie"
        # Donut has inner radius
        assert isinstance(config["series"][0]["radius"], list)

    def test_pie_style(self):
        data = {
            "pieData": [{"name": "A", "value": 10}],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(data, {"chartStyle": "pie"})
        # Pie has string radius, not list
        assert config["series"][0]["radius"] == "70%"

    def test_tooltip_enabled(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(data)
        assert "tooltip" in config

    def test_tooltip_disabled(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(data, {"showTooltip": False})
        assert "tooltip" not in config

    def test_legend_enabled(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(data)
        assert "legend" in config

    def test_legend_disabled(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(data, {"showLegend": False})
        assert "legend" not in config

    def test_label_format_value(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(
            data, {"showDataLabels": True, "labelFormat": "value"}
        )
        assert config["series"][0]["label"]["formatter"] == "{c}"

    def test_label_format_name_percentage(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(
            data, {"showDataLabels": True, "labelFormat": "name_percentage"}
        )
        assert config["series"][0]["label"]["formatter"] == "{b}\n{d}%"

    def test_data_label_position_inside(self):
        data = {"pieData": [{"name": "A", "value": 10}], "seriesName": "Test"}
        config = EChartsConfigGenerator.generate_pie_config(
            data, {"showDataLabels": True, "dataLabelPosition": "inside"}
        )
        assert config["series"][0]["label"]["position"] == "inside"

    def test_legend_position_left_adjusts_center(self):
        """When legend is on left with 'all' display, pie center shifts right"""
        data = {
            "pieData": [{"name": f"Item{i}", "value": i} for i in range(6)],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(
            data,
            {"showLegend": True, "legendDisplay": "all", "legendPosition": "left"},
        )
        assert config["series"][0]["center"][0] == "60%"

    def test_legend_position_right_adjusts_center(self):
        data = {
            "pieData": [{"name": f"Item{i}", "value": i} for i in range(6)],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(
            data,
            {"showLegend": True, "legendDisplay": "all", "legendPosition": "right"},
        )
        assert config["series"][0]["center"][0] == "40%"

    def test_legend_position_top_adjusts_center(self):
        data = {
            "pieData": [{"name": f"Item{i}", "value": i} for i in range(9)],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(
            data,
            {"showLegend": True, "legendDisplay": "all", "legendPosition": "top"},
        )
        assert config["series"][0]["center"][1] == "60%"

    def test_legend_position_bottom_adjusts_center(self):
        data = {
            "pieData": [{"name": f"Item{i}", "value": i} for i in range(9)],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(
            data,
            {"showLegend": True, "legendDisplay": "all", "legendPosition": "bottom"},
        )
        assert config["series"][0]["center"][1] == "40%"

    def test_many_legends_shrinks_pie(self):
        """With >10 legends, pie radius should shrink"""
        data = {
            "pieData": [{"name": f"Item{i}", "value": i} for i in range(12)],
            "seriesName": "Test",
        }
        config = EChartsConfigGenerator.generate_pie_config(
            data,
            {"showLegend": True, "legendDisplay": "all", "legendPosition": "left"},
        )
        # Radius should be smaller for many legends
        assert config["series"][0]["radius"][1] == "60%"


class TestGenerateLineConfig:
    """Tests for line chart config generation"""

    def test_basic_line(self):
        data = {
            "xAxisData": ["Jan", "Feb", "Mar"],
            "series": [{"name": "Sales", "data": [10, 20, 30]}],
            "legend": ["Sales"],
        }
        config = EChartsConfigGenerator.generate_line_config(data)
        assert config["series"][0]["type"] == "line"
        assert config["series"][0]["smooth"] is True  # Default smooth

    def test_straight_line_style(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_line_config(data, {"lineStyle": "straight"})
        assert config["series"][0]["smooth"] is False

    def test_show_data_points(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_line_config(data, {"showDataPoints": False})
        assert config["series"][0]["showSymbol"] is False

    def test_tooltip_axis_trigger(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_line_config(data)
        assert config["tooltip"]["trigger"] == "axis"

    def test_tooltip_disabled(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_line_config(data, {"showTooltip": False})
        assert "tooltip" not in config

    def test_multiple_series(self):
        data = {
            "xAxisData": ["A", "B"],
            "series": [
                {"name": "S1", "data": [1, 2]},
                {"name": "S2", "data": [3, 4]},
            ],
            "legend": ["S1", "S2"],
        }
        config = EChartsConfigGenerator.generate_line_config(data)
        assert len(config["series"]) == 2

    def test_data_label_position(self):
        data = {
            "xAxisData": ["A"],
            "series": [{"name": "S1", "data": [1]}],
            "legend": ["S1"],
        }
        config = EChartsConfigGenerator.generate_line_config(
            data, {"showDataLabels": True, "dataLabelPosition": "bottom"}
        )
        assert config["series"][0]["label"]["position"] == "bottom"


class TestGenerateMapConfig:
    """Tests for map chart config generation"""

    def test_basic_map_config(self):
        data = {
            "geojson": {"type": "FeatureCollection", "features": []},
            "data": [{"name": "Region A", "value": 100}],
            "min_value": 0,
            "max_value": 100,
        }
        config = EChartsConfigGenerator.generate_map_config(data)
        assert config["series"][0]["type"] == "map"
        assert "mapData" in config
        assert "mapName" in config

    def test_tooltip_enabled(self):
        data = {
            "geojson": {},
            "data": [],
            "min_value": 0,
            "max_value": 100,
        }
        config = EChartsConfigGenerator.generate_map_config(data)
        assert "tooltip" in config

    def test_tooltip_disabled(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"showTooltip": False})
        assert "tooltip" not in config

    def test_visual_map_legend(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data)
        assert "visualMap" in config
        assert config["visualMap"]["min"] == 0
        assert config["visualMap"]["max"] == 100

    def test_visual_map_hidden_when_no_legend(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"showLegend": False})
        assert "visualMap" not in config

    def test_visual_map_hidden_when_same_min_max(self):
        data = {"geojson": {}, "data": [], "min_value": 50, "max_value": 50}
        config = EChartsConfigGenerator.generate_map_config(data)
        assert "visualMap" not in config

    def test_color_scheme_reds(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"colorScheme": "Reds"})
        assert config["visualMap"]["inRange"]["color"] == ["#fef2f2", "#dc2626"]

    def test_roam_disabled(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"roam": False})
        assert config["series"][0]["roam"] is False

    def test_custom_center(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(
            data, {"centerMode": "custom", "centerLon": 78.0, "centerLat": 22.0}
        )
        assert config["series"][0]["center"] == [78.0, 22.0]

    def test_selection_enabled(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"select": True})
        assert "select" in config["series"][0]
        assert config["series"][0]["selectedMode"] == "single"

    def test_zoom_setting(self):
        data = {"geojson": {}, "data": [], "min_value": 0, "max_value": 100}
        config = EChartsConfigGenerator.generate_map_config(data, {"zoom": 2.5})
        assert config["series"][0]["zoom"] == 2.5


class TestBuildLegendConfig:
    """Tests for _build_legend_config"""

    def test_legend_hidden(self):
        config = EChartsConfigGenerator._build_legend_config([], False)
        assert config == {"show": False}

    def test_paginated_legend(self):
        config = EChartsConfigGenerator._build_legend_config(["A", "B"], True, "paginated", "top")
        assert config["type"] == "scroll"
        assert config["orient"] == "horizontal"

    def test_all_legend_top(self):
        config = EChartsConfigGenerator._build_legend_config(["A", "B"], True, "all", "top")
        assert config["type"] == "plain"
        assert config["orient"] == "horizontal"
        assert config["top"] == 20

    def test_all_legend_bottom(self):
        config = EChartsConfigGenerator._build_legend_config(["A", "B"], True, "all", "bottom")
        assert config["orient"] == "horizontal"
        assert "bottom" in config

    def test_all_legend_left(self):
        config = EChartsConfigGenerator._build_legend_config(["A", "B", "C"], True, "all", "left")
        assert config["orient"] == "vertical"
        assert config["left"] == 20

    def test_all_legend_right(self):
        config = EChartsConfigGenerator._build_legend_config(["A", "B", "C"], True, "all", "right")
        assert config["orient"] == "vertical"
        assert config["right"] == 20

    def test_many_vertical_legends_reduce_font(self):
        legends = [f"Legend{i}" for i in range(16)]
        config = EChartsConfigGenerator._build_legend_config(legends, True, "all", "left")
        assert config["textStyle"]["fontSize"] == 10
        assert config["itemGap"] == 8

    def test_medium_vertical_legends_reduce_font(self):
        legends = [f"Legend{i}" for i in range(10)]
        config = EChartsConfigGenerator._build_legend_config(legends, True, "all", "right")
        assert config["textStyle"]["fontSize"] == 11
        assert config["itemGap"] == 10

    def test_wide_horizontal_legends_reduce_font(self):
        """When total legend width exceeds ~800px, font shrinks"""
        legends = [f"VeryLongLegendName{i}" for i in range(20)]
        config = EChartsConfigGenerator._build_legend_config(legends, True, "all", "top")
        assert config["textStyle"]["fontSize"] == 10

    def test_none_display_defaults_to_paginated(self):
        config = EChartsConfigGenerator._build_legend_config(["A"], True, None, "top")
        assert config["type"] == "scroll"

    def test_none_position_defaults_to_top(self):
        config = EChartsConfigGenerator._build_legend_config(["A"], True, "all", None)
        assert config["type"] == "plain"


class TestCalculateGridSpacing:
    """Tests for _calculate_grid_spacing"""

    def test_no_legend_returns_default(self):
        grid = EChartsConfigGenerator._calculate_grid_spacing([], False)
        assert grid["containLabel"] is True

    def test_empty_legend_data(self):
        grid = EChartsConfigGenerator._calculate_grid_spacing(None, True)
        assert "containLabel" in grid

    def test_paginated_legend_extra_top(self):
        grid = EChartsConfigGenerator._calculate_grid_spacing(["A"], True, "paginated", "top")
        assert grid["top"] == "80px"

    def test_all_legend_top_wrapping(self):
        """Many legends at top should increase top spacing"""
        legends = [f"VeryLongLegend{i}" for i in range(20)]
        grid = EChartsConfigGenerator._calculate_grid_spacing(legends, True, "all", "top")
        # Should have increased top spacing
        top_px = int(grid["top"].replace("px", ""))
        assert top_px > 100

    def test_all_legend_bottom(self):
        legends = ["A", "B"]
        grid = EChartsConfigGenerator._calculate_grid_spacing(legends, True, "all", "bottom")
        assert "px" in grid["bottom"]

    def test_all_legend_left(self):
        legends = ["A", "B", "C"]
        grid = EChartsConfigGenerator._calculate_grid_spacing(legends, True, "all", "left")
        assert "px" in grid["left"]

    def test_all_legend_right(self):
        legends = ["A", "B", "C"]
        grid = EChartsConfigGenerator._calculate_grid_spacing(legends, True, "all", "right")
        assert "px" in grid["right"]

    def test_legend_width_clamped_min(self):
        """Very short legends should still have minimum width"""
        grid = EChartsConfigGenerator._calculate_grid_spacing(["A"], True, "all", "left")
        width = int(grid["left"].replace("px", ""))
        assert width >= 120

    def test_legend_width_clamped_max(self):
        """Very long legends should be capped at max width"""
        legends = ["A" * 100]
        grid = EChartsConfigGenerator._calculate_grid_spacing(legends, True, "all", "right")
        width = int(grid["right"].replace("px", ""))
        assert width <= 250
