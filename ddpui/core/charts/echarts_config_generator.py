"""ECharts configuration generator for different chart types"""
from typing import Dict, List, Any, Optional


class EChartsConfigGenerator:
    """Generate ECharts configurations based on chart type and data"""

    @staticmethod
    def generate_number_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate number/KPI chart configuration"""
        customizations = customizations or {}

        # Get the single value from data
        value = data.get("value", 0)
        is_null = data.get("is_null", False)
        subtitle = customizations.get("subtitle", "")
        number_format = customizations.get("numberFormat", "default")
        decimal_places = customizations.get("decimalPlaces", 0)
        number_size = customizations.get("numberSize", "medium")
        number_prefix = customizations.get("numberPrefix", "")
        number_suffix = customizations.get("numberSuffix", "")

        # Handle None values - show "No data" instead of formatting as number
        if is_null or value is None:
            formatted_value = "No data"
        else:
            # Format the value based on customizations
            formatted_value = EChartsConfigGenerator._format_number(
                value, number_format, decimal_places, number_prefix, number_suffix
            )

        # Map number size to font size
        size_map = {
            "small": 32,
            "medium": 48,
            "large": 64,
        }

        font_size = size_map.get(number_size, 48)

        # Create a custom configuration for displaying a single metric
        # Using a gauge series with custom text display
        config = {
            "title": {
                "text": customizations.get("title", ""),
                "left": "center",
                "top": "10%",
                "textStyle": {"fontSize": 16, "fontWeight": "normal"},
            },
            "series": [
                {
                    "type": "gauge",
                    "center": ["50%", "50%"],
                    "radius": "0%",
                    "startAngle": 0,
                    "endAngle": 0,
                    "axisLine": {"show": False},
                    "splitLine": {"show": False},
                    "axisTick": {"show": False},
                    "axisLabel": {"show": False},
                    "pointer": {"show": False},
                    "detail": {
                        "show": True,
                        "offsetCenter": [0, 0],
                        "formatter": formatted_value,
                        "fontSize": font_size,
                        "fontWeight": "bold",
                        "color": "#333",
                    },
                    "title": {
                        "show": True,
                        "offsetCenter": [
                            0,
                            font_size + 20,
                        ],  # Position subtitle below number based on size
                        "fontSize": 16,
                        "color": "#666",
                        "fontWeight": "normal",
                    },
                    "data": [{"value": value, "name": subtitle}],
                }
            ],
        }

        return config

    @staticmethod
    def _format_number(
        value: float, format_type: str, decimal_places: int, prefix: str = "", suffix: str = ""
    ) -> str:
        """Format number based on type, decimal places, prefix and suffix"""
        # Handle None values
        if value is None:
            return "No data"

        # Format the number based on type
        if format_type == "percentage":
            formatted = f"{value:.{decimal_places}f}%"
        elif format_type == "currency":
            formatted = f"${value:,.{decimal_places}f}"
        elif format_type == "comma":
            formatted = f"{value:,.{decimal_places}f}"
        else:  # default
            if decimal_places > 0:
                formatted = f"{value:.{decimal_places}f}"
            else:
                formatted = str(int(value)) if value == int(value) else str(value)

        # Add prefix and suffix if provided
        if prefix or suffix:
            return f"{prefix}{formatted}{suffix}"

        return formatted

    @staticmethod
    def generate_bar_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate bar chart configuration"""
        customizations = customizations or {}
        orientation = customizations.get("orientation", "vertical")
        is_stacked = customizations.get("stacked", False)
        show_data_labels = customizations.get("showDataLabels", False)
        x_axis_title = customizations.get("xAxisTitle", "")
        y_axis_title = customizations.get("yAxisTitle", "")
        x_axis_label_rotation = customizations.get("xAxisLabelRotation", "horizontal")
        y_axis_label_rotation = customizations.get("yAxisLabelRotation", "horizontal")
        show_tooltip = customizations.get("showTooltip", True)
        show_legend = customizations.get("showLegend", True)
        data_label_position = customizations.get("dataLabelPosition", "top")

        # Convert rotation values to degrees
        rotation_map = {
            "horizontal": 0,
            "45": -45,  # Negative for clockwise rotation
            "vertical": -90,
        }

        config = {
            "title": {"text": customizations.get("title", "")},
            "legend": {"data": data.get("legend", []), "show": show_legend},
            "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
            "xAxis": {
                "type": "category" if orientation == "vertical" else "value",
                "data": data.get("xAxisData", []) if orientation == "vertical" else None,
                "name": x_axis_title,
                "axisLabel": {"rotate": rotation_map.get(x_axis_label_rotation, 0)},
            },
            "yAxis": {
                "type": "value" if orientation == "vertical" else "category",
                "data": data.get("yAxisData", []) if orientation == "horizontal" else None,
                "name": y_axis_title,
                "axisLabel": {"rotate": rotation_map.get(y_axis_label_rotation, 0)},
            },
            "series": [],
        }

        # Build series
        for series_data in data.get("series", []):
            series_config = {
                "name": series_data.get("name", ""),
                "type": "bar",
                "data": series_data.get("data", []),
                "label": {
                    "show": show_data_labels,
                    "position": data_label_position
                    if orientation == "vertical"
                    else (
                        "right"
                        if data_label_position == "top"
                        else "left"
                        if data_label_position == "bottom"
                        else "inside"
                    ),
                },
            }
            if is_stacked:
                series_config["stack"] = "total"
            config["series"].append(series_config)

        # Add tooltip if enabled
        if show_tooltip:
            config["tooltip"] = {"trigger": "axis", "axisPointer": {"type": "shadow"}}

        return config

    @staticmethod
    def generate_pie_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate pie chart configuration"""
        customizations = customizations or {}
        chart_style = customizations.get("chartStyle", "donut")  # Default to donut
        show_data_labels = customizations.get("showDataLabels", True)
        label_format = customizations.get("labelFormat", "percentage")
        data_label_position = customizations.get("dataLabelPosition", "outside")
        legend_position = customizations.get("legendPosition", "right")
        show_legend = customizations.get("showLegend", True)
        show_tooltip = customizations.get("showTooltip", True)

        # Determine label formatter
        formatter_map = {
            "percentage": "{d}%",
            "value": "{c}",
            "name_percentage": "{b}\n{d}%",
            "name_value": "{b}\n{c}",
        }

        # Determine label position mapping
        position_map = {
            "outside": "outside",
            "inside": "inside",
            "center": "center",
            # Legacy mappings for compatibility
            "top": "outside",
            "bottom": "outside",
            "mid": "inside",
        }

        config = {
            "title": {"text": customizations.get("title", "")},
            "series": [
                {
                    "name": data.get("seriesName", "Data"),
                    "type": "pie",
                    "radius": ["40%", "70%"] if chart_style == "donut" else "70%",
                    "avoidLabelOverlap": False,
                    "label": {
                        "show": show_data_labels,
                        "position": position_map.get(data_label_position, "outside"),
                        "formatter": formatter_map.get(label_format, "{d}%"),
                    },
                    "labelLine": {"show": show_data_labels and data_label_position == "outside"},
                    "data": data.get("pieData", []),
                }
            ],
        }

        # Add tooltip if enabled
        if show_tooltip:
            config["tooltip"] = {"trigger": "item", "formatter": "{a} <br/>{b}: {c} ({d}%)"}

        # Add legend if enabled
        if show_legend:
            config["legend"] = {
                "orient": "vertical" if legend_position in ["left", "right"] else "horizontal",
                legend_position: 10 if legend_position in ["left", "right"] else "center",
                "data": [item["name"] for item in data.get("pieData", [])],
            }

        return config

    @staticmethod
    def generate_line_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate line chart configuration"""
        customizations = customizations or {}
        line_style = customizations.get("lineStyle", "smooth")  # Default to smooth
        show_data_labels = customizations.get("showDataLabels", False)
        show_data_points = customizations.get("showDataPoints", True)
        x_axis_title = customizations.get("xAxisTitle", "")
        y_axis_title = customizations.get("yAxisTitle", "")
        x_axis_label_rotation = customizations.get("xAxisLabelRotation", "horizontal")
        y_axis_label_rotation = customizations.get("yAxisLabelRotation", "horizontal")
        show_tooltip = customizations.get("showTooltip", True)
        show_legend = customizations.get("showLegend", True)
        data_label_position = customizations.get("dataLabelPosition", "top")

        # Convert rotation values to degrees
        rotation_map = {
            "horizontal": 0,
            "45": -45,  # Negative for clockwise rotation
            "vertical": -90,
        }

        # Map data label positions for line charts
        position_map = {
            "top": "top",
            "bottom": "bottom",
            "left": "left",
            "right": "right",
        }

        config = {
            "title": {"text": customizations.get("title", "")},
            "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
            "xAxis": {
                "type": "category",
                "data": data.get("xAxisData", []),
                "name": x_axis_title,
                "boundaryGap": False,
                "axisLabel": {"rotate": rotation_map.get(x_axis_label_rotation, 0)},
            },
            "yAxis": {
                "type": "value",
                "name": y_axis_title,
                "axisLabel": {"rotate": rotation_map.get(y_axis_label_rotation, 0)},
            },
            "series": [],
        }

        # Add tooltip if enabled
        if show_tooltip:
            config["tooltip"] = {"trigger": "axis"}

        # Add legend if enabled
        if show_legend:
            config["legend"] = {"data": data.get("legend", [])}

        # Build series
        for series_data in data.get("series", []):
            series_config = {
                "name": series_data.get("name", ""),
                "type": "line",
                "smooth": line_style == "smooth",
                "data": series_data.get("data", []),
                "label": {
                    "show": show_data_labels,
                    "position": position_map.get(data_label_position, "top"),
                },
                "showSymbol": show_data_points,
            }
            config["series"].append(series_config)

        return config

    @staticmethod
    def generate_map_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate choropleth map configuration"""
        customizations = customizations or {}

        # Extract map data
        geojson = data.get("geojson", {})
        map_data = data.get("data", [])
        min_value = data.get("min_value", 0)
        max_value = data.get("max_value", 100)

        # Customization options
        color_scheme = customizations.get("colorScheme", "Blues")
        show_tooltip = customizations.get("showTooltip", True)
        show_legend = customizations.get("showLegend", True)
        roam = customizations.get("roam", True)
        enable_select = customizations.get("select", False)
        zoom = customizations.get("zoom", 1.0)
        center_mode = customizations.get("centerMode", "auto")
        center_lon = customizations.get("centerLon", 0)
        center_lat = customizations.get("centerLat", 0)

        # Color schemes mapping
        color_schemes = {
            "Blues": ["#f0f9ff", "#0369a1"],
            "Reds": ["#fef2f2", "#dc2626"],
            "Greens": ["#f0fdf4", "#16a34a"],
            "Purples": ["#faf5ff", "#9333ea"],
            "Oranges": ["#fff7ed", "#ea580c"],
            "Greys": ["#f9fafb", "#4b5563"],
        }

        colors = color_schemes.get(color_scheme, color_schemes["Blues"])

        # Generate unique map name based on geojson content
        map_name = f"map_{hash(str(geojson)) % 10000}"

        # Build series with customizations
        series_config = {
            "type": "map",
            "map": map_name,
            "data": map_data,
            "roam": roam,  # Enable/disable zoom and pan
            "zoom": zoom,  # Initial zoom level
            "emphasis": {"label": {"show": True}, "itemStyle": {"areaColor": "#ffa500"}},
        }

        # Set center position if custom mode
        if center_mode == "custom":
            series_config["center"] = [center_lon, center_lat]

        # Enable selection if requested
        if enable_select:
            series_config["select"] = {"itemStyle": {"areaColor": "#ff6b6b"}}
            series_config["selectedMode"] = "single"

        config = {
            "title": {"text": customizations.get("title", ""), "left": "center"},
            "series": [series_config],
            "mapData": geojson,  # Custom property for frontend registration
            "mapName": map_name,  # Custom property for frontend registration
        }

        # Add tooltip if enabled
        if show_tooltip:
            config["tooltip"] = {"show": True, "trigger": "item", "formatter": "{b}<br/>{c}"}

        # Add visual map (legend) if enabled
        if show_legend and max_value > min_value:
            config["visualMap"] = {
                "show": True,
                "min": min_value,
                "max": max_value,
                "inRange": {"color": colors},
                "text": ["High", "Low"],
                "calculable": True,
                "left": "left",
                "bottom": "20%",
            }

        return config
