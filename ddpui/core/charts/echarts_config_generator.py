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
        subtitle = customizations.get("subtitle", "")
        number_format = customizations.get("numberFormat", "default")
        decimal_places = customizations.get("decimalPlaces", 0)

        # Format the value based on customizations
        formatted_value = EChartsConfigGenerator._format_number(
            value, number_format, decimal_places
        )

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
                        "fontSize": 48,
                        "fontWeight": "bold",
                        "color": "#333",
                    },
                    "title": {
                        "show": True,
                        "offsetCenter": [0, 60],
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
    def _format_number(value: float, format_type: str, decimal_places: int) -> str:
        """Format number based on type and decimal places"""
        if format_type == "percentage":
            return f"{value:.{decimal_places}f}%"
        elif format_type == "currency":
            return f"${value:,.{decimal_places}f}"
        elif format_type == "comma":
            return f"{value:,.{decimal_places}f}"
        else:  # default
            if decimal_places > 0:
                return f"{value:.{decimal_places}f}"
            else:
                return str(int(value)) if value == int(value) else str(value)

    @staticmethod
    def generate_bar_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate bar chart configuration"""
        customizations = customizations or {}
        orientation = customizations.get("orientation", "vertical")
        is_stacked = customizations.get("stacked", False)
        show_data_labels = customizations.get("showDataLabels", False)
        x_axis_title = customizations.get("xAxisTitle", "")
        y_axis_title = customizations.get("yAxisTitle", "")

        config = {
            "title": {"text": customizations.get("title", "")},
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "legend": {"data": data.get("legend", [])},
            "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
            "xAxis": {
                "type": "category" if orientation == "vertical" else "value",
                "data": data.get("xAxisData", []) if orientation == "vertical" else None,
                "name": x_axis_title,
            },
            "yAxis": {
                "type": "value" if orientation == "vertical" else "category",
                "data": data.get("yAxisData", []) if orientation == "horizontal" else None,
                "name": y_axis_title,
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
                    "position": "top" if orientation == "vertical" else "right",
                },
            }
            if is_stacked:
                series_config["stack"] = "total"
            config["series"].append(series_config)

        return config

    @staticmethod
    def generate_pie_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate pie chart configuration"""
        customizations = customizations or {}
        chart_style = customizations.get("chartStyle", "pie")
        show_data_labels = customizations.get("showDataLabels", True)
        label_format = customizations.get("labelFormat", "percentage")
        legend_position = customizations.get("legendPosition", "right")

        # Determine label formatter
        formatter_map = {
            "percentage": "{b}: {d}%",
            "value": "{b}: {c}",
            "name_percentage": "{b}\n{d}%",
            "name_value": "{b}\n{c}",
        }

        config = {
            "title": {"text": customizations.get("title", "")},
            "tooltip": {"trigger": "item", "formatter": "{a} <br/>{b}: {c} ({d}%)"},
            "legend": {
                "orient": "vertical" if legend_position in ["left", "right"] else "horizontal",
                legend_position: 10 if legend_position in ["left", "right"] else "center",
                "data": [item["name"] for item in data.get("pieData", [])],
            },
            "series": [
                {
                    "name": data.get("seriesName", "Data"),
                    "type": "pie",
                    "radius": ["40%", "70%"] if chart_style == "donut" else "70%",
                    "avoidLabelOverlap": False,
                    "label": {
                        "show": show_data_labels,
                        "formatter": formatter_map.get(label_format, "{b}: {d}%"),
                    },
                    "labelLine": {"show": show_data_labels},
                    "data": data.get("pieData", []),
                }
            ],
        }

        return config

    @staticmethod
    def generate_line_config(data: Dict[str, Any], customizations: Dict[str, Any] = None) -> Dict:
        """Generate line chart configuration"""
        customizations = customizations or {}
        line_style = customizations.get("lineStyle", "straight")
        show_data_labels = customizations.get("showDataLabels", False)
        show_data_points = customizations.get("showDataPoints", True)
        x_axis_title = customizations.get("xAxisTitle", "")
        y_axis_title = customizations.get("yAxisTitle", "")

        config = {
            "title": {"text": customizations.get("title", "")},
            "tooltip": {"trigger": "axis"},
            "legend": {"data": data.get("legend", [])},
            "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
            "xAxis": {
                "type": "category",
                "data": data.get("xAxisData", []),
                "name": x_axis_title,
                "boundaryGap": False,
            },
            "yAxis": {"type": "value", "name": y_axis_title},
            "series": [],
        }

        # Build series
        for series_data in data.get("series", []):
            series_config = {
                "name": series_data.get("name", ""),
                "type": "line",
                "smooth": line_style == "smooth",
                "data": series_data.get("data", []),
                "label": {"show": show_data_labels, "position": "top"},
                "showSymbol": show_data_points,
            }
            config["series"].append(series_config)

        return config
