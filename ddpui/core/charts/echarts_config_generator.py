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
        # Center all content vertically as a unit using offsetCenter for positioning

        # Calculate offset to position chart title above the number
        # Title should be positioned above the number value
        title_offset_y = -(font_size // 2 + 30)  # Position title above number
        subtitle_offset_y = font_size // 2 + 25  # Position subtitle below number

        config = {
            "title": {
                "text": customizations.get("title", ""),
                "left": "center",
                "top": "middle",  # Use middle positioning
                "textStyle": {
                    "fontSize": 16,
                    "fontWeight": "normal",
                    "color": "#666",
                    "lineHeight": 20,
                },
                # Offset the title upward to sit above the number
                "subtextStyle": {"fontSize": 14},
                # Use padding to create space
                "padding": [0, 0, font_size + 60, 0],  # [top, right, bottom, left]
            },
            "series": [
                {
                    "type": "gauge",
                    "center": ["50%", "50%"],  # Always center
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
                        "offsetCenter": [0, subtitle_offset_y],
                        "fontSize": 14,
                        "color": "#999",
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
        legend_display = customizations.get("legendDisplay", "paginated")
        legend_position = customizations.get("legendPosition", "top")
        data_label_position = customizations.get("dataLabelPosition", "top")

        # Convert rotation values to degrees
        rotation_map = {
            "horizontal": 0,
            "45": -45,  # Negative for clockwise rotation
            "vertical": -90,
        }

        legend_data = data.get("legend", [])
        legend_config = EChartsConfigGenerator._build_legend_config(
            legend_data, show_legend, legend_display, legend_position
        )
        grid_config = EChartsConfigGenerator._calculate_grid_spacing(
            legend_data, show_legend, legend_display, legend_position
        )

        config = {
            "title": {"text": customizations.get("title", "")},
            "legend": legend_config,
            "grid": grid_config,
            "xAxis": {
                "type": "category" if orientation == "vertical" else "value",
                "data": data.get("xAxisData", []) if orientation == "vertical" else None,
                "name": x_axis_title,
                "nameLocation": "center",
                "nameGap": 30,
                "axisLabel": {"rotate": rotation_map.get(x_axis_label_rotation, 0)},
            },
            "yAxis": {
                "type": "value" if orientation == "vertical" else "category",
                "data": data.get("yAxisData", []) if orientation == "horizontal" else None,
                "name": y_axis_title,
                "nameLocation": "center",
                "nameGap": 50,
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
        show_data_labels = customizations.get("showDataLabels", False)
        label_format = customizations.get("labelFormat", "percentage")
        data_label_position = customizations.get("dataLabelPosition", "outside")
        legend_position = customizations.get("legendPosition", "top")
        legend_display = customizations.get("legendDisplay", "paginated")
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

        # Calculate pie positioning based on legend
        legend_data = [item["name"] for item in data.get("pieData", [])]
        pie_center = ["50%", "50%"]  # Default center
        pie_radius = ["40%", "70%"] if chart_style == "donut" else "70%"

        # Adjust pie position and size based on legend placement when showing all legends
        if show_legend and legend_display == "all" and legend_data:
            num_legends = len(legend_data)

            if legend_position == "left":
                pie_center[0] = "60%"  # Move pie to the right
                if num_legends > 10:
                    pie_radius = ["35%", "60%"] if chart_style == "donut" else "60%"
                elif num_legends > 5:
                    pie_radius = ["37%", "65%"] if chart_style == "donut" else "65%"

            elif legend_position == "right":
                pie_center[0] = "40%"  # Move pie to the left
                if num_legends > 10:
                    pie_radius = ["35%", "60%"] if chart_style == "donut" else "60%"
                elif num_legends > 5:
                    pie_radius = ["37%", "65%"] if chart_style == "donut" else "65%"

            elif legend_position == "top":
                pie_center[1] = "60%"  # Move pie down
                if num_legends > 8:  # Many legends might wrap to multiple rows
                    pie_radius = ["35%", "60%"] if chart_style == "donut" else "60%"

            elif legend_position == "bottom":
                pie_center[1] = "40%"  # Move pie up more to avoid overlap
                if num_legends > 8:
                    pie_radius = ["30%", "55%"] if chart_style == "donut" else "55%"
                elif num_legends > 4:
                    pie_radius = ["35%", "60%"] if chart_style == "donut" else "60%"

        config = {
            "title": {"text": customizations.get("title", "")},
            "series": [
                {
                    "name": data.get("seriesName", "Data"),
                    "type": "pie",
                    "radius": pie_radius,
                    "center": pie_center,
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
            legend_data = [item["name"] for item in data.get("pieData", [])]
            config["legend"] = EChartsConfigGenerator._build_legend_config(
                legend_data, show_legend, legend_display, legend_position
            )

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
        legend_display = customizations.get("legendDisplay", "paginated")
        legend_position = customizations.get("legendPosition", "top")
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

        legend_data = data.get("legend", [])
        legend_config = EChartsConfigGenerator._build_legend_config(
            legend_data, show_legend, legend_display, legend_position
        )
        grid_config = EChartsConfigGenerator._calculate_grid_spacing(
            legend_data, show_legend, legend_display, legend_position
        )

        config = {
            "title": {"text": customizations.get("title", "")},
            "legend": legend_config,
            "grid": grid_config,
            "xAxis": {
                "type": "category",
                "data": data.get("xAxisData", []),
                "name": x_axis_title,
                "nameLocation": "center",
                "nameGap": 30,
                "boundaryGap": False,
                "axisLabel": {"rotate": rotation_map.get(x_axis_label_rotation, 0)},
            },
            "yAxis": {
                "type": "value",
                "name": y_axis_title,
                "nameLocation": "center",
                "nameGap": 50,
                "axisLabel": {"rotate": rotation_map.get(y_axis_label_rotation, 0)},
            },
            "series": [],
        }

        # Add tooltip if enabled
        if show_tooltip:
            config["tooltip"] = {"trigger": "axis"}

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

    @staticmethod
    def _build_legend_config(
        legend_data: List[str],
        show_legend: bool,
        legend_display: str = "paginated",
        legend_position: str = "top",
    ) -> Dict[str, Any]:
        """Build legend configuration based on display and position options with proper spacing"""
        if not show_legend:
            return {"show": False}

        # Ensure we have valid parameters with defaults
        legend_display = legend_display or "paginated"
        legend_position = legend_position or "top"

        # For "all" display mode, ensure we have a valid position
        if legend_display == "all" and not legend_position:
            legend_position = "top"

        num_legends = len(legend_data) if legend_data else 0

        # Base legend configuration
        legend_config = {
            "data": legend_data,
            "show": True,
            "textStyle": {"fontSize": 12, "color": "#333"},
            "itemGap": 15,  # Gap between legend items
            "padding": [10, 15, 10, 15],  # [top, right, bottom, left] padding around legend
        }

        # Configure legend based on display type
        if legend_display == "paginated":
            # Use scrollable legend (default behavior) - always horizontal at top
            legend_config.update(
                {
                    "type": "scroll",
                    "orient": "horizontal",
                    "top": 15,
                    "left": "center",
                    "pageIconColor": "#2f4554",
                    "pageIconInactiveColor": "#aaa",
                    "pageIconSize": 15,
                    "pageTextStyle": {"color": "#333"},
                    "animation": True,
                    "pageButtonItemGap": 5,
                }
            )
        else:  # legend_display == "all"
            # Show all legends in chart area with specified position
            legend_config["type"] = "plain"

            # Estimate space needed for legends
            avg_char_width = 8  # pixels per character
            icon_width = 25  # legend icon + gap
            item_gap = legend_config["itemGap"]

            # Configure position and orientation based on legend_position
            if legend_position == "top":
                legend_config.update(
                    {
                        "orient": "horizontal",
                        "top": 20,
                        "left": "center",
                    }
                )
                # For horizontal legends, calculate if they need wrapping
                if num_legends > 0:
                    # Estimate total width needed
                    estimated_width = sum(
                        len(str(item)) * avg_char_width + icon_width for item in legend_data
                    )
                    estimated_width += (num_legends - 1) * item_gap

                    # If too wide, enable wrapping or use smaller font
                    if estimated_width > 800:  # Assume chart width ~800px
                        legend_config["textStyle"]["fontSize"] = 10
                        legend_config["itemGap"] = 10

            elif legend_position == "bottom":
                legend_config.update(
                    {
                        "orient": "horizontal",
                        "bottom": 30,  # More space from chart edge
                        "left": "center",
                    }
                )
                # Similar width estimation as top
                if num_legends > 0:
                    estimated_width = sum(
                        len(str(item)) * avg_char_width + icon_width for item in legend_data
                    )
                    estimated_width += (num_legends - 1) * item_gap

                    if estimated_width > 800:
                        legend_config["textStyle"]["fontSize"] = 10
                        legend_config["itemGap"] = 10
                        legend_config["bottom"] = 35  # Even more space when text is smaller

            elif legend_position == "left":
                legend_config.update(
                    {
                        "orient": "vertical",
                        "left": 20,
                        "top": "middle",
                    }
                )
                # For vertical legends, adjust item gap based on number of items
                if num_legends > 15:  # Many items
                    legend_config["itemGap"] = 8
                    legend_config["textStyle"]["fontSize"] = 10
                elif num_legends > 8:
                    legend_config["itemGap"] = 10
                    legend_config["textStyle"]["fontSize"] = 11

            else:  # right (default)
                legend_config.update(
                    {
                        "orient": "vertical",
                        "right": 20,
                        "top": "middle",
                    }
                )
                # Similar vertical adjustments as left
                if num_legends > 15:
                    legend_config["itemGap"] = 8
                    legend_config["textStyle"]["fontSize"] = 10
                elif num_legends > 8:
                    legend_config["itemGap"] = 10
                    legend_config["textStyle"]["fontSize"] = 11

        return legend_config

    @staticmethod
    def _calculate_grid_spacing(
        legend_data: List[str],
        show_legend: bool,
        legend_display: str = "paginated",
        legend_position: str = "top",
    ) -> Dict[str, str]:
        """Calculate grid spacing to accommodate legends with proper margins"""
        # Default grid spacing
        default_grid = {
            "left": "3%",
            "right": "4%",
            "top": "60px",  # Space for title
            "bottom": "3%",
            "containLabel": True,
        }

        if not show_legend or not legend_data:
            return default_grid

        num_legends = len(legend_data)

        # For paginated legends, minimal adjustment needed (they're compact)
        if legend_display == "paginated":
            default_grid["top"] = "80px"  # Extra space for pagination controls
            return default_grid

        # For "all" legends, calculate space based on position and content
        if legend_display == "all":
            avg_char_width = 8
            icon_width = 25
            item_gap = 15
            legend_padding = 30  # padding around legend

            if legend_position == "top":
                # Estimate height needed for horizontal legend
                estimated_width = sum(
                    len(str(item)) * avg_char_width + icon_width for item in legend_data
                )
                estimated_width += (num_legends - 1) * item_gap

                # Calculate number of rows needed if wrapping occurs
                chart_width = 800  # assumed chart width
                if estimated_width > chart_width:
                    rows_needed = max(1, (estimated_width // chart_width) + 1)
                    legend_height = rows_needed * 25 + legend_padding  # 25px per row
                else:
                    legend_height = 40 + legend_padding  # single row

                default_grid["top"] = f"{60 + legend_height}px"

            elif legend_position == "bottom":
                # Similar calculation as top but with more generous spacing
                estimated_width = sum(
                    len(str(item)) * avg_char_width + icon_width for item in legend_data
                )
                estimated_width += (num_legends - 1) * item_gap

                chart_width = 800
                if estimated_width > chart_width:
                    rows_needed = max(1, (estimated_width // chart_width) + 1)
                    legend_height = rows_needed * 30 + legend_padding + 20  # Extra space for bottom
                else:
                    legend_height = 50 + legend_padding + 20  # More space for single row at bottom

                default_grid["bottom"] = f"{legend_height}px"

            elif legend_position == "left":
                # Calculate width needed for vertical legend
                max_text_width = (
                    max(len(str(item)) * avg_char_width for item in legend_data)
                    if legend_data
                    else 0
                )
                legend_width = max_text_width + icon_width + legend_padding

                # Ensure minimum and maximum widths
                legend_width = max(120, min(legend_width, 250))  # 120px min, 250px max

                default_grid["left"] = f"{legend_width}px"

            else:  # right
                # Calculate width needed for vertical legend
                max_text_width = (
                    max(len(str(item)) * avg_char_width for item in legend_data)
                    if legend_data
                    else 0
                )
                legend_width = max_text_width + icon_width + legend_padding

                # Ensure minimum and maximum widths
                legend_width = max(120, min(legend_width, 250))

                default_grid["right"] = f"{legend_width}px"

        return default_grid
