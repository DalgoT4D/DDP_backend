"""Chart validation module for validating chart configurations"""

from typing import Dict, List, Optional, Tuple
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.charts.validator")


class ChartValidationError(Exception):
    """Custom exception for chart validation errors"""

    pass


class ChartValidator:
    """Validates chart configurations based on chart type and computation type"""

    # Valid chart types
    VALID_CHART_TYPES = ["bar", "pie", "line", "number", "map", "table"]

    # Valid computation types
    VALID_COMPUTATION_TYPES = ["raw", "aggregated"]

    # Valid aggregate functions
    VALID_AGGREGATE_FUNCTIONS = ["sum", "avg", "count", "min", "max", "count_distinct"]

    @staticmethod
    def validate_chart_config(
        chart_type: str,
        computation_type: str,
        extra_config: Dict,
        schema_name: str,
        table_name: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate chart configuration based on chart type and computation type

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Basic validation
            ChartValidator._validate_basic_fields(
                chart_type, computation_type, schema_name, table_name
            )

            # Extract column configurations from extra_config
            x_axis = extra_config.get("x_axis_column")
            y_axis = extra_config.get("y_axis_column")
            dimension_col = extra_config.get("dimension_column")
            customizations = extra_config.get("customizations", {})

            # Extract multiple metrics for bar/line charts
            metrics = extra_config.get("metrics", [])

            # Validate based on chart type
            if chart_type == "bar":
                ChartValidator._validate_bar_chart(
                    computation_type,
                    x_axis,
                    y_axis,
                    dimension_col,
                    metrics,
                )
            elif chart_type == "pie":
                ChartValidator._validate_pie_chart(
                    computation_type, x_axis, y_axis, dimension_col, metrics
                )
            elif chart_type == "line":
                ChartValidator._validate_line_chart(
                    computation_type,
                    x_axis,
                    y_axis,
                    dimension_col,
                    metrics,
                )
            elif chart_type == "number":
                ChartValidator._validate_number_chart(computation_type, metrics, customizations)
            elif chart_type == "map":
                ChartValidator._validate_map_chart(
                    computation_type,
                    extra_config.get("geographic_column"),
                    extra_config.get("value_column"),
                    extra_config.get("selected_geojson_id"),
                    metrics,
                    customizations,
                )
            elif chart_type == "table":
                ChartValidator._validate_table_chart(computation_type, schema_name, table_name)

            return True, None

        except ChartValidationError as e:
            logger.warning(f"Chart validation failed: {str(e)}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Unexpected error during chart validation: {str(e)}")
            return False, f"Validation error: {str(e)}"

    @staticmethod
    def _validate_basic_fields(
        chart_type: str, computation_type: str, schema_name: str, table_name: str
    ) -> None:
        """Validate basic required fields"""
        if not chart_type:
            raise ChartValidationError("Chart type is required")

        if chart_type not in ChartValidator.VALID_CHART_TYPES:
            raise ChartValidationError(
                f"Invalid chart type '{chart_type}'. Must be one of: {', '.join(ChartValidator.VALID_CHART_TYPES)}"
            )

        if not computation_type:
            raise ChartValidationError("Computation type is required")

        if computation_type not in ChartValidator.VALID_COMPUTATION_TYPES:
            raise ChartValidationError(
                f"Invalid computation type '{computation_type}'. Must be one of: {', '.join(ChartValidator.VALID_COMPUTATION_TYPES)}"
            )

        if not schema_name:
            raise ChartValidationError("Schema name is required")

        if not table_name:
            raise ChartValidationError("Table name is required")

    @staticmethod
    def _validate_aggregate_function(aggregate_func: Optional[str]) -> None:
        """Validate aggregate function"""
        if aggregate_func and aggregate_func not in ChartValidator.VALID_AGGREGATE_FUNCTIONS:
            raise ChartValidationError(
                f"Invalid aggregate function '{aggregate_func}'. Must be one of: {', '.join(ChartValidator.VALID_AGGREGATE_FUNCTIONS)}"
            )

    @staticmethod
    def _validate_bar_chart(
        computation_type: str,
        x_axis: Optional[str],
        y_axis: Optional[str],
        dimension_col: Optional[str],
        metrics: Optional[List] = None,
    ) -> None:
        """Validate bar chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError("Bar chart with raw data requires X-axis column")
            if not y_axis:
                raise ChartValidationError("Bar chart with raw data requires Y-axis column")

            # Ensure aggregated fields are not set for raw data
            if dimension_col or metrics:
                raise ChartValidationError(
                    "Bar chart with raw data should not have dimension column or metrics"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Bar chart with aggregated data requires dimension column"
                )

            # Check for multiple metrics (new approach) or single metric (legacy approach)
            if metrics and len(metrics) > 0:
                # Multiple metrics approach - validate each metric
                for i, metric in enumerate(metrics):
                    if not isinstance(metric, dict):
                        raise ChartValidationError(f"Metric {i+1} must be a dictionary")

                    metric_agg = metric.get("aggregation")
                    metric_col = metric.get("column")

                    if not metric_agg:
                        raise ChartValidationError(f"Metric {i+1} requires aggregation function")

                    ChartValidator._validate_aggregate_function(metric_agg)

                    # Validate column requirement (not needed for count)
                    if not metric_col and metric_agg != "count":
                        raise ChartValidationError(
                            f"Metric {i+1} requires column except for count function"
                        )

            else:
                # No metrics provided - require metrics for aggregated bar charts
                raise ChartValidationError(
                    "Bar chart with aggregated data requires at least one metric"
                )

            # Ensure raw fields are not set for aggregated data
            if x_axis or y_axis:
                raise ChartValidationError(
                    "Bar chart with aggregated data should not have X-axis or Y-axis columns"
                )

    @staticmethod
    def _validate_pie_chart(
        computation_type: str,
        x_axis: Optional[str],
        y_axis: Optional[str],
        dimension_col: Optional[str],
        metrics: Optional[List] = None,
    ) -> None:
        """Validate pie chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError(
                    "Pie chart with raw data requires X-axis column (for grouping)"
                )

            # Pie charts with raw data don't need y-axis or metrics
            if dimension_col or metrics:
                raise ChartValidationError(
                    "Pie chart with raw data should not have dimension column or metrics"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Pie chart with aggregated data requires dimension column"
                )

            # Validate metrics - pie charts need exactly one metric
            if not metrics or len(metrics) == 0:
                raise ChartValidationError(
                    "Pie chart with aggregated data requires exactly one metric"
                )

            if len(metrics) > 1:
                raise ChartValidationError(
                    "Pie charts support only one metric. Multiple metrics are not allowed."
                )

            # Validate the first metric (pie charts use first metric only)
            metric = metrics[0]
            if not isinstance(metric, dict):
                raise ChartValidationError("Metric must be a dictionary")

            metric_agg = metric.get("aggregation")
            metric_col = metric.get("column")

            if not metric_agg:
                raise ChartValidationError("Metric requires aggregation function")

            ChartValidator._validate_aggregate_function(metric_agg)

            # Validate column requirement (not needed for count)
            if not metric_col and metric_agg != "count":
                raise ChartValidationError("Metric requires column except for count function")

            # Ensure raw fields are not set for aggregated data
            if x_axis or y_axis:
                raise ChartValidationError(
                    "Pie chart with aggregated data should not have X-axis or Y-axis columns"
                )

    @staticmethod
    def _validate_line_chart(
        computation_type: str,
        x_axis: Optional[str],
        y_axis: Optional[str],
        dimension_col: Optional[str],
        metrics: Optional[List] = None,
    ) -> None:
        """Validate line chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError("Line chart with raw data requires X-axis column")
            if not y_axis:
                raise ChartValidationError("Line chart with raw data requires Y-axis column")

            # Ensure aggregated fields are not set for raw data
            if dimension_col or metrics:
                raise ChartValidationError(
                    "Line chart with raw data should not have dimension column or metrics"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Line chart with aggregated data requires dimension column"
                )

            # Check for multiple metrics (new approach) or single metric (legacy approach)
            if metrics and len(metrics) > 0:
                # Multiple metrics approach - validate each metric
                for i, metric in enumerate(metrics):
                    if not isinstance(metric, dict):
                        raise ChartValidationError(f"Metric {i+1} must be a dictionary")

                    metric_agg = metric.get("aggregation")
                    metric_col = metric.get("column")

                    if not metric_agg:
                        raise ChartValidationError(f"Metric {i+1} requires aggregation function")

                    ChartValidator._validate_aggregate_function(metric_agg)

                    # Validate column requirement (not needed for count)
                    if not metric_col and metric_agg != "count":
                        raise ChartValidationError(
                            f"Metric {i+1} requires column except for count function"
                        )

            else:
                # No metrics provided - require metrics for aggregated line charts
                raise ChartValidationError(
                    "Line chart with aggregated data requires at least one metric"
                )

            # Ensure raw fields are not set for aggregated data
            if x_axis or y_axis:
                raise ChartValidationError(
                    "Line chart with aggregated data should not have X-axis or Y-axis columns"
                )

    @staticmethod
    def _validate_number_chart(
        computation_type: str,
        metrics: Optional[List] = None,
        customizations: Dict = None,
    ) -> None:
        """Validate number chart configuration"""
        # Number charts only support aggregated data
        if computation_type != "aggregated":
            raise ChartValidationError("Number charts only support aggregated data")

        # Validate metrics - number charts need exactly one metric
        if not metrics or len(metrics) == 0:
            raise ChartValidationError("Number chart requires exactly one metric")

        if len(metrics) > 1:
            raise ChartValidationError(
                "Number charts support only one metric. Multiple metrics are not allowed."
            )

        # Validate the first metric (number charts use first metric only)
        metric = metrics[0]
        if not isinstance(metric, dict):
            raise ChartValidationError("Metric must be a dictionary")

        metric_agg = metric.get("aggregation")
        metric_col = metric.get("column")

        if not metric_agg:
            raise ChartValidationError("Metric requires aggregation function")

        ChartValidator._validate_aggregate_function(metric_agg)

        # Validate column requirement (not needed for count)
        if not metric_col and metric_agg != "count":
            raise ChartValidationError("Metric requires column except for count function")

        # Validate customizations if present
        if customizations:
            number_format = customizations.get("numberFormat")
            if number_format and number_format not in [
                "default",
                "comma",
                "percentage",
                "currency",
            ]:
                raise ChartValidationError(
                    f"Invalid number format '{number_format}'. Must be one of: default, comma, percentage, currency"
                )

            decimal_places = customizations.get("decimalPlaces")
            if decimal_places is not None:
                try:
                    dp = int(decimal_places)
                    if dp < 0 or dp > 10:
                        raise ChartValidationError("Decimal places must be between 0 and 10")
                except (ValueError, TypeError):
                    raise ChartValidationError("Decimal places must be a valid number")

    @staticmethod
    def validate_for_update(
        existing_chart_type: str,
        new_chart_type: Optional[str],
        new_computation_type: Optional[str],
        extra_config: Dict,
        schema_name: str,
        table_name: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate chart configuration for updates

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Use existing chart type if new one is not provided
        chart_type = new_chart_type or existing_chart_type

        # For updates, we need to be more lenient as partial updates are allowed
        # But if chart type or computation type changes, full validation is needed
        if new_chart_type or new_computation_type:
            # Full validation needed
            computation_type = new_computation_type or extra_config.get("computation_type", "raw")
            return ChartValidator.validate_chart_config(
                chart_type, computation_type, extra_config, schema_name, table_name
            )

        # For partial updates, just validate what's provided
        try:
            # If aggregate function is updated, validate it
            if "aggregate_function" in extra_config:
                ChartValidator._validate_aggregate_function(extra_config["aggregate_function"])

            # Validate number format if updated
            customizations = extra_config.get("customizations", {})
            if "numberFormat" in customizations:
                number_format = customizations["numberFormat"]
                if number_format not in ["default", "comma", "percentage", "currency"]:
                    raise ChartValidationError(
                        f"Invalid number format '{number_format}'. Must be one of: default, comma, percentage, currency"
                    )

            return True, None

        except ChartValidationError as e:
            return False, str(e)

    @staticmethod
    def _validate_map_chart(
        computation_type: str,
        geographic_column: Optional[str],
        value_column: Optional[str],
        selected_geojson_id: Optional[int],
        metrics: Optional[List] = None,
        customizations: Dict = None,
    ) -> None:
        """Validate map chart configuration"""
        # Maps only support aggregated data
        if computation_type != "aggregated":
            raise ChartValidationError("Map charts only support aggregated data")

        if not geographic_column:
            raise ChartValidationError("Map chart requires geographic column")

        # Validate metrics - map charts need exactly one metric
        if not metrics or len(metrics) == 0:
            raise ChartValidationError("Map chart requires exactly one metric")

        if len(metrics) > 1:
            raise ChartValidationError(
                "Map charts support only one metric. Multiple metrics are not allowed."
            )

        # Validate the metric
        metric = metrics[0]
        if not isinstance(metric, dict):
            raise ChartValidationError("Metric must be a dictionary")

        metric_agg = metric.get("aggregation")
        metric_col = metric.get("column")

        if not metric_agg:
            raise ChartValidationError("Metric requires aggregation function")

        ChartValidator._validate_aggregate_function(metric_agg)

        # Validate column requirement (not needed for count)
        if not metric_col and metric_agg != "count":
            raise ChartValidationError("Metric requires column except for count function")

        if not selected_geojson_id:
            raise ChartValidationError("Map chart requires selected GeoJSON")

        # Validate customizations if present
        if customizations:
            color_scheme = customizations.get("colorScheme")
            if color_scheme and color_scheme not in [
                "Blues",
                "Reds",
                "Greens",
                "Purples",
                "Oranges",
                "Greys",
            ]:
                raise ChartValidationError(
                    f"Invalid color scheme '{color_scheme}'. Must be one of: Blues, Reds, Greens, Purples, Oranges, Greys"
                )

    @staticmethod
    def _validate_table_chart(
        computation_type: str,
        schema_name: str,
        table_name: str,
    ) -> None:
        """Validate table chart configuration"""
        # Tables support both raw and aggregated data
        # Only basic validation needed - schema and table are already validated in _validate_basic_fields
        # No additional column requirements for table charts
        pass
