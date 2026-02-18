"""Chart validation module for validating chart configurations"""

from typing import Dict, List, Optional, Tuple
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.charts.validator")


class ChartValidationError(Exception):
    """Custom exception for chart validation errors"""

    pass


class ChartValidator:
    """Validates chart configurations based on chart type"""

    # Valid chart types
    VALID_CHART_TYPES = ["bar", "pie", "line", "number", "map", "table"]

    # Valid aggregate functions
    VALID_AGGREGATE_FUNCTIONS = ["sum", "avg", "count", "min", "max", "count_distinct"]

    @staticmethod
    def validate_chart_config(
        chart_type: str,
        extra_config: Dict,
        schema_name: str,
        table_name: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate chart configuration based on chart type

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Basic validation
            ChartValidator._validate_basic_fields(chart_type, schema_name, table_name)

            # Extract column configurations from extra_config
            dimension_col = extra_config.get("dimension_column")
            customizations = extra_config.get("customizations", {})

            # Extract metrics
            metrics = extra_config.get("metrics", [])

            # Validate based on chart type
            if chart_type == "bar":
                ChartValidator._validate_bar_chart(dimension_col, metrics)
            elif chart_type == "pie":
                ChartValidator._validate_pie_chart(dimension_col, metrics)
            elif chart_type == "line":
                ChartValidator._validate_line_chart(dimension_col, metrics)
            elif chart_type == "number":
                ChartValidator._validate_number_chart(metrics, customizations)
            elif chart_type == "map":
                ChartValidator._validate_map_chart(
                    extra_config.get("geographic_column"),
                    extra_config.get("selected_geojson_id"),
                    metrics,
                    customizations,
                )
            elif chart_type == "table":
                ChartValidator._validate_table_chart(schema_name, table_name)

            return True, None

        except ChartValidationError as e:
            logger.warning(f"Chart validation failed: {str(e)}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Unexpected error during chart validation: {str(e)}")
            return False, f"Validation error: {str(e)}"

    @staticmethod
    def _validate_basic_fields(chart_type: str, schema_name: str, table_name: str) -> None:
        """Validate basic required fields"""
        if not chart_type:
            raise ChartValidationError("Chart type is required")

        if chart_type not in ChartValidator.VALID_CHART_TYPES:
            raise ChartValidationError(
                f"Invalid chart type '{chart_type}'. Must be one of: {', '.join(ChartValidator.VALID_CHART_TYPES)}"
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
    def _validate_metrics(metrics: List, chart_type: str, allow_multiple: bool = True) -> None:
        """Validate metrics configuration"""
        if not metrics or len(metrics) == 0:
            raise ChartValidationError(
                f"{chart_type.capitalize()} chart requires at least one metric"
            )

        if not allow_multiple and len(metrics) > 1:
            raise ChartValidationError(
                f"{chart_type.capitalize()} charts support only one metric. Multiple metrics are not allowed."
            )

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

    @staticmethod
    def _validate_bar_chart(
        dimension_col: Optional[str],
        metrics: List,
    ) -> None:
        """Validate bar chart configuration"""
        if not dimension_col:
            raise ChartValidationError("Bar chart requires dimension column")

        ChartValidator._validate_metrics(metrics, "bar", allow_multiple=True)

    @staticmethod
    def _validate_pie_chart(
        dimension_col: Optional[str],
        metrics: List,
    ) -> None:
        """Validate pie chart configuration"""
        if not dimension_col:
            raise ChartValidationError("Pie chart requires dimension column")

        ChartValidator._validate_metrics(metrics, "pie", allow_multiple=False)

    @staticmethod
    def _validate_line_chart(
        dimension_col: Optional[str],
        metrics: List,
    ) -> None:
        """Validate line chart configuration"""
        if not dimension_col:
            raise ChartValidationError("Line chart requires dimension column")

        ChartValidator._validate_metrics(metrics, "line", allow_multiple=True)

    @staticmethod
    def _validate_number_chart(
        metrics: List,
        customizations: Dict = None,
    ) -> None:
        """Validate number chart configuration"""
        ChartValidator._validate_metrics(metrics, "number", allow_multiple=False)

        # Validate customizations if present
        if customizations:
            number_format = customizations.get("numberFormat")
            if number_format and number_format not in [
                "default",
                "comma",
                "percentage",
                "currency",
                "indian",
                "international",
                "adaptive_international",
                "adaptive_indian",
            ]:
                raise ChartValidationError(
                    f"Invalid number format '{number_format}'. Must be one of: default, comma, percentage, currency,indian, international"
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
        # But if chart type changes, full validation is needed
        if new_chart_type:
            return ChartValidator.validate_chart_config(
                chart_type, extra_config, schema_name, table_name
            )

        # For partial updates, just validate what's provided
        try:
            # Validate number format if updated
            customizations = extra_config.get("customizations", {})
            if "numberFormat" in customizations:
                number_format = customizations["numberFormat"]
                if number_format not in ["default", "comma", "percentage", "currency", "indian", "international", "adaptive_international", "adaptive_indian"]:
                    raise ChartValidationError(
                        f"Invalid number format '{number_format}'. Must be one of: default, comma, percentage, currency, indian, international"
                    )

            return True, None

        except ChartValidationError as e:
            return False, str(e)

    @staticmethod
    def _validate_map_chart(
        geographic_column: Optional[str],
        selected_geojson_id: Optional[int],
        metrics: List,
        customizations: Dict = None,
    ) -> None:
        """Validate map chart configuration"""
        if not geographic_column:
            raise ChartValidationError("Map chart requires geographic column")

        ChartValidator._validate_metrics(metrics, "map", allow_multiple=False)

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
        schema_name: str,
        table_name: str,
    ) -> None:
        """Validate table chart configuration"""
        # Tables only need schema and table - already validated in _validate_basic_fields
        pass
