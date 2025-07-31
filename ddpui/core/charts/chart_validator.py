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
    VALID_CHART_TYPES = ["bar", "pie", "line", "number", "map"]

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
            aggregate_col = extra_config.get("aggregate_column")
            aggregate_func = extra_config.get("aggregate_function")
            customizations = extra_config.get("customizations", {})

            # Validate based on chart type
            if chart_type == "bar":
                ChartValidator._validate_bar_chart(
                    computation_type, x_axis, y_axis, dimension_col, aggregate_col, aggregate_func
                )
            elif chart_type == "pie":
                ChartValidator._validate_pie_chart(
                    computation_type, x_axis, y_axis, dimension_col, aggregate_col, aggregate_func
                )
            elif chart_type == "line":
                ChartValidator._validate_line_chart(
                    computation_type, x_axis, y_axis, dimension_col, aggregate_col, aggregate_func
                )
            elif chart_type == "number":
                ChartValidator._validate_number_chart(
                    computation_type, aggregate_col, aggregate_func, customizations
                )
            elif chart_type == "map":
                # Map validation to be implemented later
                raise ChartValidationError("Map charts are not yet implemented")

            return True, None

        except ChartValidationError as e:
            logger.warning(f"Chart validation failed: {str(e)}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Unexpected error during chart validation: {str(e)}", exc_info=True)
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
        aggregate_col: Optional[str],
        aggregate_func: Optional[str],
    ) -> None:
        """Validate bar chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError("Bar chart with raw data requires X-axis column")
            if not y_axis:
                raise ChartValidationError("Bar chart with raw data requires Y-axis column")

            # Ensure aggregated fields are not set for raw data
            if dimension_col or aggregate_col or aggregate_func:
                raise ChartValidationError(
                    "Bar chart with raw data should not have dimension, aggregate column, or aggregate function"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Bar chart with aggregated data requires dimension column"
                )
            if not aggregate_col:
                raise ChartValidationError(
                    "Bar chart with aggregated data requires aggregate column"
                )
            if not aggregate_func:
                raise ChartValidationError(
                    "Bar chart with aggregated data requires aggregate function"
                )

            ChartValidator._validate_aggregate_function(aggregate_func)

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
        aggregate_col: Optional[str],
        aggregate_func: Optional[str],
    ) -> None:
        """Validate pie chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError(
                    "Pie chart with raw data requires X-axis column (for grouping)"
                )

            # Pie charts with raw data don't need y-axis
            if dimension_col or aggregate_col or aggregate_func:
                raise ChartValidationError(
                    "Pie chart with raw data should not have dimension, aggregate column, or aggregate function"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Pie chart with aggregated data requires dimension column"
                )
            if not aggregate_col:
                raise ChartValidationError(
                    "Pie chart with aggregated data requires aggregate column"
                )
            if not aggregate_func:
                raise ChartValidationError(
                    "Pie chart with aggregated data requires aggregate function"
                )

            ChartValidator._validate_aggregate_function(aggregate_func)

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
        aggregate_col: Optional[str],
        aggregate_func: Optional[str],
    ) -> None:
        """Validate line chart configuration"""
        if computation_type == "raw":
            if not x_axis:
                raise ChartValidationError("Line chart with raw data requires X-axis column")
            if not y_axis:
                raise ChartValidationError("Line chart with raw data requires Y-axis column")

            # Ensure aggregated fields are not set for raw data
            if dimension_col or aggregate_col or aggregate_func:
                raise ChartValidationError(
                    "Line chart with raw data should not have dimension, aggregate column, or aggregate function"
                )

        else:  # aggregated
            if not dimension_col:
                raise ChartValidationError(
                    "Line chart with aggregated data requires dimension column"
                )
            if not aggregate_col:
                raise ChartValidationError(
                    "Line chart with aggregated data requires aggregate column"
                )
            if not aggregate_func:
                raise ChartValidationError(
                    "Line chart with aggregated data requires aggregate function"
                )

            ChartValidator._validate_aggregate_function(aggregate_func)

            # Ensure raw fields are not set for aggregated data
            if x_axis or y_axis:
                raise ChartValidationError(
                    "Line chart with aggregated data should not have X-axis or Y-axis columns"
                )

    @staticmethod
    def _validate_number_chart(
        computation_type: str,
        aggregate_col: Optional[str],
        aggregate_func: Optional[str],
        customizations: Dict,
    ) -> None:
        """Validate number chart configuration"""
        # Number charts only support aggregated data
        if computation_type != "aggregated":
            raise ChartValidationError("Number charts only support aggregated data")

        if not aggregate_col:
            raise ChartValidationError("Number chart requires aggregate column")

        if not aggregate_func:
            raise ChartValidationError("Number chart requires aggregate function")

        ChartValidator._validate_aggregate_function(aggregate_func)

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
