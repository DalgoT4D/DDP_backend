"""Validation utilities for chart APIs"""
from typing import List, Optional
import re
from ninja.errors import HttpError


class ChartValidation:
    """Validation utilities for chart-related operations"""

    # SQL injection prevention patterns
    INVALID_SQL_PATTERNS = [
        r";\s*DROP",
        r";\s*DELETE",
        r";\s*UPDATE",
        r";\s*INSERT",
        r";\s*ALTER",
        r";\s*CREATE",
        r";\s*TRUNCATE",
        r"--",
        r"/\*.*\*/",
        r"UNION\s+SELECT",
        r"OR\s+1\s*=\s*1",
        r"AND\s+1\s*=\s*1",
    ]

    # Valid identifier pattern (alphanumeric, underscore, no special chars)
    VALID_IDENTIFIER_PATTERN = r"^[a-zA-Z][a-zA-Z0-9_]*$"

    # Maximum lengths
    MAX_TITLE_LENGTH = 255
    MAX_DESCRIPTION_LENGTH = 1000
    MAX_SCHEMA_NAME_LENGTH = 128
    MAX_TABLE_NAME_LENGTH = 128
    MAX_COLUMN_NAME_LENGTH = 128

    # Valid chart types
    VALID_CHART_TYPES = ["bar", "pie", "line"]

    # Valid computation types
    VALID_COMPUTATION_TYPES = ["raw", "aggregated"]

    # Valid aggregation functions
    VALID_AGGREGATE_FUNCTIONS = ["sum", "avg", "count", "min", "max"]

    @classmethod
    def validate_identifier(cls, name: str, field_name: str) -> str:
        """Validate SQL identifier (schema, table, column names)"""
        if not name:
            raise HttpError(400, f"{field_name} cannot be empty")

        # Check length
        max_length = {
            "schema_name": cls.MAX_SCHEMA_NAME_LENGTH,
            "table_name": cls.MAX_TABLE_NAME_LENGTH,
            "column_name": cls.MAX_COLUMN_NAME_LENGTH,
        }.get(field_name, cls.MAX_COLUMN_NAME_LENGTH)

        if len(name) > max_length:
            raise HttpError(400, f"{field_name} exceeds maximum length of {max_length}")

        # Check for SQL injection patterns
        for pattern in cls.INVALID_SQL_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                raise HttpError(400, f"Invalid characters in {field_name}")

        # Check valid identifier pattern
        if not re.match(cls.VALID_IDENTIFIER_PATTERN, name):
            raise HttpError(
                400,
                f"{field_name} must start with a letter and contain only letters, numbers, and underscores",
            )

        return name

    @classmethod
    def validate_chart_type(cls, chart_type: str) -> str:
        """Validate chart type"""
        if chart_type not in cls.VALID_CHART_TYPES:
            raise HttpError(
                400, f"Invalid chart type. Must be one of: {', '.join(cls.VALID_CHART_TYPES)}"
            )
        return chart_type

    @classmethod
    def validate_computation_type(cls, computation_type: str) -> str:
        """Validate computation type"""
        if computation_type not in cls.VALID_COMPUTATION_TYPES:
            raise HttpError(
                400,
                f"Invalid computation type. Must be one of: {', '.join(cls.VALID_COMPUTATION_TYPES)}",
            )
        return computation_type

    @classmethod
    def validate_aggregate_function(cls, func: str) -> str:
        """Validate aggregation function"""
        if func not in cls.VALID_AGGREGATE_FUNCTIONS:
            raise HttpError(
                400,
                f"Invalid aggregation function. Must be one of: {', '.join(cls.VALID_AGGREGATE_FUNCTIONS)}",
            )
        return func

    @classmethod
    def validate_title(cls, title: str) -> str:
        """Validate chart title"""
        if not title or not title.strip():
            raise HttpError(400, "Title cannot be empty")

        title = title.strip()
        if len(title) > cls.MAX_TITLE_LENGTH:
            raise HttpError(400, f"Title exceeds maximum length of {cls.MAX_TITLE_LENGTH}")

        return title

    @classmethod
    def validate_description(cls, description: Optional[str]) -> Optional[str]:
        """Validate chart description"""
        if description is None:
            return None

        description = description.strip()
        if len(description) > cls.MAX_DESCRIPTION_LENGTH:
            raise HttpError(
                400, f"Description exceeds maximum length of {cls.MAX_DESCRIPTION_LENGTH}"
            )

        return description if description else None

    @classmethod
    def validate_pagination(cls, offset: int, limit: int) -> tuple[int, int]:
        """Validate pagination parameters"""
        if offset < 0:
            raise HttpError(400, "Offset must be non-negative")

        if limit <= 0:
            raise HttpError(400, "Limit must be positive")

        # Cap limit to prevent excessive data retrieval
        MAX_LIMIT = 10000
        if limit > MAX_LIMIT:
            raise HttpError(400, f"Limit cannot exceed {MAX_LIMIT}")

        return offset, limit

    @classmethod
    def validate_chart_data_payload(cls, payload) -> None:
        """Validate complete chart data payload"""
        # Validate chart and computation types
        cls.validate_chart_type(payload.chart_type)
        cls.validate_computation_type(payload.computation_type)

        # Validate schema and table names
        cls.validate_identifier(payload.schema_name, "schema_name")
        cls.validate_identifier(payload.table_name, "table_name")

        # Validate pagination
        cls.validate_pagination(payload.offset, payload.limit)

        # Validate based on computation type
        if payload.computation_type == "raw":
            # For raw data, need at least one axis
            if not payload.x_axis and not payload.y_axis:
                raise HttpError(
                    400, "At least one axis (x_axis or y_axis) must be specified for raw data"
                )

            # Validate column names if provided
            if payload.x_axis:
                cls.validate_identifier(payload.x_axis, "column_name")
            if payload.y_axis:
                cls.validate_identifier(payload.y_axis, "column_name")
            if payload.extra_dimension:
                cls.validate_identifier(payload.extra_dimension, "column_name")

        else:  # aggregated
            # For aggregated data, all fields are required
            if not payload.dimension_col:
                raise HttpError(400, "dimension_col is required for aggregated data")
            if not payload.aggregate_col:
                raise HttpError(400, "aggregate_col is required for aggregated data")
            if not payload.aggregate_func:
                raise HttpError(400, "aggregate_func is required for aggregated data")

            # Validate fields
            cls.validate_identifier(payload.dimension_col, "column_name")
            cls.validate_identifier(payload.aggregate_col, "column_name")
            cls.validate_aggregate_function(payload.aggregate_func)

            if payload.extra_dimension:
                cls.validate_identifier(payload.extra_dimension, "column_name")

    @classmethod
    def validate_customizations(cls, customizations: dict) -> dict:
        """Validate chart customizations"""
        if not isinstance(customizations, dict):
            raise HttpError(400, "Customizations must be a dictionary")

        # Define allowed customization keys and their types
        ALLOWED_KEYS = {
            # Bar chart customizations
            "orientation": str,
            "stacked": bool,
            "showDataLabels": bool,
            "xAxisTitle": str,
            "yAxisTitle": str,
            # Pie chart customizations
            "donut": bool,
            # Line chart customizations
            "smooth": bool,
            # Common customizations
            "showLegend": bool,
            "legendPosition": str,
            "colorScheme": str,
        }

        # Validate each customization
        validated = {}
        for key, value in customizations.items():
            if key not in ALLOWED_KEYS:
                # Skip unknown keys instead of failing
                continue

            expected_type = ALLOWED_KEYS[key]
            if not isinstance(value, expected_type):
                raise HttpError(
                    400, f"Customization '{key}' must be of type {expected_type.__name__}"
                )

            # Additional validation for specific fields
            if key in ["xAxisTitle", "yAxisTitle"] and len(str(value)) > 100:
                raise HttpError(400, f"{key} exceeds maximum length of 100")

            if key == "legendPosition" and value not in ["top", "bottom", "left", "right"]:
                raise HttpError(400, "legendPosition must be one of: top, bottom, left, right")

            if key == "orientation" and value not in ["horizontal", "vertical"]:
                raise HttpError(400, "orientation must be one of: horizontal, vertical")

            validated[key] = value

        return validated
