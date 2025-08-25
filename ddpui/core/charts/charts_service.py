"""Chart service module for handling chart business logic"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from decimal import Decimal
import hashlib
import json

from django.utils import timezone
from sqlalchemy import column, func, and_, or_, text
from sqlalchemy.dialects import postgresql

from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.core.dbtautomation_service import map_airbyte_keys_to_postgres_keys
from ddpui.schemas.chart_schema import (
    ChartDataPayload,
    ChartDataResponse,
    DataPreviewResponse,
    ExecuteChartQuery,
    TransformDataForChart,
)


def get_pagination_params(payload: ChartDataPayload):
    """
    Extract pagination parameters from payload.
    Returns (limit, offset) tuple with proper defaults.
    """
    # Check if pagination is enabled in extra_config
    if (
        payload.extra_config
        and payload.extra_config.get("pagination")
        and payload.extra_config["pagination"].get("enabled")
    ):
        page_size = payload.extra_config["pagination"].get("page_size", 50)
        # For preview/build, always start from offset 0
        return page_size, 0
    else:
        # Default pagination when not explicitly set
        # Use old payload.limit/offset if available (backward compatibility)
        limit = getattr(payload, "limit", 100)
        offset = getattr(payload, "offset", 0)
        return limit, offset


logger = CustomLogger("ddpui.charts")

# Global configuration for null value handling
NULL_VALUE_LABEL = "Unknown"


def handle_null_value(value: Any, null_label: Optional[str] = None) -> Any:
    """Convert None/null values to configured label for display purposes

    Args:
        value: The value to check for None
        null_label: Optional custom label to use instead of default

    Returns:
        The original value if not None, otherwise the null label
    """
    if value is None:
        return null_label or NULL_VALUE_LABEL
    return value


def safe_get_value(row: Dict[str, Any], key: str, null_label: Optional[str] = None) -> Any:
    """Safely get value from dict with null handling

    Args:
        row: Dictionary to get value from
        key: Key to look up
        null_label: Optional custom label for null values

    Returns:
        The value with null handling applied
    """
    value = row.get(key)
    return handle_null_value(value, null_label)


def get_warehouse_client(org_warehouse: OrgWarehouse) -> Warehouse:
    """Get warehouse client using the standard method"""
    return WarehouseFactory.get_warehouse_client(org_warehouse)


def get_aggregate_column_name(aggregate_func: str, aggregate_col: str) -> str:
    """Get the correct aggregate column name for data retrieval"""
    if aggregate_func and aggregate_func.lower() == "count" and aggregate_col is None:
        return "count_all"
    if aggregate_func:
        return f"{aggregate_func}_{aggregate_col}"
    return "unknown_metric"


def get_aggregate_display_name(aggregate_func: str, aggregate_col: str) -> str:
    """Get the display name for aggregate columns in chart legends"""
    if aggregate_func and aggregate_func.lower() == "count" and aggregate_col is None:
        return "Total Count"
    if aggregate_func:
        return f"{aggregate_func}({aggregate_col})"
    return "Unknown Metric"


def convert_value(value: Any, preserve_none: bool = False) -> Any:
    """Convert values to JSON-serializable format

    Args:
        value: The value to convert
        preserve_none: If True, return None as-is; if False, convert to NULL_VALUE_LABEL

    Returns:
        JSON-serializable value
    """
    if value is None:
        return None if preserve_none else NULL_VALUE_LABEL
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    return value


def build_multi_metric_query(
    payload: ChartDataPayload,
    query_builder: AggQueryBuilder,
) -> AggQueryBuilder:
    """Build query for multiple metrics on bar/line/pie/table/map charts"""
    if not payload.dimension_col:
        raise ValueError("dimension_col is required for multiple metrics charts")

    if not payload.metrics or len(payload.metrics) == 0:
        raise ValueError("At least one metric is required for multiple metrics charts")

    # Add dimension column
    query_builder.add_column(column(payload.dimension_col))

    # Add all metrics as aggregate columns
    for metric in payload.metrics:
        if not metric.aggregation:
            raise ValueError(f"Aggregation function is required for metric")

        # Handle count with None column case
        if metric.aggregation.lower() == "count" and metric.column is None:
            alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
        else:
            if not metric.column:
                raise ValueError(f"Column is required for {metric.aggregation} aggregation")
            alias = metric.alias or f"{metric.aggregation}_{metric.column}"

        query_builder.add_aggregate_column(
            metric.column,
            metric.aggregation,
            alias,
        )

    # Group by dimension column
    query_builder.group_cols_by(payload.dimension_col)

    # Add extra dimension if specified
    if payload.extra_dimension:
        query_builder.add_column(column(payload.extra_dimension))
        query_builder.group_cols_by(payload.extra_dimension)

    return query_builder


def build_chart_query(
    payload: ChartDataPayload,
) -> AggQueryBuilder:
    """Build query using unified AggQueryBuilder for both raw and aggregated queries"""
    query_builder = AggQueryBuilder()
    query_builder.fetch_from(payload.table_name, payload.schema_name)

    if payload.computation_type == "raw":
        if payload.x_axis:
            query_builder.add_column(column(payload.x_axis))
        if payload.y_axis:
            query_builder.add_column(column(payload.y_axis))
        if payload.extra_dimension:
            query_builder.add_column(column(payload.extra_dimension))

        # Validate that at least one column is specified
        if not payload.x_axis and not payload.y_axis:
            raise ValueError(
                "At least one column (x_axis or y_axis) must be specified for raw data"
            )

    else:  # aggregated
        # Check if multiple metrics are provided for bar/line/pie/table/map charts
        if payload.metrics and payload.chart_type in ["bar", "line", "pie", "table", "map"]:
            # Use multiple metrics approach
            query_builder = build_multi_metric_query(payload, query_builder)

            # Apply chart-level filters, sorting, and pagination (same as single metric approach)

            # Apply dashboard filters if provided
            if payload.dashboard_filters:
                query_builder = apply_dashboard_filters(query_builder, payload.dashboard_filters)

            # Apply chart-level filters if provided
            if payload.extra_config and payload.extra_config.get("filters"):
                query_builder = apply_chart_filters(query_builder, payload.extra_config["filters"])

            # Apply chart-level sorting if provided
            if payload.extra_config and payload.extra_config.get("sort"):
                query_builder = apply_chart_sorting(
                    query_builder, payload.extra_config["sort"], payload
                )

            # Apply pagination
            limit, offset = get_pagination_params(payload)
            query_builder.limit_rows(limit)
            query_builder.offset_rows(offset)

            return query_builder

        # For number charts, we don't need dimension columns
        if payload.chart_type == "number":
            # Handle count with None column case - use "count_all" alias
            if (
                payload.aggregate_func
                and payload.aggregate_func.lower() == "count"
                and payload.aggregate_col is None
            ):
                alias = "count_all"
            elif payload.aggregate_func:
                alias = f"{payload.aggregate_func}_{payload.aggregate_col}"
            else:
                # This shouldn't happen with proper validation, but handle gracefully
                alias = "unknown_metric"

            # Just add the aggregate column without any grouping
            query_builder.add_aggregate_column(
                payload.aggregate_col,
                payload.aggregate_func,
                alias,
            )
        else:
            # Add dimension column for other chart types
            query_builder.add_column(column(payload.dimension_col))

            # Handle count with None column case - use "count_all" alias
            if (
                payload.aggregate_func
                and payload.aggregate_func.lower() == "count"
                and payload.aggregate_col is None
            ):
                alias = "count_all"
            elif payload.aggregate_func:
                alias = f"{payload.aggregate_func}_{payload.aggregate_col}"
            else:
                # This shouldn't happen with proper validation, but handle gracefully
                alias = "unknown_metric"

            # Add aggregate column
            query_builder.add_aggregate_column(
                payload.aggregate_col,
                payload.aggregate_func,
                alias,
            )

            # Group by dimension column
            query_builder.group_cols_by(payload.dimension_col)

            # Add extra dimension if specified
            if payload.extra_dimension:
                query_builder.add_column(column(payload.extra_dimension))
                query_builder.group_cols_by(payload.extra_dimension)

    # Apply dashboard filters if provided
    if payload.dashboard_filters:
        query_builder = apply_dashboard_filters(query_builder, payload.dashboard_filters)

    # Apply chart-level filters if provided
    if payload.extra_config and payload.extra_config.get("filters"):
        query_builder = apply_chart_filters(query_builder, payload.extra_config["filters"])

    # Apply chart-level sorting if provided
    if payload.extra_config and payload.extra_config.get("sort"):
        query_builder = apply_chart_sorting(query_builder, payload.extra_config["sort"], payload)

    # Apply pagination
    limit, offset = get_pagination_params(payload)
    query_builder.limit_rows(limit)
    query_builder.offset_rows(offset)

    return query_builder


def apply_dashboard_filters(
    query_builder: AggQueryBuilder, filters: List[Dict[str, Any]]
) -> AggQueryBuilder:
    """Apply dashboard filters to the query builder using WHERE clauses

    Args:
        query_builder: The AggQueryBuilder instance to modify
        filters: List of resolved filter dictionaries with format:
                {
                    'filter_id': str,
                    'column': str,
                    'type': str ('value', 'numerical', or 'datetime'),
                    'value': Any,
                    'settings': dict
                }

    Returns:
        Modified query builder with applied filters
    """
    if not filters:
        return query_builder

    for filter_config in filters:
        column_name = filter_config["column"]
        filter_type = filter_config["type"]
        value = filter_config["value"]

        if value is None:
            continue

        if filter_type == "value":
            if isinstance(value, list):
                # Multiple values - use IN clause
                if len(value) > 0:
                    # Convert list values to proper SQL format
                    query_builder.where_clause(column(column_name).in_(value))
            else:
                # Single value - use equality
                query_builder.where_clause(column(column_name) == value)

        elif filter_type == "numerical":
            if isinstance(value, dict):
                # Range filter
                if "min" in value and "max" in value:
                    query_builder.where_clause(
                        and_(
                            column(column_name) >= value["min"], column(column_name) <= value["max"]
                        )
                    )
            else:
                # Single numerical value
                query_builder.where_clause(column(column_name) == value)

        elif filter_type == "datetime":
            # NEW: Handle datetime filters
            if isinstance(value, dict):
                # Date range filter
                if "start_date" in value and value["start_date"]:
                    query_builder.where_clause(column(column_name) >= value["start_date"])
                if "end_date" in value and value["end_date"]:
                    # Add 1 day to end_date to include the full day
                    end_date_inclusive = value["end_date"] + "T23:59:59"
                    query_builder.where_clause(column(column_name) <= end_date_inclusive)
            elif isinstance(value, str):
                # Single date - filter for that specific day
                start_of_day = value + "T00:00:00"
                end_of_day = value + "T23:59:59"
                query_builder.where_clause(
                    and_(column(column_name) >= start_of_day, column(column_name) <= end_of_day)
                )

    return query_builder


def apply_chart_filters(
    query_builder: AggQueryBuilder, filters: List[Dict[str, Any]]
) -> AggQueryBuilder:
    """Apply chart-level filters to the query builder using WHERE clauses

    Groups filters by column+operator combination and uses OR logic for same combinations
    to handle cases like: state_name = 'A' OR state_name = 'B'

    Args:
        query_builder: The AggQueryBuilder instance to modify
        filters: List of chart filter dictionaries with format:
                {
                    'column': str,
                    'operator': str,
                    'value': Any,
                    'data_type': str (optional)
                }

    Returns:
        Modified query builder with applied filters
    """
    if not filters:
        return query_builder

    from collections import defaultdict

    # Group filters by column+operator combination
    grouped_filters = defaultdict(list)
    single_filters = []

    for filter_config in filters:
        column_name = filter_config["column"]
        operator = filter_config["operator"]
        value = filter_config["value"]

        if not column_name or operator is None:
            continue

        # Operators that can be grouped (multiple values with OR)
        if operator in ["equals", "not_equals"]:
            grouped_filters[(column_name, operator)].append(value)
        else:
            # Other operators are applied individually
            single_filters.append(filter_config)

    # Apply grouped filters (multiple values with OR logic)
    for (column_name, operator), values in grouped_filters.items():
        if len(values) == 1:
            # Single value, apply normally
            value = values[0]
            if operator == "equals":
                query_builder.where_clause(column(column_name) == value)
            elif operator == "not_equals":
                query_builder.where_clause(column(column_name) != value)
        else:
            # Multiple values, use OR logic
            if operator == "equals":
                # state_name = 'A' OR state_name = 'B' OR state_name = 'C'
                or_conditions = [column(column_name) == value for value in values]
                query_builder.where_clause(or_(*or_conditions))
            elif operator == "not_equals":
                # state_name != 'A' AND state_name != 'B' AND state_name != 'C'
                and_conditions = [column(column_name) != value for value in values]
                query_builder.where_clause(and_(*and_conditions))

    # Apply single filters (non-groupable operators)
    for filter_config in single_filters:
        column_name = filter_config["column"]
        operator = filter_config["operator"]
        value = filter_config["value"]

        if operator == "greater_than":
            query_builder.where_clause(column(column_name) > value)
        elif operator == "less_than":
            query_builder.where_clause(column(column_name) < value)
        elif operator == "greater_than_equal":
            query_builder.where_clause(column(column_name) >= value)
        elif operator == "less_than_equal":
            query_builder.where_clause(column(column_name) <= value)
        elif operator == "like":
            query_builder.where_clause(column(column_name).like(f"%{value}%"))
        elif operator == "like_case_insensitive":
            query_builder.where_clause(
                func.lower(column(column_name)).like(f"%{str(value).lower()}%")
            )
        elif operator == "contains":  # Keep for backward compatibility
            query_builder.where_clause(column(column_name).like(f"%{value}%"))
        elif operator == "not_contains":  # Keep for backward compatibility
            query_builder.where_clause(~column(column_name).like(f"%{value}%"))
        elif operator == "in":
            # Convert comma-separated string to list
            if isinstance(value, str):
                values = [v.strip() for v in value.split(",") if v.strip()]
            else:
                values = value if isinstance(value, list) else [value]
            if values:
                query_builder.where_clause(column(column_name).in_(values))
        elif operator == "not_in":
            # Convert comma-separated string to list
            if isinstance(value, str):
                values = [v.strip() for v in value.split(",") if v.strip()]
            else:
                values = value if isinstance(value, list) else [value]
            if values:
                query_builder.where_clause(~column(column_name).in_(values))
        elif operator == "is_null":
            query_builder.where_clause(column(column_name).is_(None))
        elif operator == "is_not_null":
            query_builder.where_clause(column(column_name).isnot(None))

    return query_builder


def apply_chart_sorting(
    query_builder: AggQueryBuilder, sort_config: List[Dict[str, Any]], payload=None
) -> AggQueryBuilder:
    """Apply chart-level sorting to the query builder

    Args:
        query_builder: The AggQueryBuilder instance to modify
        sort_config: List of sort dictionaries with format:
                    {
                        'column': str,
                        'direction': str ('asc' or 'desc')
                    }
        payload: The chart data payload for context about aggregate columns

    Returns:
        Modified query builder with applied sorting
    """
    if not sort_config:
        return query_builder

    # Prepare sort columns as list of tuples for order_cols_by method
    sort_cols = []
    for sort_item in sort_config:
        column_name = sort_item.get("column")
        direction = sort_item.get("direction", "asc")

        if not column_name:
            continue

        # For aggregated queries, if sorting by the aggregate column,
        # use the aggregated column alias instead of the raw column
        if payload and payload.computation_type == "aggregated":
            # Check if sorting by a metric column (multiple metrics approach)
            if payload.metrics and len(payload.metrics) > 0:
                # Find the first metric that matches the column name
                matching_metric = None
                for metric in payload.metrics:
                    if metric.column == column_name:
                        matching_metric = metric
                        break

                if matching_metric:
                    # Always use the alias for sorting aggregated data
                    sort_column = (
                        matching_metric.alias or f"{matching_metric.aggregation}_{column_name}"
                    )
                else:
                    # If no matching metric found, assume it's the dimension column
                    sort_column = column_name
            # Legacy single metric approach
            elif hasattr(payload, "aggregate_col") and column_name == payload.aggregate_col:
                # Use the aggregated column alias pattern
                aggregate_func = getattr(payload, "aggregate_func", "sum")
                sort_column = f"{aggregate_func}_{column_name}"
            else:
                sort_column = column_name
        else:
            sort_column = column_name

        sort_cols.append((sort_column, direction))

    if sort_cols:
        query_builder.order_cols_by(sort_cols)

    return query_builder


def execute_query(
    warehouse_client: Warehouse,
    query_builder: AggQueryBuilder,
    column_mapping: List[Tuple[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Execute query and convert results to dictionaries

    Args:
        warehouse_client: The warehouse client to execute queries
        query_builder: The query builder with the query to execute
        column_mapping: Optional list of (column_name, index) tuples for result mapping.
                       If not provided, results will be returned as tuples.

    Returns:
        List of dictionaries with query results
    """
    # Build and compile SQL
    sql_stmt = query_builder.build()
    compiled_stmt = sql_stmt.compile(
        bind=warehouse_client.engine, compile_kwargs={"literal_binds": True}
    )

    logger.info(f"Generated SQL: {compiled_stmt}")

    # Execute query
    results: list[dict] = warehouse_client.execute(compiled_stmt)

    logger.info(f"Query executed successfully, fetched {len(results)} rows")

    # Return raw results if no mapping provided
    return list(results)


def execute_chart_query(
    warehouse_client: Warehouse, query_builder: AggQueryBuilder, payload: ExecuteChartQuery
) -> List[Dict[str, Any]]:
    """Execute query and convert results to dictionaries for charts"""
    # Build column mapping based on computation type
    column_mapping = []

    if payload.computation_type == "raw":
        # For raw queries, columns are in the order they were added
        col_index = 0
        if payload.x_axis:
            column_mapping.append((payload.x_axis, col_index))
            col_index += 1
        if payload.y_axis:
            column_mapping.append((payload.y_axis, col_index))
            col_index += 1
        if payload.extra_dimension:
            column_mapping.append((payload.extra_dimension, col_index))

    else:  # aggregated
        col_index = 0
        column_mapping.append((payload.dimension_col, col_index))
        col_index += 1

        # Handle multiple metrics for bar/line charts
        if payload.metrics:
            for metric in payload.metrics:
                if metric.aggregation.lower() == "count" and metric.column is None:
                    alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                else:
                    alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                column_mapping.append((alias, col_index))
                col_index += 1
        else:
            # Handle single metric (legacy approach)
            if (
                payload.aggregate_func
                and payload.aggregate_func.lower() == "count"
                and payload.aggregate_col is None
            ):
                agg_col_name = "count_all"
            elif payload.aggregate_func:
                agg_col_name = f"{payload.aggregate_func}_{payload.aggregate_col}"
            else:
                agg_col_name = "unknown_metric"
            column_mapping.append((agg_col_name, col_index))
            col_index += 1

        if payload.extra_dimension:
            column_mapping.append((payload.extra_dimension, col_index))

    return execute_query(warehouse_client, query_builder, column_mapping)


def transform_data_for_chart(
    results: List[Dict[str, Any]],
    payload: TransformDataForChart,
) -> Dict[str, Any]:
    """Transform query results to chart-specific data format"""

    # Get custom null label from customizations if provided
    null_label = payload.customizations.get("nullValueLabel") if payload.customizations else None

    # Handle None values - pie charts only need x_axis for raw data
    if (
        payload.computation_type == "raw"
        and payload.chart_type != "pie"
        and (not payload.x_axis or not payload.y_axis)
    ):
        return {}
    elif payload.computation_type == "raw" and payload.chart_type == "pie" and not payload.x_axis:
        return {}

    if payload.chart_type == "bar":
        if payload.computation_type == "raw":
            return {
                "xAxisData": [
                    convert_value(safe_get_value(row, payload.x_axis, null_label))
                    for row in results
                ],
                "series": [
                    {
                        "name": payload.y_axis,
                        "data": [
                            convert_value(
                                safe_get_value(row, payload.y_axis, null_label), preserve_none=True
                            )
                            for row in results
                        ],
                    }
                ],
                "legend": [payload.y_axis],
            }
        else:  # aggregated
            # Handle multiple metrics for bar charts
            if payload.metrics:
                if payload.extra_dimension:
                    # Handle multiple metrics WITH extra dimension (grouped bars)
                    grouped_data = {}
                    x_values = set()

                    # First, collect all x values and group by extra dimension
                    for row in results:
                        dimension = handle_null_value(
                            safe_get_value(row, payload.extra_dimension, null_label), null_label
                        )
                        x_value = handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        x_values.add(x_value)

                        if dimension not in grouped_data:
                            grouped_data[dimension] = {}

                        # For multiple metrics, we need to handle each metric
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                                display_name = metric.alias or "Total Count"
                            else:
                                if metric.alias:
                                    alias = metric.alias
                                else:
                                    alias = f"{metric.aggregation}_{metric.column}"
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            # Create a unique key for this dimension-metric combination
                            metric_dimension_key = f"{dimension}_{display_name}"
                            if metric_dimension_key not in grouped_data:
                                grouped_data[metric_dimension_key] = {}

                            grouped_data[metric_dimension_key][x_value] = row.get(alias, 0)

                    x_axis_data = sorted(list(x_values))

                    # Create series for each dimension-metric combination
                    series_data = []
                    legend_data = []

                    # Group by extra dimension for better organization
                    dimensions = sorted(set([key.split("_")[0] for key in grouped_data.keys()]))

                    for dimension in dimensions:
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                display_name = metric.alias or "Total Count"
                            else:
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            metric_dimension_key = f"{dimension}_{display_name}"
                            if metric_dimension_key in grouped_data:
                                series_name = f"{dimension} - {display_name}"
                                series_data.append(
                                    {
                                        "name": series_name,
                                        "data": [
                                            grouped_data[metric_dimension_key].get(x, 0)
                                            for x in x_axis_data
                                        ],
                                    }
                                )
                                legend_data.append(series_name)

                    return {
                        "xAxisData": x_axis_data,
                        "series": series_data,
                        "legend": legend_data,
                    }
                else:
                    # Handle multiple metrics WITHOUT extra dimension (existing logic)
                    x_axis_data = [
                        handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        for row in results
                    ]

                    series_data = []
                    legend_data = []

                    for metric in payload.metrics:
                        if metric.aggregation.lower() == "count" and metric.column is None:
                            alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                            display_name = metric.alias or "Total Count"
                        else:
                            # The alias should match what was created in build_multi_metric_query
                            # If metric.alias exists, use it as the exact alias, otherwise generate default
                            if metric.alias:
                                alias = metric.alias
                            else:
                                alias = f"{metric.aggregation}_{metric.column}"
                            display_name = metric.alias or f"{metric.aggregation}({metric.column})"

                        metric_data = [row.get(alias, 0) for row in results]

                        series_data.append(
                            {
                                "name": display_name,
                                "data": metric_data,
                            }
                        )
                        legend_data.append(display_name)

                    return {
                        "xAxisData": x_axis_data,
                        "series": series_data,
                        "legend": legend_data,
                    }
            elif payload.extra_dimension:
                # Group by extra dimension
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = handle_null_value(
                        safe_get_value(row, payload.extra_dimension, null_label), null_label
                    )
                    x_value = handle_null_value(
                        safe_get_value(row, payload.dimension_col, null_label), null_label
                    )
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    agg_col_name = get_aggregate_column_name(
                        payload.aggregate_func, payload.aggregate_col
                    )
                    grouped_data[dimension][x_value] = row.get(agg_col_name, 0)

                x_axis_data = sorted(list(x_values))

                return {
                    "xAxisData": x_axis_data,
                    "series": [
                        {
                            "name": dimension,
                            "data": [grouped_data[dimension].get(x, 0) for x in x_axis_data],
                        }
                        for dimension in grouped_data.keys()
                    ],
                    "legend": list(grouped_data.keys()),
                }
            else:
                return {
                    "xAxisData": [
                        handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        for row in results
                    ],
                    "series": [
                        {
                            "name": get_aggregate_display_name(
                                payload.aggregate_func, payload.aggregate_col
                            ),
                            "data": [
                                row.get(
                                    get_aggregate_column_name(
                                        payload.aggregate_func, payload.aggregate_col
                                    ),
                                    0,
                                )
                                for row in results
                            ],
                        }
                    ],
                    "legend": [
                        get_aggregate_display_name(payload.aggregate_func, payload.aggregate_col)
                    ],
                }

    elif payload.chart_type == "pie":
        if payload.computation_type == "raw":
            # For raw data, count occurrences
            value_counts = {}
            for row in results:
                key = handle_null_value(safe_get_value(row, payload.x_axis, null_label), null_label)
                value_counts[key] = value_counts.get(key, 0) + 1

            return {
                "pieData": [{"value": count, "name": name} for name, count in value_counts.items()],
                "seriesName": payload.x_axis,
            }
        else:  # aggregated
            # Handle multiple metrics for pie charts
            if payload.metrics:
                if payload.extra_dimension:
                    # For pie charts with extra dimension, create separate slices for each metric-dimension-x combination
                    pie_data = []
                    for row in results:
                        dimension = handle_null_value(
                            safe_get_value(row, payload.extra_dimension, null_label), null_label
                        )
                        x_value = handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )

                        # Create separate pie slices for each metric (same pattern as bar/line charts)
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                                display_name = metric.alias or "Total Count"
                            else:
                                alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            metric_value = row.get(alias, 0)
                            slice_name = (
                                f"{dimension} - {display_name}"  # Same pattern as bar/line charts
                            )

                            pie_data.append({"value": metric_value, "name": slice_name})

                    return {
                        "pieData": pie_data,
                        "seriesName": "Metrics by Dimension",
                    }
                else:
                    # Handle multiple metrics WITHOUT extra dimension
                    # Create separate slices for each metric-dimension combination
                    pie_data = []
                    for row in results:
                        x_value = handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )

                        # Create separate pie slices for each metric (same pattern as bar/line charts)
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                                display_name = metric.alias or "Total Count"
                            else:
                                alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            metric_value = row.get(alias, 0)
                            slice_name = (
                                f"{x_value} - {display_name}"  # Same pattern as bar/line charts
                            )

                            pie_data.append({"value": metric_value, "name": slice_name})

                    return {
                        "pieData": pie_data,
                        "seriesName": "Metrics by Dimension",
                    }
            else:
                # Handle single metric (existing logic)
                return {
                    "pieData": [
                        {
                            "value": row.get(
                                get_aggregate_column_name(
                                    payload.aggregate_func, payload.aggregate_col
                                ),
                                0,
                            ),
                            "name": handle_null_value(
                                safe_get_value(row, payload.dimension_col, null_label), null_label
                            ),
                        }
                        for row in results
                    ],
                    "seriesName": get_aggregate_display_name(
                        payload.aggregate_func, payload.aggregate_col
                    ),
                }

    elif payload.chart_type == "line":
        if payload.computation_type == "raw":
            return {
                "xAxisData": [
                    convert_value(safe_get_value(row, payload.x_axis, null_label))
                    for row in results
                ],
                "series": [
                    {
                        "name": payload.y_axis,
                        "data": [
                            convert_value(
                                safe_get_value(row, payload.y_axis, null_label), preserve_none=True
                            )
                            for row in results
                        ],
                    }
                ],
                "legend": [payload.y_axis],
            }
        else:  # aggregated
            # Handle multiple metrics for line charts
            if payload.metrics:
                if payload.extra_dimension:
                    # Handle multiple metrics WITH extra dimension (grouped lines)
                    grouped_data = {}
                    x_values = set()

                    # First, collect all x values and group by extra dimension
                    for row in results:
                        dimension = handle_null_value(
                            safe_get_value(row, payload.extra_dimension, null_label), null_label
                        )
                        x_value = handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        x_values.add(x_value)

                        if dimension not in grouped_data:
                            grouped_data[dimension] = {}

                        # For multiple metrics, we need to handle each metric
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                                display_name = metric.alias or "Total Count"
                            else:
                                if metric.alias:
                                    alias = metric.alias
                                else:
                                    alias = f"{metric.aggregation}_{metric.column}"
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            # Create a unique key for this dimension-metric combination
                            metric_dimension_key = f"{dimension}_{display_name}"
                            if metric_dimension_key not in grouped_data:
                                grouped_data[metric_dimension_key] = {}

                            grouped_data[metric_dimension_key][x_value] = row.get(alias, 0)

                    x_axis_data = sorted(list(x_values))

                    # Create series for each dimension-metric combination
                    series_data = []
                    legend_data = []

                    # Group by extra dimension for better organization
                    dimensions = sorted(set([key.split("_")[0] for key in grouped_data.keys()]))

                    for dimension in dimensions:
                        for metric in payload.metrics:
                            if metric.aggregation.lower() == "count" and metric.column is None:
                                display_name = metric.alias or "Total Count"
                            else:
                                display_name = (
                                    metric.alias or f"{metric.aggregation}({metric.column})"
                                )

                            metric_dimension_key = f"{dimension}_{display_name}"
                            if metric_dimension_key in grouped_data:
                                series_name = f"{dimension} - {display_name}"
                                series_data.append(
                                    {
                                        "name": series_name,
                                        "data": [
                                            grouped_data[metric_dimension_key].get(x, 0)
                                            for x in x_axis_data
                                        ],
                                    }
                                )
                                legend_data.append(series_name)

                    return {
                        "xAxisData": x_axis_data,
                        "series": series_data,
                        "legend": legend_data,
                    }
                else:
                    # Handle multiple metrics WITHOUT extra dimension (existing logic)
                    x_axis_data = [
                        handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        for row in results
                    ]

                    series_data = []
                    legend_data = []

                    for metric in payload.metrics:
                        if metric.aggregation.lower() == "count" and metric.column is None:
                            alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                            display_name = metric.alias or "Total Count"
                        else:
                            alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                            display_name = metric.alias or f"{metric.aggregation}({metric.column})"

                        series_data.append(
                            {
                                "name": display_name,
                                "data": [row.get(alias, 0) for row in results],
                            }
                        )
                        legend_data.append(display_name)

                    return {
                        "xAxisData": x_axis_data,
                        "series": series_data,
                        "legend": legend_data,
                    }
            elif payload.extra_dimension:
                # Similar to bar chart grouping
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = handle_null_value(
                        safe_get_value(row, payload.extra_dimension, null_label), null_label
                    )
                    x_value = handle_null_value(
                        safe_get_value(row, payload.dimension_col, null_label), null_label
                    )
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    agg_col_name = get_aggregate_column_name(
                        payload.aggregate_func, payload.aggregate_col
                    )
                    grouped_data[dimension][x_value] = row.get(agg_col_name, 0)

                x_axis_data = sorted(list(x_values))

                return {
                    "xAxisData": x_axis_data,
                    "series": [
                        {
                            "name": dimension,
                            "data": [grouped_data[dimension].get(x, 0) for x in x_axis_data],
                        }
                        for dimension in grouped_data.keys()
                    ],
                    "legend": list(grouped_data.keys()),
                }
            else:
                return {
                    "xAxisData": [
                        handle_null_value(
                            safe_get_value(row, payload.dimension_col, null_label), null_label
                        )
                        for row in results
                    ],
                    "series": [
                        {
                            "name": get_aggregate_display_name(
                                payload.aggregate_func, payload.aggregate_col
                            ),
                            "data": [
                                row.get(
                                    get_aggregate_column_name(
                                        payload.aggregate_func, payload.aggregate_col
                                    ),
                                    0,
                                )
                                for row in results
                            ],
                        }
                    ],
                    "legend": [
                        get_aggregate_display_name(payload.aggregate_func, payload.aggregate_col)
                    ],
                }

    elif payload.chart_type == "table":
        # Table charts return raw data for frontend table display
        # The frontend handles the table rendering, we just need to return the data

        if payload.metrics:
            # Multiple metrics - return structured data with all columns
            table_data = []
            for row in results:
                row_data = {}

                # Add dimension column
                row_data[payload.dimension_col] = handle_null_value(
                    safe_get_value(row, payload.dimension_col, null_label), null_label
                )

                # Add extra dimension if present
                if payload.extra_dimension:
                    row_data[payload.extra_dimension] = handle_null_value(
                        safe_get_value(row, payload.extra_dimension, null_label), null_label
                    )

                # Add all metric columns
                for metric in payload.metrics:
                    if metric.aggregation.lower() == "count" and metric.column is None:
                        alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                        display_name = metric.alias or "Total Count"
                    else:
                        alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                        display_name = metric.alias or f"{metric.aggregation}({metric.column})"

                    row_data[display_name] = row.get(alias, 0)

                table_data.append(row_data)

            return {
                "tableData": table_data,
                "columns": list(table_data[0].keys()) if table_data else [],
            }
        else:
            # Single metric (legacy) - return structured data
            table_data = []
            for row in results:
                row_data = {}

                # Add dimension column
                row_data[payload.dimension_col] = handle_null_value(
                    safe_get_value(row, payload.dimension_col, null_label), null_label
                )

                # Add extra dimension if present
                if payload.extra_dimension:
                    row_data[payload.extra_dimension] = handle_null_value(
                        safe_get_value(row, payload.extra_dimension, null_label), null_label
                    )

                # Add metric column
                agg_col_name = get_aggregate_column_name(
                    payload.aggregate_func, payload.aggregate_col
                )
                display_name = get_aggregate_display_name(
                    payload.aggregate_func, payload.aggregate_col
                )
                row_data[display_name] = row.get(agg_col_name, 0)

                table_data.append(row_data)

            return {
                "tableData": table_data,
                "columns": list(table_data[0].keys()) if table_data else [],
            }

    elif payload.chart_type == "number":
        # Number charts only support aggregated data and return a single value
        if payload.computation_type == "aggregated" and results:
            # Get the first (and should be only) row
            row = results[0] if results else {}
            agg_col_name = get_aggregate_column_name(payload.aggregate_func, payload.aggregate_col)
            value = row.get(agg_col_name, 0)

            # Handle None values - show "No data" instead of trying to format None as a number
            if value is None:
                return {
                    "value": None,
                    "metric_name": get_aggregate_display_name(
                        payload.aggregate_func, payload.aggregate_col
                    ),
                    "is_null": True,
                }

            return {
                "value": value,
                "metric_name": get_aggregate_display_name(
                    payload.aggregate_func, payload.aggregate_col
                ),
                "is_null": False,
            }
        else:
            return {"value": None, "metric_name": "No data", "is_null": True}

    return {}


def get_chart_data_table_preview(
    org_warehouse: OrgWarehouse,
    payload: ChartDataPayload,
) -> Dict[str, Any]:
    """Get paginated table preview with column information

    Can fetch all columns (default) or specific columns for chart preview.
    """
    warehouse = get_warehouse_client(org_warehouse)

    # Use the same query builder as chart data
    query_builder = build_chart_query(payload)

    # Build column mapping based on computation type
    column_mapping = []
    columns = []

    if payload.computation_type == "raw":
        col_index = 0
        if payload.x_axis:
            column_mapping.append((payload.x_axis, col_index))
            columns.append(payload.x_axis)
            col_index += 1
        if payload.y_axis:
            column_mapping.append((payload.y_axis, col_index))
            columns.append(payload.y_axis)
            col_index += 1
        if payload.extra_dimension:
            column_mapping.append((payload.extra_dimension, col_index))
            columns.append(payload.extra_dimension)
    else:  # aggregated
        col_index = 0
        column_mapping.append((payload.dimension_col, col_index))
        columns.append(payload.dimension_col)
        col_index += 1

        # Handle multiple metrics
        if payload.metrics:
            for metric in payload.metrics:
                if metric.aggregation.lower() == "count" and metric.column is None:
                    alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    display_name = metric.alias or "Total Count"
                else:
                    alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                    display_name = metric.alias or f"{metric.aggregation}({metric.column})"
                column_mapping.append((alias, col_index))
                columns.append(display_name)
                col_index += 1
        else:
            # Single metric (legacy approach)
            agg_col_name = get_aggregate_column_name(payload.aggregate_func, payload.aggregate_col)
            column_mapping.append((agg_col_name, col_index))
            columns.append(agg_col_name)
            col_index += 1

        if payload.extra_dimension:
            column_mapping.append((payload.extra_dimension, col_index))
            columns.append(payload.extra_dimension)

    # Execute query with column mapping
    data_dicts = execute_query(warehouse, query_builder, column_mapping)

    # For chart preview, we don't need column types for specific columns
    column_types = {col: "unknown" for col in columns}

    # Calculate page info
    limit, offset = get_pagination_params(payload)
    page_size = limit
    page = (offset // page_size) + 1 if page_size > 0 else 1

    # Get total count using the existing query_builder as subquery (without LIMIT/OFFSET)
    # Temporarily remove pagination from existing query builder
    original_limit = query_builder.limit_records
    original_offset = query_builder.offset_records
    query_builder.limit_records = None
    query_builder.offset_records = 0

    # Build the original query as subquery and wrap with COUNT(*)
    original_subquery = query_builder.build()
    count_sql = f"SELECT COUNT(*) as total FROM ({original_subquery.compile(bind=warehouse.engine, compile_kwargs={'literal_binds': True})}) as subquery"

    # Restore original pagination settings
    query_builder.limit_records = original_limit
    query_builder.offset_records = original_offset

    count_result = warehouse.execute(count_sql)
    total_rows = count_result[0]["total"] if count_result else 0

    return {
        "columns": columns,
        "column_types": column_types,
        "data": data_dicts,
        "total_rows": total_rows,
        "page": page,
        "page_size": page_size,
    }
