"""Chart service module for handling chart business logic"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import column, func, and_, or_, text

from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.chart_schema import (
    ChartDataPayload,
    ExecuteChartQuery,
    TransformDataForChart,
)


def apply_time_grain(column_expr, time_grain: str, warehouse_type: str = "postgres"):
    """
    Apply time grain to a datetime column using database-specific functions.

    Args:
        column_expr: SQLAlchemy column expression
        time_grain: One of 'year', 'month', 'day', 'hour', 'minute', 'second'
        warehouse_type: Type of warehouse ('postgres' or 'bigquery')

    Returns:
        SQLAlchemy expression with time grain applied
    """
    if not time_grain:
        return column_expr

    if warehouse_type.lower() in ["postgres", "postgresql"]:
        # PostgreSQL uses DATE_TRUNC function
        return func.date_trunc(time_grain, column_expr)
    elif warehouse_type.lower() == "bigquery":
        # BigQuery uses different functions for different time grains
        if time_grain == "year":
            return func.datetime_trunc(column_expr, text("YEAR"))
        elif time_grain == "month":
            return func.datetime_trunc(column_expr, text("MONTH"))
        elif time_grain == "day":
            return func.datetime_trunc(column_expr, text("DAY"))
        elif time_grain == "hour":
            return func.datetime_trunc(column_expr, text("HOUR"))
        elif time_grain == "minute":
            return func.datetime_trunc(column_expr, text("MINUTE"))
        elif time_grain == "second":
            return func.datetime_trunc(column_expr, text("SECOND"))
    else:
        # Default to PostgreSQL syntax for other databases
        return func.date_trunc(time_grain, column_expr)

    return column_expr


def get_pagination_params(payload: ChartDataPayload):
    """
    Extract pagination parameters from payload.
    Returns (limit, offset) tuple with proper defaults.
    """
    # Table charts use a custom preview pagination path; avoid inner LIMIT/OFFSET
    # so that table-specific query logic is applied consistently.
    if payload.chart_type == "table":
        return None, None

    # Check if pagination is enabled in extra_config
    if (
        payload.extra_config
        and payload.extra_config.get("pagination")
        and payload.extra_config["pagination"].get("enabled")
    ):
        page_size = payload.extra_config["pagination"].get("page_size", 50)
        # For preview/build, always start from offset 0
        return page_size, 0

    return None, None


def normalize_dimensions(payload: ChartDataPayload) -> List[str]:
    """
    Normalize dimensions from payload, handling backward compatibility.
    For table charts: prefer dimensions list, fallback to dimension_col + extra_dimension
    For other charts: use dimension_col if present

    Returns list of dimension column names.
    """
    logger.info(
        f"normalize_dimensions - chart_type: {payload.chart_type}, dimensions: {payload.dimensions}, dimension_col: {payload.dimension_col}, extra_dimension: {payload.extra_dimension}"
    )

    if payload.chart_type == "table":
        # For table charts, use dimensions list if available
        if payload.dimensions:
            # Filter out empty strings
            filtered_dims = [d for d in payload.dimensions if d and d.strip()]
            if filtered_dims:
                logger.info(f"normalize_dimensions - Using dimensions array: {filtered_dims}")
                return filtered_dims
            else:
                logger.warning(
                    f"normalize_dimensions - dimensions array was provided but all were empty: {payload.dimensions}"
                )
        # Backward compatibility: convert dimension_col + extra_dimension to list
        dims = []
        if payload.dimension_col:
            dims.append(payload.dimension_col)
        if payload.extra_dimension:
            dims.append(payload.extra_dimension)
        if dims:
            logger.info(f"normalize_dimensions - Using backward-compatible dimensions: {dims}")
        else:
            logger.warning(f"normalize_dimensions - No dimensions found in payload for table chart")
        return dims
    else:
        # For other charts, return single dimension if present
        if payload.dimension_col:
            return [payload.dimension_col]
        return []


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
    org_warehouse: OrgWarehouse = None,
) -> AggQueryBuilder:
    """Build query for multiple metrics on bar/line/pie/table/map charts"""
    dimensions = normalize_dimensions(payload)

    logger.info(
        f"build_multi_metric_query - chart_type: {payload.chart_type}, dimensions: {dimensions}, metrics: {payload.metrics}"
    )

    # For non-table charts, require at least one dimension
    if payload.chart_type != "table" and not dimensions:
        raise ValueError("dimension_col is required for multiple metrics charts")

    # For table charts, metrics are optional
    if payload.chart_type != "table":
        if not payload.metrics or len(payload.metrics) == 0:
            raise ValueError("At least one metric is required for multiple metrics charts")

    # Add all dimension columns with time grain if specified
    time_grain = payload.extra_config.get("time_grain") if payload.extra_config else None

    for dim_col in dimensions:
        if not dim_col or not dim_col.strip():
            logger.warning(f"Skipping empty dimension column: {dim_col}")
            continue

        dimension_column = column(dim_col)

        # Apply time grain if specified and warehouse type is available
        if time_grain and org_warehouse:
            warehouse_type = org_warehouse.wtype.lower()
            dimension_column = apply_time_grain(dimension_column, time_grain, warehouse_type)
            # Add label to preserve original column name for data access
            dimension_column = dimension_column.label(dim_col)
        else:
            # Even without time grain, ensure we have a label for consistent key access
            dimension_column = dimension_column.label(dim_col)

        query_builder.add_column(dimension_column)
        logger.info(f"Added dimension column to query: {dim_col}")

        # Group by dimension column (use the same time grain logic)
        if time_grain and org_warehouse:
            # When time grain is applied, group by the time grain expression (without label)
            warehouse_type = org_warehouse.wtype.lower()
            time_grain_expr = apply_time_grain(column(dim_col), time_grain, warehouse_type)
            query_builder.group_cols_by(time_grain_expr)
        else:
            # Normal grouping by column name
            query_builder.group_cols_by(dim_col)

    # Add all metrics as aggregate columns (if present)
    if payload.metrics:
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

    # Add default ordering by time grain column when time grain is applied
    if time_grain and org_warehouse and dimensions:
        # Order by the first dimension column (which will have time grain applied) in ascending order (chronological)
        query_builder.order_cols_by([(dimensions[0], "asc")])

    return query_builder


def build_chart_query(
    payload: ChartDataPayload, org_warehouse: OrgWarehouse = None
) -> AggQueryBuilder:
    """Build query using unified AggQueryBuilder for both raw and aggregated queries"""

    # Get pagination parameters
    limit, offset = get_pagination_params(payload)

    # If pagination is enabled, create a subquery with LIMIT/OFFSET first
    if limit is not None:
        # Step 1: Create inner query that selects all columns with LIMIT/OFFSET
        inner_query_builder = AggQueryBuilder()
        inner_query_builder.fetch_from(payload.table_name, payload.schema_name)

        # Add all columns from the table (SELECT * equivalent)
        inner_query_builder.add_column(text("*"))

        # Apply LIMIT/OFFSET to inner query
        inner_query_builder.limit_rows(limit)
        inner_query_builder.offset_rows(offset)

        # Create subquery
        inner_subquery = inner_query_builder.subquery("paginated_data")

        # Step 2: Build main query on the paginated data
        query_builder = AggQueryBuilder()
        query_builder.fetch_from_subquery(inner_subquery)
    else:
        # No pagination, use original table directly
        query_builder = AggQueryBuilder()
        query_builder.fetch_from(payload.table_name, payload.schema_name)

        # Now build the rest of the query logic on top of the (possibly paginated) data source
        # Table charts can work with just dimensions (no metrics) - non-aggregated query
        # Other charts require metrics for aggregation
        if payload.chart_type != "table":
            if not payload.metrics or len(payload.metrics) == 0:
                raise ValueError("At least one metric is required for aggregated charts")
        elif payload.chart_type == "table":
            # Table charts: if no metrics, just select dimensions (non-aggregated)
            dimensions = normalize_dimensions(payload)
            if not dimensions:
                raise ValueError("At least one dimension is required for table charts")

            if not payload.metrics or len(payload.metrics) == 0:
                # Non-aggregated query: just select dimension columns
                logger.info(
                    f"Table chart - no metrics, selecting {len(dimensions)} dimensions directly"
                )
                for dim_col in dimensions:
                    if not dim_col or not dim_col.strip():
                        continue
                    dim_expr = column(dim_col)
                    # Always label to ensure consistent key access
                    dim_expr = dim_expr.label(dim_col)
                    query_builder.add_column(dim_expr)
                    logger.info(f"Added dimension column (no metrics): {dim_col}")
                # No GROUP BY needed for non-aggregated queries
            else:
                # Aggregated query: use multi-metric query builder
                logger.info(
                    f"Table chart - has metrics, using build_multi_metric_query with {len(dimensions)} dimensions"
                )
                query_builder = build_multi_metric_query(payload, query_builder, org_warehouse)

            # Apply filters and sorting before returning
            if payload.dashboard_filters:
                query_builder = apply_dashboard_filters(query_builder, payload.dashboard_filters)
            if payload.extra_config and payload.extra_config.get("filters"):
                query_builder = apply_chart_filters(query_builder, payload.extra_config["filters"])
            if payload.extra_config and payload.extra_config.get("sort"):
                query_builder = apply_chart_sorting(
                    query_builder, payload.extra_config["sort"], payload
                )

            return query_builder

        # For number charts, we don't need dimension columns
        if payload.chart_type == "number":
            # Use first metric for number charts
            metric = payload.metrics[0]

            # Handle count with None column case
            if metric.aggregation.lower() == "count" and metric.column is None:
                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
            else:
                if not metric.column:
                    raise ValueError(f"Column is required for {metric.aggregation} aggregation")
                alias = metric.alias or f"{metric.aggregation}_{metric.column}"

            # Just add the aggregate column without any grouping
            query_builder.add_aggregate_column(
                metric.column,
                metric.aggregation,
                alias,
            )
        elif payload.chart_type == "pie":
            # Pie charts need dimension and one metric
            if not payload.dimension_col:
                raise ValueError("dimension_col is required for pie charts")

            # Add dimension column with time grain if specified
            dimension_column = column(payload.dimension_col)

            # Apply time grain if specified and warehouse type is available
            time_grain = payload.extra_config.get("time_grain") if payload.extra_config else None
            if time_grain and org_warehouse:
                warehouse_type = org_warehouse.wtype.lower()
                dimension_column = apply_time_grain(dimension_column, time_grain, warehouse_type)
                # Add label to preserve original column name for data access
                dimension_column = dimension_column.label(payload.dimension_col)

            query_builder.add_column(dimension_column)

            # Add extra dimension if specified (for combination slices)
            if payload.extra_dimension:
                query_builder.add_column(column(payload.extra_dimension))

            # Use first metric for pie charts
            metric = payload.metrics[0]

            # Handle count with None column case
            if metric.aggregation.lower() == "count" and metric.column is None:
                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
            else:
                if not metric.column:
                    raise ValueError(f"Column is required for {metric.aggregation} aggregation")
                alias = metric.alias or f"{metric.aggregation}_{metric.column}"

            # Add aggregate column
            query_builder.add_aggregate_column(
                metric.column,
                metric.aggregation,
                alias,
            )

            # Group by dimension column and extra dimension if provided
            if time_grain and org_warehouse:
                # When time grain is applied, group by the time grain expression (without label)
                warehouse_type = org_warehouse.wtype.lower()
                time_grain_expr = apply_time_grain(
                    column(payload.dimension_col), time_grain, warehouse_type
                )
                query_builder.group_cols_by(time_grain_expr)
            else:
                # Normal grouping by column name
                query_builder.group_cols_by(payload.dimension_col)

            if payload.extra_dimension:
                query_builder.group_cols_by(payload.extra_dimension)

            # Add default ordering by time grain column when time grain is applied
            if time_grain and org_warehouse:
                # Order by the dimension column (which will have time grain applied) in ascending order (chronological)
                query_builder.order_cols_by([(payload.dimension_col, "asc")])
        else:
            # Bar, line, and other charts - use multi-metric query
            query_builder = build_multi_metric_query(payload, query_builder, org_warehouse)

    # Apply dashboard filters if provided
    if payload.dashboard_filters:
        query_builder = apply_dashboard_filters(query_builder, payload.dashboard_filters)

    # Apply chart-level filters if provided
    if payload.extra_config and payload.extra_config.get("filters"):
        query_builder = apply_chart_filters(query_builder, payload.extra_config["filters"])

    # Apply chart-level sorting if provided
    if payload.extra_config and payload.extra_config.get("sort"):
        query_builder = apply_chart_sorting(query_builder, payload.extra_config["sort"], payload)

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

        # Try to match against metric aliases first
        matching_metric = None
        if payload and payload.metrics:
            for metric in payload.metrics:
                if metric.alias == column_name:
                    matching_metric = metric
                    break

        if matching_metric:
            # It's a metric - generate the actual SQL alias that matches SELECT clause
            if matching_metric.aggregation.lower() == "count" and matching_metric.column is None:
                sort_column = (
                    f"count_all_{matching_metric.alias}" if matching_metric.alias else "count_all"
                )
            else:
                sort_column = (
                    matching_metric.alias
                    or f"{matching_metric.aggregation}_{matching_metric.column}"
                )
        else:
            # It's a dimension column - use as-is
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
    if results and len(results) > 0:
        logger.info(f"Query result columns (first row keys): {list(results[0].keys())}")
        logger.info(f"Query result sample (first row): {results[0]}")

    # Return raw results if no mapping provided
    return list(results)


def execute_chart_query(
    warehouse_client: Warehouse, query_builder: AggQueryBuilder, payload: ExecuteChartQuery
) -> List[Dict[str, Any]]:
    """Execute query and convert results to dictionaries for charts"""
    # Build column mapping - all charts use aggregated (metrics-based) approach
    column_mapping = []
    col_index = 0

    # For table charts with multiple dimensions, handle dimensions array
    if payload.chart_type == "table" and payload.dimensions:
        # Add all dimensions from the array
        for dim_col in payload.dimensions:
            if dim_col and dim_col.strip():
                column_mapping.append((dim_col, col_index))
                col_index += 1
                logger.info(
                    f"execute_chart_query - Added dimension to mapping: {dim_col} at index {col_index - 1}"
                )
    else:
        # For other charts or backward compatibility, use dimension_col
        if payload.chart_type != "number" and payload.dimension_col:
            column_mapping.append((payload.dimension_col, col_index))
            col_index += 1

    # Handle metrics - metrics are required for all charts (except table charts without metrics)
    if payload.metrics:
        for metric in payload.metrics:
            if metric.aggregation.lower() == "count" and metric.column is None:
                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
            else:
                alias = metric.alias or f"{metric.aggregation}_{metric.column}"
            column_mapping.append((alias, col_index))
            col_index += 1

    # Handle extra_dimension for backward compatibility (only if not using dimensions array)
    if payload.extra_dimension and not (payload.chart_type == "table" and payload.dimensions):
        column_mapping.append((payload.extra_dimension, col_index))

    logger.info(
        f"execute_chart_query - column_mapping: {column_mapping}, chart_type: {payload.chart_type}"
    )
    return execute_query(warehouse_client, query_builder, column_mapping)


def transform_data_for_chart(
    results: List[Dict[str, Any]],
    payload: TransformDataForChart,
) -> Dict[str, Any]:
    """Transform query results to chart-specific data format"""

    # Get custom null label from customizations if provided
    null_label = payload.customizations.get("nullValueLabel") if payload.customizations else None

    if payload.chart_type == "bar":
        # All charts use aggregated (metrics-based) approach
        if not payload.metrics or len(payload.metrics) == 0:
            return {}

        # Check if we have extra_dimension for grouping
        if payload.extra_dimension:
            # Group by extra dimension with multiple metrics
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

                # Store each metric value for this dimension-x_value combination
                for metric in payload.metrics:
                    if metric.aggregation.lower() == "count" and metric.column is None:
                        alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    else:
                        alias = metric.alias or f"{metric.aggregation}_{metric.column}"

                    # Create key for this metric
                    metric_key = metric.alias or f"{metric.aggregation}_{metric.column or 'all'}"

                    if metric_key not in grouped_data[dimension]:
                        grouped_data[dimension][metric_key] = {}

                    grouped_data[dimension][metric_key][x_value] = row.get(alias, 0)

            x_axis_data = sorted(list(x_values))

            series_data = []
            legend_data = []

            # If we have multiple metrics, create series for each dimension-metric combination
            if len(payload.metrics) > 1:
                for dimension, metrics_data in grouped_data.items():
                    for metric_key, values in metrics_data.items():
                        display_name = f"{dimension} - {metric_key}"
                        series_data.append(
                            {
                                "name": display_name,
                                "data": [values.get(x, 0) for x in x_axis_data],
                            }
                        )
                        legend_data.append(display_name)
            else:
                # Single metric - just use dimension as series name
                for dimension, metrics_data in grouped_data.items():
                    # Get the first (and only) metric data
                    metric_values = next(iter(metrics_data.values()))
                    series_data.append(
                        {
                            "name": dimension,
                            "data": [metric_values.get(x, 0) for x in x_axis_data],
                        }
                    )
                    legend_data.append(dimension)

            return {
                "xAxisData": x_axis_data,  # For vertical bars
                "yAxisData": x_axis_data,  # For horizontal bars
                "series": series_data,
                "legend": legend_data,
            }
        else:
            # No extra dimension, just metrics
            x_axis_data = [
                handle_null_value(
                    safe_get_value(row, payload.dimension_col, null_label), null_label
                )
                for row in results
            ]

            # Create series for each dimension-metric combination
            series_data = []
            legend_data = []

            for metric in payload.metrics:
                if metric.aggregation.lower() == "count" and metric.column is None:
                    alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    display_name = metric.alias or "Total Count"
                else:
                    alias = metric.alias or f"{metric.aggregation}_{metric.column}"
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
                "xAxisData": x_axis_data,  # For vertical bars
                "yAxisData": x_axis_data,  # For horizontal bars
                "series": series_data,
                "legend": legend_data,
            }

    elif payload.chart_type == "pie":
        # All charts use aggregated (metrics-based) approach
        if not payload.metrics or len(payload.metrics) == 0:
            return {}

        # Use first metric for pie charts
        metric = payload.metrics[0]
        if metric.aggregation.lower() == "count" and metric.column is None:
            alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
            display_name = metric.alias or "Total Count"
        else:
            alias = metric.alias or f"{metric.aggregation}_{metric.column}"
            display_name = metric.alias or f"{metric.aggregation}({metric.column})"

        pie_data = []
        for row in results:
            # Create slice name based on dimension_col and extra_dimension
            if payload.extra_dimension:
                # Combine both dimensions for the slice name
                dimension_value = handle_null_value(
                    safe_get_value(row, payload.dimension_col, null_label), null_label
                )
                extra_dimension_value = handle_null_value(
                    safe_get_value(row, payload.extra_dimension, null_label), null_label
                )
                slice_name = f"{dimension_value} - {extra_dimension_value}"
            else:
                # Just use the main dimension
                slice_name = handle_null_value(
                    safe_get_value(row, payload.dimension_col, null_label), null_label
                )

            pie_data.append(
                {
                    "value": row.get(alias, 0),
                    "name": slice_name,
                }
            )

        # Apply slice limiting if configured
        max_slices = None
        if payload.customizations and "maxSlices" in payload.customizations:
            max_slices = payload.customizations["maxSlices"]

        if (
            max_slices
            and isinstance(max_slices, int)
            and max_slices > 0
            and len(pie_data) > max_slices
        ):
            # Sort pie data by value in descending order
            pie_data_sorted = sorted(pie_data, key=lambda x: x["value"], reverse=True)

            # Take top N slices
            top_slices = pie_data_sorted[:max_slices]

            # Group remaining slices under "Other"
            remaining_slices = pie_data_sorted[max_slices:]
            other_value = sum(slice_data["value"] for slice_data in remaining_slices)

            if other_value > 0:
                top_slices.append({"value": other_value, "name": "Other"})

            pie_data = top_slices

        return {
            "pieData": pie_data,
            "seriesName": display_name,
        }

    elif payload.chart_type == "line":
        # All charts use aggregated (metrics-based) approach
        if not payload.metrics or len(payload.metrics) == 0:
            return {}

        # Check if we have extra_dimension for grouping
        if payload.extra_dimension:
            # Similar to bar chart grouping with metrics
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

                # Store each metric value for this dimension-x_value combination
                for metric in payload.metrics:
                    if metric.aggregation.lower() == "count" and metric.column is None:
                        alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    else:
                        alias = metric.alias or f"{metric.aggregation}_{metric.column}"

                    # Create key for this metric
                    metric_key = metric.alias or f"{metric.aggregation}_{metric.column or 'all'}"

                    if metric_key not in grouped_data[dimension]:
                        grouped_data[dimension][metric_key] = {}

                    grouped_data[dimension][metric_key][x_value] = row.get(alias, 0)

            x_axis_data = sorted(list(x_values))

            series_data = []
            legend_data = []

            # If we have multiple metrics, create series for each dimension-metric combination
            if len(payload.metrics) > 1:
                for dimension, metrics_data in grouped_data.items():
                    for metric_key, values in metrics_data.items():
                        display_name = f"{dimension} - {metric_key}"
                        series_data.append(
                            {
                                "name": display_name,
                                "data": [values.get(x, 0) for x in x_axis_data],
                            }
                        )
                        legend_data.append(display_name)
            else:
                # Single metric - just use dimension as series name
                for dimension, metrics_data in grouped_data.items():
                    # Get the first (and only) metric data
                    metric_values = next(iter(metrics_data.values()))
                    series_data.append(
                        {
                            "name": dimension,
                            "data": [metric_values.get(x, 0) for x in x_axis_data],
                        }
                    )
                    legend_data.append(dimension)

            return {
                "xAxisData": x_axis_data,
                "series": series_data,
                "legend": legend_data,
            }
        else:
            # No extra dimension, just metrics
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

    elif payload.chart_type == "table":
        # Table charts return raw data for frontend table display
        # The frontend handles the table rendering, we just need to return the data
        # Get dimensions from payload.dimensions if available, otherwise normalize from dimension_col/extra_dimension
        if payload.dimensions and len(payload.dimensions) > 0:
            dimensions = [d for d in payload.dimensions if d and d.strip()]
            logger.info(f"Table chart - Using payload.dimensions: {dimensions}")
        else:
            # Fallback to normalize_dimensions logic for backward compatibility
            dims = []
            if payload.dimension_col:
                dims.append(payload.dimension_col)
            if payload.extra_dimension:
                dims.append(payload.extra_dimension)
            dimensions = dims
            logger.info(f"Table chart - Using fallback dimensions: {dimensions}")

        logger.info(
            f"Table chart - dimensions: {dimensions}, dimensions_count: {len(dimensions)}, metrics: {payload.metrics}, results count: {len(results)}"
        )
        if results:
            logger.info(f"Sample row keys: {list(results[0].keys())}")

        table_data = []
        for row in results:
            row_data = {}

            # Add all dimension columns
            for dim_col in dimensions:
                # Try to get the value - check both the column name and if it exists in row
                value = row.get(dim_col)
                if value is None:
                    # Try with different key variations (in case of time grain or aliases)
                    # The row keys should match dim_col due to .label() in query builder
                    logger.warning(
                        f"Dimension column '{dim_col}' not found in row. Available keys: {list(row.keys())}"
                    )
                row_data[dim_col] = handle_null_value(
                    safe_get_value(row, dim_col, null_label), null_label
                )

            # Add all metric columns if present
            if payload.metrics:
                for metric in payload.metrics:
                    if metric.aggregation.lower() == "count" and metric.column is None:
                        alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                        display_name = metric.alias or "Total Count"
                    else:
                        alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                        display_name = metric.alias or f"{metric.aggregation}({metric.column})"

                    row_data[display_name] = row.get(alias, 0)

            table_data.append(row_data)

        # Build columns list from dimensions + metrics (not from data keys)
        # This ensures all configured columns are included even if data is empty
        columns_list = []
        # Add all dimension columns
        for dim_col in dimensions:
            columns_list.append(dim_col)
        # Add all metric columns
        if payload.metrics:
            for metric in payload.metrics:
                if metric.aggregation.lower() == "count" and metric.column is None:
                    alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    display_name = metric.alias or "Total Count"
                else:
                    alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                    display_name = metric.alias or f"{metric.aggregation}({metric.column})"
                columns_list.append(display_name)

        logger.info(f"Table chart - returning {len(table_data)} rows with columns: {columns_list}")
        logger.info(
            f"Table chart - data row keys (for comparison): {list(table_data[0].keys()) if table_data else []}"
        )

        return {
            "tableData": table_data,
            "columns": columns_list,  # Use explicit columns list, not data keys
        }

    elif payload.chart_type == "number":
        # Number charts return a single aggregated value
        if results:
            if not payload.metrics or len(payload.metrics) == 0:
                return {"value": None, "metric_name": "No data", "is_null": True}

            # Use first metric for number charts
            metric = payload.metrics[0]
            row = results[0] if results else {}

            if metric.aggregation.lower() == "count" and metric.column is None:
                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                display_name = metric.alias or "Total Count"
            else:
                alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                display_name = metric.alias or f"{metric.aggregation}({metric.column})"

            value = row.get(alias, 0)

            # Handle None values - show "No data" instead of trying to format None as a number
            if value is None:
                return {
                    "value": None,
                    "metric_name": display_name,
                    "is_null": True,
                }

            return {
                "value": value,
                "metric_name": display_name,
                "is_null": False,
            }
        else:
            return {"value": None, "metric_name": "No data", "is_null": True}

    return {}


def get_chart_data_table_preview(
    org_warehouse: OrgWarehouse,
    payload: ChartDataPayload,
    page: int = 0,
    limit: int = 100,
) -> Dict[str, Any]:
    """Get paginated table preview with column information

    Can fetch all columns (default) or specific columns for chart preview.
    """
    warehouse = get_warehouse_client(org_warehouse)

    # Use the same query builder as chart data
    query_builder = build_chart_query(payload, org_warehouse)

    # Build column mapping - use normalized dimensions
    column_mapping = []
    columns = []
    col_index = 0

    dimensions = normalize_dimensions(payload)

    logger.info(
        f"Table preview - dimensions: {dimensions}, dimensions_count: {len(dimensions)}, metrics: {payload.metrics}"
    )
    logger.info(
        f"Table preview - payload.dimensions: {payload.dimensions}, payload.dimension_col: {payload.dimension_col}, payload.extra_dimension: {payload.extra_dimension}"
    )

    # Add all dimension columns
    if not dimensions or len(dimensions) == 0:
        logger.error(
            f"Table preview - ERROR: No dimensions found after normalization! Payload had: dimensions={payload.dimensions}, dimension_col={payload.dimension_col}, extra_dimension={payload.extra_dimension}"
        )

    for dim_col in dimensions:
        if not dim_col or not dim_col.strip():
            logger.warning(f"Table preview - Skipping empty dimension: '{dim_col}'")
            continue
        column_mapping.append((dim_col, col_index))
        columns.append(dim_col)
        col_index += 1
        logger.info(
            f"Table preview - Added dimension column to mapping: {dim_col} at index {col_index - 1}"
        )

    # Handle multiple metrics (if present)
    if payload.metrics:
        for metric in payload.metrics:
            if metric.aggregation.lower() == "count" and metric.column is None:
                alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                display_name = metric.alias or "Total Count"
            else:
                alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                display_name = metric.alias or f"{metric.aggregation}({metric.column})"
            # Use display_name for columns array to match transform_data_for_chart
            # But use alias for column_mapping to match query results
            column_mapping.append((alias, col_index))
            columns.append(display_name)  # Use display_name to match transform_data_for_chart
            col_index += 1

    # apply the pagination limits on the query
    offset = page * limit
    query_builder.limit_records = limit
    query_builder.offset_records = offset

    # Execute query with column mapping
    data_dicts = execute_query(warehouse, query_builder, column_mapping)

    # Transform data to use display_names instead of aliases for metrics
    # This ensures consistency with transform_data_for_chart
    transformed_data = []
    for row in data_dicts:
        transformed_row = {}

        # Copy dimension columns as-is (they use column names directly)
        for dim_col in dimensions:
            transformed_row[dim_col] = row.get(dim_col)

        # Transform metric columns from alias to display_name
        if payload.metrics:
            for metric in payload.metrics:
                if metric.aggregation.lower() == "count" and metric.column is None:
                    alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                    display_name = metric.alias or "Total Count"
                else:
                    alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                    display_name = metric.alias or f"{metric.aggregation}({metric.column})"

                # Map from alias (query result key) to display_name (column name)
                transformed_row[display_name] = row.get(alias, 0)

        transformed_data.append(transformed_row)

    logger.info(f"Table preview - transformed {len(transformed_data)} rows, columns: {columns}")
    if transformed_data:
        logger.info(f"Table preview - first row keys: {list(transformed_data[0].keys())}")

    # For chart preview, we don't need column types for specific columns
    column_types = {col: "unknown" for col in columns}

    return {
        "columns": columns,  # Now uses display_names for metrics
        "column_types": column_types,
        "data": transformed_data,  # Data rows now use display_names as keys
        "page": page,
        "limit": limit,
    }


def get_chart_data_total_rows(
    org_warehouse: OrgWarehouse,
    payload: ChartDataPayload,
) -> int:
    """Get total number of rows for the chart data query"""
    warehouse = get_warehouse_client(org_warehouse)

    # Use the same query builder as chart data
    query_builder = build_chart_query(payload, org_warehouse)

    # Build the original query as subquery and wrap with COUNT(*)
    original_subquery = query_builder.build()
    count_sql = f"SELECT COUNT(*) as total FROM ({original_subquery.compile(bind=warehouse.engine, compile_kwargs={'literal_binds': True})}) as subquery"

    count_result = warehouse.execute(count_sql)
    total_rows = count_result[0]["total"] if count_result else 0

    return total_rows
