"""Chart service module for handling chart business logic"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from decimal import Decimal
import hashlib
import json

from django.utils import timezone
from sqlalchemy import column
from sqlalchemy.dialects import postgresql

from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart, ChartSnapshot
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

logger = CustomLogger("ddpui.charts")


def get_warehouse_client(org_warehouse: OrgWarehouse) -> Warehouse:
    """Get warehouse client using the standard method"""
    return WarehouseFactory.get_warehouse_client(org_warehouse)


def convert_value(value: Any) -> Any:
    """Convert values to JSON-serializable format"""
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    return value


def get_query_hash(
    chart_type: str,
    computation_type: str,
    schema_name: str,
    table_name: str,
    x_axis: Optional[str] = None,
    y_axis: Optional[str] = None,
    dimension_col: Optional[str] = None,
    aggregate_col: Optional[str] = None,
    aggregate_func: Optional[str] = None,
    extra_dimension: Optional[str] = None,
    offset: int = 0,
    limit: int = 100,
) -> str:
    """Generate hash for query caching"""
    query_dict = {
        "chart_type": chart_type,
        "computation_type": computation_type,
        "schema_name": schema_name,
        "table_name": table_name,
        "x_axis": x_axis,
        "y_axis": y_axis,
        "dimension_col": dimension_col,
        "aggregate_col": aggregate_col,
        "aggregate_func": aggregate_func,
        "extra_dimension": extra_dimension,
        "offset": offset,
        "limit": limit,
    }
    query_str = json.dumps(query_dict, sort_keys=True)
    return hashlib.sha256(query_str.encode()).hexdigest()


def get_cached_data(query_hash: str) -> Optional[Tuple[dict, dict]]:
    """Retrieve cached chart data if available"""
    cached_snapshot = (
        ChartSnapshot.objects.filter(query_hash=query_hash, expires_at__gt=timezone.now())
        .order_by("-created_at")
        .first()
    )

    if cached_snapshot:
        logger.info(f"Using cached data for query hash: {query_hash}")
        return cached_snapshot.data, cached_snapshot.echarts_config

    return None


def build_chart_query(
    payload: ChartDataPayload,
) -> AggQueryBuilder:
    """Build query using unified AggQueryBuilder for both raw and aggregated queries"""
    query_builder = AggQueryBuilder()
    query_builder.fetch_from(payload.table_name, payload.schema_name)

    if payload.computation_type == "raw":
        # For raw queries, add regular columns without aggregation
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
        # Add dimension column
        query_builder.add_column(column(payload.dimension_col))

        # Add aggregate column
        query_builder.add_aggregate_column(
            payload.aggregate_col,
            payload.aggregate_func,
            f"{payload.aggregate_func}_{payload.aggregate_col}",
        )

        # Group by dimension column
        query_builder.group_cols_by(payload.dimension_col)

        # Add extra dimension if specified
        if payload.extra_dimension:
            query_builder.add_column(column(payload.extra_dimension))
            query_builder.group_cols_by(payload.extra_dimension)

    # Add pagination
    query_builder.limit_rows(payload.limit)
    query_builder.offset_rows(payload.offset)

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
        # For aggregated queries, columns are: dimension_col, aggregate_result, [extra_dimension]
        col_index = 0
        column_mapping.append((payload.dimension_col, col_index))
        col_index += 1
        # The aggregate column name is formatted as func_column
        agg_col_name = f"{payload.aggregate_func}_{payload.aggregate_col}"
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
                "xAxisData": [convert_value(row[payload.x_axis]) for row in results],
                "series": [
                    {
                        "name": payload.y_axis,
                        "data": [convert_value(row[payload.y_axis]) for row in results],
                    }
                ],
                "legend": [payload.y_axis],
            }
        else:  # aggregated
            if payload.extra_dimension:
                # Group by extra dimension
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = row[payload.extra_dimension]
                    x_value = row[payload.dimension_col]
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    grouped_data[dimension][x_value] = row.get(
                        f"{payload.aggregate_func}_{payload.aggregate_col}", 0
                    )

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
                    "xAxisData": [row[payload.dimension_col] for row in results],
                    "series": [
                        {
                            "name": f"{payload.aggregate_func}({payload.aggregate_col})",
                            "data": [
                                row[f"{payload.aggregate_func}_{payload.aggregate_col}"]
                                for row in results
                            ],
                        }
                    ],
                    "legend": [f"{payload.aggregate_func}({payload.aggregate_col})"],
                }

    elif payload.chart_type == "pie":
        if payload.computation_type == "raw":
            # For raw data, count occurrences
            value_counts = {}
            for row in results:
                key = str(row[payload.x_axis])
                value_counts[key] = value_counts.get(key, 0) + 1

            return {
                "pieData": [{"value": count, "name": name} for name, count in value_counts.items()],
                "seriesName": payload.x_axis,
            }
        else:  # aggregated
            return {
                "pieData": [
                    {
                        "value": row[f"{payload.aggregate_func}_{payload.aggregate_col}"],
                        "name": str(row[payload.dimension_col]),
                    }
                    for row in results
                ],
                "seriesName": f"{payload.aggregate_func}({payload.aggregate_col})",
            }

    elif payload.chart_type == "line":
        if payload.computation_type == "raw":
            return {
                "xAxisData": [convert_value(row[payload.x_axis]) for row in results],
                "series": [
                    {
                        "name": payload.y_axis,
                        "data": [convert_value(row[payload.y_axis]) for row in results],
                    }
                ],
                "legend": [payload.y_axis],
            }
        else:  # aggregated
            if payload.extra_dimension:
                # Similar to bar chart grouping
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = row[payload.extra_dimension]
                    x_value = row[payload.dimension_col]
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    grouped_data[dimension][x_value] = row.get(
                        f"{payload.aggregate_func}_{payload.aggregate_col}", 0
                    )

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
                    "xAxisData": [row[payload.dimension_col] for row in results],
                    "series": [
                        {
                            "name": f"{payload.aggregate_func}({payload.aggregate_col})",
                            "data": [
                                row[f"{payload.aggregate_func}_{payload.aggregate_col}"]
                                for row in results
                            ],
                        }
                    ],
                    "legend": [f"{payload.aggregate_func}({payload.aggregate_col})"],
                }

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
        agg_col_name = f"{payload.aggregate_func}_{payload.aggregate_col}"
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
    page_size = payload.limit
    page = (payload.offset // page_size) + 1

    # Get total count
    count_query = f"SELECT COUNT(*) as total FROM {payload.schema_name}.{payload.table_name}"
    count_result = warehouse.execute(count_query)
    total_rows = count_result[0]["total"] if count_result else 0

    return {
        "columns": columns,
        "column_types": column_types,
        "data": data_dicts,
        "total_rows": total_rows,
        "page": page,
        "page_size": page_size,
    }
