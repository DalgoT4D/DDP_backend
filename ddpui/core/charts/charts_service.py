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
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials
from ddpui.core.dbtautomation_service import map_airbyte_keys_to_postgres_keys

logger = CustomLogger("ddpui.charts")


def get_warehouse_client(org_warehouse: OrgWarehouse):
    """Get warehouse client using the standard method"""
    credentials = retrieve_warehouse_credentials(org_warehouse)
    if org_warehouse.wtype == "postgres":
        credentials = map_airbyte_keys_to_postgres_keys(credentials)
    return get_client(org_warehouse.wtype, credentials, org_warehouse.bq_location)


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
    computation_type: str,
    table_name: str,
    schema_name: str,
    x_axis: Optional[str] = None,
    y_axis: Optional[str] = None,
    dimension_col: Optional[str] = None,
    aggregate_col: Optional[str] = None,
    aggregate_func: Optional[str] = None,
    extra_dimension: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> AggQueryBuilder:
    """Build query using unified AggQueryBuilder for both raw and aggregated queries"""
    query_builder = AggQueryBuilder()
    query_builder.fetch_from(table_name, schema_name)

    if computation_type == "raw":
        # For raw queries, add regular columns without aggregation
        if x_axis:
            query_builder.add_column(column(x_axis))
        if y_axis:
            query_builder.add_column(column(y_axis))
        if extra_dimension:
            query_builder.add_column(column(extra_dimension))

        # Validate that at least one column is specified
        if not x_axis and not y_axis:
            raise ValueError(
                "At least one column (x_axis or y_axis) must be specified for raw data"
            )

    else:  # aggregated
        # Add dimension column
        query_builder.add_column(column(dimension_col))

        # Add aggregate column
        query_builder.add_aggregate_column(
            aggregate_col, aggregate_func, f"{aggregate_func}_{aggregate_col}"
        )

        # Group by dimension column
        query_builder.group_cols_by(dimension_col)

        # Add extra dimension if specified
        if extra_dimension:
            query_builder.add_column(column(extra_dimension))
            query_builder.group_cols_by(extra_dimension)

    # Add pagination
    query_builder.limit_rows(limit)
    query_builder.offset_rows(offset)

    return query_builder


def execute_chart_query(
    warehouse_client,
    query_builder: AggQueryBuilder,
    computation_type: str,
    x_axis: Optional[str] = None,
    y_axis: Optional[str] = None,
    dimension_col: Optional[str] = None,
    aggregate_col: Optional[str] = None,
    aggregate_func: Optional[str] = None,
    extra_dimension: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute query and convert results to dictionaries"""
    # Build and compile SQL
    sql_stmt = query_builder.build()
    compiled = sql_stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    sql = str(compiled)

    logger.info(f"Generated SQL: {sql}")

    # Execute query
    results = warehouse_client.execute(sql)

    # Convert tuple results to dictionaries
    dict_results = []

    if computation_type == "raw":
        # For raw queries, columns are in the order they were added
        for row in results:
            row_dict = {}
            col_index = 0
            if x_axis:
                row_dict[x_axis] = row[col_index]
                col_index += 1
            if y_axis:
                row_dict[y_axis] = row[col_index]
                col_index += 1
            if extra_dimension:
                row_dict[extra_dimension] = row[col_index]
            dict_results.append(row_dict)

    else:  # aggregated
        # For aggregated queries, columns are: dimension_col, aggregate_result, [extra_dimension]
        for row in results:
            row_dict = {}
            col_index = 0
            row_dict[dimension_col] = row[col_index]
            col_index += 1
            # The aggregate column name is formatted as func_column
            agg_col_name = f"{aggregate_func}_{aggregate_col}"
            row_dict[agg_col_name] = row[col_index]
            col_index += 1
            if extra_dimension:
                row_dict[extra_dimension] = row[col_index]
            dict_results.append(row_dict)

    return dict_results


def transform_data_for_chart(
    results: List[Dict[str, Any]],
    chart_type: str,
    computation_type: str,
    x_axis: Optional[str] = None,
    y_axis: Optional[str] = None,
    dimension_col: Optional[str] = None,
    aggregate_col: Optional[str] = None,
    aggregate_func: Optional[str] = None,
    extra_dimension: Optional[str] = None,
) -> Dict[str, Any]:
    """Transform query results to chart-specific data format"""

    # Handle None values - pie charts only need x_axis for raw data
    if computation_type == "raw" and chart_type != "pie" and (not x_axis or not y_axis):
        return {}
    elif computation_type == "raw" and chart_type == "pie" and not x_axis:
        return {}

    if chart_type == "bar":
        if computation_type == "raw":
            return {
                "xAxisData": [convert_value(row[x_axis]) for row in results],
                "series": [
                    {
                        "name": y_axis,
                        "data": [convert_value(row[y_axis]) for row in results],
                    }
                ],
                "legend": [y_axis],
            }
        else:  # aggregated
            if extra_dimension:
                # Group by extra dimension
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = row[extra_dimension]
                    x_value = row[dimension_col]
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    grouped_data[dimension][x_value] = row.get(
                        f"{aggregate_func}_{aggregate_col}", 0
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
                    "xAxisData": [row[dimension_col] for row in results],
                    "series": [
                        {
                            "name": f"{aggregate_func}({aggregate_col})",
                            "data": [row[f"{aggregate_func}_{aggregate_col}"] for row in results],
                        }
                    ],
                    "legend": [f"{aggregate_func}({aggregate_col})"],
                }

    elif chart_type == "pie":
        if computation_type == "raw":
            # For raw data, count occurrences
            value_counts = {}
            for row in results:
                key = str(row[x_axis])
                value_counts[key] = value_counts.get(key, 0) + 1

            return {
                "pieData": [{"value": count, "name": name} for name, count in value_counts.items()],
                "seriesName": x_axis,
            }
        else:  # aggregated
            return {
                "pieData": [
                    {
                        "value": row[f"{aggregate_func}_{aggregate_col}"],
                        "name": str(row[dimension_col]),
                    }
                    for row in results
                ],
                "seriesName": f"{aggregate_func}({aggregate_col})",
            }

    elif chart_type == "line":
        if computation_type == "raw":
            return {
                "xAxisData": [convert_value(row[x_axis]) for row in results],
                "series": [
                    {
                        "name": y_axis,
                        "data": [convert_value(row[y_axis]) for row in results],
                    }
                ],
                "legend": [y_axis],
            }
        else:  # aggregated
            if extra_dimension:
                # Similar to bar chart grouping
                grouped_data = {}
                x_values = set()

                for row in results:
                    dimension = row[extra_dimension]
                    x_value = row[dimension_col]
                    x_values.add(x_value)

                    if dimension not in grouped_data:
                        grouped_data[dimension] = {}

                    grouped_data[dimension][x_value] = row.get(
                        f"{aggregate_func}_{aggregate_col}", 0
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
                    "xAxisData": [row[dimension_col] for row in results],
                    "series": [
                        {
                            "name": f"{aggregate_func}({aggregate_col})",
                            "data": [row[f"{aggregate_func}_{aggregate_col}"] for row in results],
                        }
                    ],
                    "legend": [f"{aggregate_func}({aggregate_col})"],
                }

    return {}


def get_table_preview(
    org_warehouse: OrgWarehouse,
    schema_name: str,
    table_name: str,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    """Get paginated table preview with column information"""
    warehouse = get_warehouse_client(org_warehouse)

    # Get column information
    columns_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    column_info = warehouse.execute(columns_query)
    columns = [col[0] for col in column_info]
    column_types = {col[0]: col[1] for col in column_info}

    # Build data query using AggQueryBuilder
    query_builder = AggQueryBuilder()
    query_builder.fetch_from(table_name, schema_name)

    # Add all columns
    for col in columns:
        query_builder.add_column(column(col))

    # Add pagination
    page_size = limit
    page = (offset // page_size) + 1
    query_builder.limit_rows(page_size)
    query_builder.offset_rows(offset)

    # Execute query
    sql_stmt = query_builder.build()
    compiled = sql_stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    sql = str(compiled)

    logger.info(f"Generated SQL for preview: {sql}")

    results = warehouse.execute(sql)

    # Convert tuple results to dictionaries
    data_dicts = []
    for row in results:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col] = row[i]
        data_dicts.append(row_dict)

    # Get total count
    count_query = f"SELECT COUNT(*) as total FROM {schema_name}.{table_name}"
    count_result = warehouse.execute(count_query)
    total_rows = count_result[0][0] if count_result else 0

    return {
        "columns": columns,
        "column_types": column_types,
        "data": data_dicts,
        "total_rows": total_rows,
        "page": page,
        "page_size": page_size,
    }
