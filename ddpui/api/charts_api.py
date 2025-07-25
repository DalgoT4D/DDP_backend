"""Chart API endpoints"""
from typing import Optional, List
from datetime import datetime, timedelta
import hashlib
import json

from django.shortcuts import get_object_or_404
from django.db.models import Q
from django.utils import timezone
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart, ChartSnapshot
from ddpui.datainsights.query_builder import AggQueryBuilder, QueryBuilder
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from sqlalchemy import column
from ddpui.core.echarts_config_generator import EChartsConfigGenerator
from ddpui.core import dbtautomation_service
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

charts_router = Router()


class ChartCreate(Schema):
    """Schema for creating a chart"""

    title: str
    description: Optional[str] = None
    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str

    # All column configuration and customizations in config
    config: dict


class ChartUpdate(Schema):
    """Schema for updating a chart"""

    title: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict] = None
    is_favorite: Optional[bool] = None


class ChartResponse(Schema):
    """Schema for chart response"""

    id: int
    title: str
    description: Optional[str]
    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str
    config: dict  # Contains all column configuration and customizations
    is_favorite: bool
    created_at: datetime
    updated_at: datetime


class ChartDataPayload(Schema):
    """Schema for chart data request"""

    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str

    # For raw data
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None

    # For aggregated data
    dimension_col: Optional[str] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    extra_dimension: Optional[str] = None

    # Customizations
    customizations: Optional[dict] = None

    # Pagination
    offset: int = 0
    limit: int = 100


class ChartDataResponse(Schema):
    """Schema for chart data response"""

    data: dict
    echarts_config: dict


class DataPreviewResponse(Schema):
    """Schema for data preview response"""

    columns: List[str]
    column_types: dict
    data: List[dict]
    total_rows: int
    page: int
    page_size: int


def has_schema_access(request, schema_name: str) -> bool:
    """Check if user has access to schema"""
    # TODO: Implement proper schema access control
    # For now, allow access to all schemas in the org
    return True


def convert_value(value):
    """Convert values to JSON-serializable format"""
    from datetime import datetime, date
    from decimal import Decimal

    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    return value


def transform_data_for_chart(results, chart_type, computation_type, payload):
    """Transform query results to chart-specific data format"""

    # Handle None values in payload - pie charts only need x_axis
    if (
        computation_type == "raw"
        and chart_type != "pie"
        and (not payload.x_axis or not payload.y_axis)
    ):
        return {}
    elif computation_type == "raw" and chart_type == "pie" and not payload.x_axis:
        return {}

    if chart_type == "bar":
        if computation_type == "raw":
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

    elif chart_type == "pie":
        if computation_type == "raw":
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

    elif chart_type == "line":
        if computation_type == "raw":
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


def get_query_hash(payload: ChartDataPayload) -> str:
    """Generate hash for query caching"""
    query_dict = {
        "chart_type": payload.chart_type,
        "computation_type": payload.computation_type,
        "schema_name": payload.schema_name,
        "table_name": payload.table_name,
        "x_axis": payload.x_axis,
        "y_axis": payload.y_axis,
        "dimension_col": payload.dimension_col,
        "aggregate_col": payload.aggregate_col,
        "aggregate_func": payload.aggregate_func,
        "extra_dimension": payload.extra_dimension,
        "offset": payload.offset,
        "limit": payload.limit,
    }
    query_str = json.dumps(query_dict, sort_keys=True)
    return hashlib.sha256(query_str.encode()).hexdigest()


@charts_router.get("/", response=List[ChartResponse])
@has_permission(["can_view_chart"])
def list_charts(request):
    """List all charts for the organization"""
    orguser = request.orguser
    charts = Chart.objects.filter(org=orguser.org).order_by("-updated_at")
    return charts


@charts_router.post("/chart-data/", response=ChartDataResponse)
@has_permission(["can_view_warehouse_data"])
def get_chart_data(request, payload: ChartDataPayload):
    """Get chart data with ECharts configuration"""
    orguser = request.orguser

    # Log the incoming payload for debugging
    logger.info(
        f"Chart data request - Type: {payload.computation_type}, Schema: {payload.schema_name}, Table: {payload.table_name}"
    )
    logger.info(
        f"Columns - x_axis: {payload.x_axis}, y_axis: {payload.y_axis}, dimension_col: {payload.dimension_col}, aggregate_col: {payload.aggregate_col}"
    )

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access denied to schema")

    # Check cache first
    query_hash = get_query_hash(payload)
    cached_snapshot = (
        ChartSnapshot.objects.filter(query_hash=query_hash, expires_at__gt=timezone.now())
        .order_by("-created_at")
        .first()
    )

    if cached_snapshot:
        logger.info(f"Using cached data for query hash: {query_hash}")
        return ChartDataResponse(
            data=cached_snapshot.data, echarts_config=cached_snapshot.echarts_config
        )

    # Build query based on computation type
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    warehouse = dbtautomation_service._get_wclient(org_warehouse)

    if payload.computation_type == "raw":
        # Build raw data query
        query_builder = QueryBuilder()
        query_builder.fetch_from(payload.table_name, payload.schema_name)

        # Add columns only if they exist
        if payload.x_axis:
            query_builder.add_column(payload.x_axis)
        if payload.y_axis:
            query_builder.add_column(payload.y_axis)

        if payload.extra_dimension:
            query_builder.add_column(payload.extra_dimension)

        # If no columns specified, raise error
        if not payload.x_axis and not payload.y_axis:
            raise HttpError(
                400, "At least one column (x_axis or y_axis) must be specified for raw data"
            )

    else:  # aggregated
        # Build aggregated query
        query_builder = AggQueryBuilder()
        query_builder.fetch_from(payload.table_name, payload.schema_name)
        query_builder.add_column(column(payload.dimension_col))
        query_builder.add_aggregate_column(
            payload.aggregate_col,
            payload.aggregate_func,
            f"{payload.aggregate_func}_{payload.aggregate_col}",
        )
        query_builder.group_cols_by(payload.dimension_col)

        if payload.extra_dimension:
            query_builder.add_column(column(payload.extra_dimension))
            query_builder.group_cols_by(payload.extra_dimension)

    # Add pagination
    query_builder.limit_rows(payload.limit)
    query_builder.offset_rows(payload.offset)

    # Execute query
    sql_stmt = query_builder.build()
    # Compile the SQLAlchemy statement to raw SQL string
    # Use the PostgreSQL dialect for proper compilation
    from sqlalchemy.dialects import postgresql

    compiled = sql_stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    sql = str(compiled)

    # Log the generated SQL for debugging
    logger.info(f"Generated SQL: {sql}")

    results = warehouse.execute(sql)

    # Convert tuple results to dictionaries
    # For aggregated queries, we need to map the result columns correctly
    dict_results = []
    if payload.computation_type == "raw":
        # For raw queries, columns are in the order they were added
        for row in results:
            row_dict = {}
            col_index = 0
            if payload.x_axis:
                row_dict[payload.x_axis] = row[col_index]
                col_index += 1
            if payload.y_axis:
                row_dict[payload.y_axis] = row[col_index]
                col_index += 1
            if payload.extra_dimension:
                row_dict[payload.extra_dimension] = row[col_index]
            dict_results.append(row_dict)
    else:  # aggregated
        # For aggregated queries, columns are: dimension_col, aggregate_result, [extra_dimension]
        for row in results:
            row_dict = {}
            col_index = 0
            row_dict[payload.dimension_col] = row[col_index]
            col_index += 1
            # The aggregate column name is formatted as func_column
            agg_col_name = f"{payload.aggregate_func}_{payload.aggregate_col}"
            row_dict[agg_col_name] = row[col_index]
            col_index += 1
            if payload.extra_dimension:
                row_dict[payload.extra_dimension] = row[col_index]
            dict_results.append(row_dict)

    # Transform data for chart
    chart_data = transform_data_for_chart(
        dict_results, payload.chart_type, payload.computation_type, payload
    )

    # Generate ECharts config
    config_generators = {
        "bar": EChartsConfigGenerator.generate_bar_config,
        "pie": EChartsConfigGenerator.generate_pie_config,
        "line": EChartsConfigGenerator.generate_line_config,
    }

    echarts_config = config_generators[payload.chart_type](chart_data, payload.customizations)

    # Cache the result (optional - you can make this configurable)
    # Note: This is a simplified caching mechanism. In production,
    # you might want to associate this with a specific chart ID

    return ChartDataResponse(data=chart_data, echarts_config=echarts_config)


@charts_router.post("/chart-data-preview/", response=DataPreviewResponse)
@has_permission(["can_view_warehouse_data"])
def get_chart_data_preview(request, payload: ChartDataPayload):
    """Get paginated data preview for chart"""
    orguser = request.orguser

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access denied to schema")

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    warehouse = dbtautomation_service._get_wclient(org_warehouse)

    # Build query to get column info
    columns_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{payload.schema_name}' 
        AND table_name = '{payload.table_name}'
        ORDER BY ordinal_position
    """
    column_info = warehouse.execute(columns_query)
    # Handle tuple results from warehouse.execute
    columns = [col[0] for col in column_info]  # column_name is first element
    column_types = {col[0]: col[1] for col in column_info}  # column_name: data_type

    # Build data query
    query_builder = QueryBuilder()
    query_builder.fetch_from(payload.table_name, payload.schema_name)

    # Add all columns
    for col in columns:
        query_builder.add_column(col)

    # Add pagination
    page_size = payload.limit
    page = (payload.offset // page_size) + 1
    query_builder.limit_rows(page_size)
    query_builder.offset_rows(payload.offset)

    # Execute query
    sql_stmt = query_builder.build()
    # Compile the SQLAlchemy statement to raw SQL string
    # Use the PostgreSQL dialect for proper compilation
    from sqlalchemy.dialects import postgresql

    compiled = sql_stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    sql = str(compiled)

    # Log the generated SQL for debugging
    logger.info(f"Generated SQL: {sql}")

    results = warehouse.execute(sql)

    # Convert tuple results to dictionaries
    data_dicts = []
    for row in results:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col] = row[i]
        data_dicts.append(row_dict)

    # Get total count
    count_query = f"""
        SELECT COUNT(*) as total 
        FROM {payload.schema_name}.{payload.table_name}
    """
    count_result = warehouse.execute(count_query)
    total_rows = count_result[0][0] if count_result else 0  # First element of first tuple

    return DataPreviewResponse(
        columns=columns,
        column_types=column_types,
        data=data_dicts,
        total_rows=total_rows,
        page=page,
        page_size=page_size,
    )


@charts_router.get("/{chart_id}/", response=ChartResponse)
@has_permission(["can_view_chart"])
def get_chart(request, chart_id: int):
    """Get a specific chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)
    return chart


@charts_router.get("/{chart_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_chart"])
def get_chart_data_by_id(request, chart_id: int):
    """Get chart data using saved chart configuration"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

    # Build payload from chart config
    config = chart.config
    payload = ChartDataPayload(
        chart_type=chart.chart_type,
        computation_type=chart.computation_type,
        schema_name=chart.schema_name,
        table_name=chart.table_name,
        x_axis=config.get("x_axis_column"),
        y_axis=config.get("y_axis_column"),
        dimension_col=config.get("dimension_column"),
        aggregate_col=config.get("aggregate_column"),
        aggregate_func=config.get("aggregate_function"),
        extra_dimension=config.get("extra_dimension_column"),
        customizations=config.get("customizations", {}),
        offset=0,
        limit=100,
    )

    # Use existing get_chart_data logic
    return get_chart_data(request, payload)


@charts_router.post("/", response=ChartResponse)
@has_permission(["can_create_chart"])
def create_chart(request, payload: ChartCreate):
    """Create a new chart"""
    orguser = request.orguser

    # Validate config structure
    config = payload.config
    if payload.computation_type == "raw":
        if not config.get("x_axis_column") and not config.get("y_axis_column"):
            raise HttpError(400, "At least one axis column must be specified for raw data")
    else:  # aggregated
        if (
            not config.get("dimension_column")
            or not config.get("aggregate_column")
            or not config.get("aggregate_function")
        ):
            raise HttpError(
                400, "Dimension, aggregate column and function are required for aggregated data"
            )

    chart = Chart.objects.create(
        title=payload.title,
        description=payload.description,
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        config=config,
        user=orguser,
        org=orguser.org,
    )
    return chart


@charts_router.put("/{chart_id}/", response=ChartResponse)
@has_permission(["can_edit_chart"])
def update_chart(request, chart_id: int, payload: ChartUpdate):
    """Update a chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

    if payload.title is not None:
        chart.title = payload.title
    if payload.description is not None:
        chart.description = payload.description
    if payload.config is not None:
        chart.config = payload.config
    if payload.is_favorite is not None:
        chart.is_favorite = payload.is_favorite

    chart.save()
    return chart


@charts_router.delete("/{chart_id}/")
@has_permission(["can_delete_chart"])
def delete_chart(request, chart_id: int):
    """Delete a chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)
    chart.delete()
    return {"success": True}
