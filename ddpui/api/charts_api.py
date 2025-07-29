"""Chart API endpoints"""

from typing import Optional, List
from datetime import datetime

from django.shortcuts import get_object_or_404
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart, ChartSnapshot
from ddpui.core.charts import charts_service
from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.chart_schema import (
    ChartCreate,
    ChartUpdate,
    ChartResponse,
    ChartDataPayload,
    ChartDataResponse,
    DataPreviewResponse,
    ExecuteChartQuery,
    TransformDataForChart,
)

logger = CustomLogger("ddpui")

charts_router = Router()


def has_schema_access(request, schema_name: str) -> bool:
    """Check if user has access to schema"""
    # TODO: Implement proper schema access control
    # For now, allow access to all schemas in the org
    return True


@charts_router.get("/", response=List[ChartResponse])
# @has_permission(["can_view_chart"])
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
    query_hash = charts_service.get_query_hash(
        payload.chart_type,
        payload.computation_type,
        payload.schema_name,
        payload.table_name,
        payload.x_axis,
        payload.y_axis,
        payload.dimension_col,
        payload.aggregate_col,
        payload.aggregate_func,
        payload.extra_dimension,
        payload.offset,
        payload.limit,
    )

    cached_data = charts_service.get_cached_data(query_hash)
    if cached_data:
        return ChartDataResponse(data=cached_data[0], echarts_config=cached_data[1])

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Get warehouse client
    warehouse = charts_service.get_warehouse_client(org_warehouse)

    # Build query
    try:
        query_builder = charts_service.build_chart_query(payload)
    except ValueError as e:
        raise HttpError(400, str(e))

    execute_payload = ExecuteChartQuery(
        computation_type=payload.computation_type,
        x_axis=payload.x_axis,
        y_axis=payload.y_axis,
        dimension_col=payload.dimension_col,
        aggregate_col=payload.aggregate_col,
        aggregate_func=payload.aggregate_func,
        extra_dimension=payload.extra_dimension,
    )

    # Execute query
    dict_results = charts_service.execute_chart_query(warehouse, query_builder, execute_payload)

    # Transform data for chart
    transform_payload = TransformDataForChart(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        x_axis=payload.x_axis,
        y_axis=payload.y_axis,
        dimension_col=payload.dimension_col,
        aggregate_col=payload.aggregate_col,
        aggregate_func=payload.aggregate_func,
        extra_dimension=payload.extra_dimension,
        customizations=payload.customizations,
    )
    chart_data = charts_service.transform_data_for_chart(dict_results, transform_payload)

    # Generate ECharts config
    config_generators = {
        "bar": EChartsConfigGenerator.generate_bar_config,
        "pie": EChartsConfigGenerator.generate_pie_config,
        "line": EChartsConfigGenerator.generate_line_config,
    }

    echarts_config = config_generators[payload.chart_type](chart_data, payload.customizations)

    return ChartDataResponse(data=chart_data, echarts_config=echarts_config)


@charts_router.post("/chart-data-preview/", response=DataPreviewResponse)
# @has_permission(["can_view_warehouse_data"])
def get_chart_data_preview(request, payload: ChartDataPayload):
    """Get paginated data preview for chart using the same query as chart data"""
    orguser = request.orguser

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access denied to schema")

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Get table preview using the same query builder as chart data
    # This ensures preview shows exactly what will be used for the chart
    preview_data = charts_service.get_chart_data_table_preview(org_warehouse, payload)

    return DataPreviewResponse(
        columns=preview_data["columns"],
        column_types=preview_data["column_types"],
        data=preview_data["data"],
        total_rows=preview_data["total_rows"],
        page=preview_data["page"],
        page_size=preview_data["page_size"],
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
