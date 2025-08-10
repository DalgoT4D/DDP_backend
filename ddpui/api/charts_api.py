"""Chart API endpoints"""

from typing import Optional, List
from datetime import datetime

from django.shortcuts import get_object_or_404
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.core.charts import charts_service
from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator
from ddpui.core.charts.chart_validator import ChartValidator
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
    GeoJSONListResponse,
    GeoJSONDetailResponse,
)

logger = CustomLogger("ddpui")

charts_router = Router()


def has_schema_access(request, schema_name: str) -> bool:
    """Check if user has access to schema"""
    # TODO: Implement proper schema access control
    # For now, allow access to all schemas in the org
    return True


def generate_chart_render_config(chart: Chart, org_warehouse: OrgWarehouse) -> dict:
    """Generate ECharts render config from chart's extra_config"""
    logger.info(f"Generating render config for chart {chart.id}: {chart.title}")

    try:
        extra_config = chart.extra_config
        logger.debug(f"Chart {chart.id} extra_config: {extra_config}")

        payload = ChartDataPayload(
            chart_type=chart.chart_type,
            computation_type=chart.computation_type,
            schema_name=chart.schema_name,
            table_name=chart.table_name,
            x_axis=extra_config.get("x_axis_column"),
            y_axis=extra_config.get("y_axis_column"),
            dimension_col=extra_config.get("dimension_column"),
            aggregate_col=extra_config.get("aggregate_column"),
            aggregate_func=extra_config.get("aggregate_function"),
            extra_dimension=extra_config.get("extra_dimension_column"),
            customizations=extra_config.get("customizations", {}),
        )

        # Use the common function to generate config and data
        result = generate_chart_data_and_config(payload, org_warehouse, chart_id=chart.id)
        return result.get("echarts_config", {})

    except Exception as e:
        logger.error(f"Error generating render_config for chart {chart.id}: {str(e)}")
        return {}


def generate_chart_data_and_config(payload: ChartDataPayload, org_warehouse, chart_id=None) -> dict:
    """Generate chart data and ECharts config from payload"""
    chart_id_str = f"chart {chart_id}" if chart_id else "chart"

    logger.info(
        f"Building query for {chart_id_str} - Type: {payload.chart_type}, Computation: {payload.computation_type}"
    )

    # Handle maps differently
    if payload.chart_type == "map":
        return generate_map_data_and_config(payload, org_warehouse, chart_id)

    # Get warehouse client
    warehouse = charts_service.get_warehouse_client(org_warehouse)

    # Build query
    query_builder = charts_service.build_chart_query(payload)
    logger.debug(f"Query built for {chart_id_str}: {query_builder}")

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
    logger.info(f"Executing query for {chart_id_str}")
    dict_results = charts_service.execute_chart_query(warehouse, query_builder, execute_payload)
    logger.debug(f"Query results for {chart_id_str}: {len(dict_results)} rows")

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
        "number": EChartsConfigGenerator.generate_number_config,
    }

    logger.info(f"Generating ECharts config for {chart_id_str} with type {payload.chart_type}")
    echarts_config = config_generators[payload.chart_type](chart_data, payload.customizations)

    logger.info(f"Successfully generated data and config for {chart_id_str}")

    return {"data": chart_data, "echarts_config": echarts_config}


def generate_map_data_and_config(payload: ChartDataPayload, org_warehouse, chart_id=None) -> dict:
    """Generate map data and ECharts config from payload"""
    chart_id_str = f"map {chart_id}" if chart_id else "map"

    logger.info(f"Generating map data for {chart_id_str}")

    # Import map services
    from ddpui.core.charts.maps_service import build_map_query, transform_data_for_map
    from ddpui.models.geojson import GeoJSON

    # Get GeoJSON data
    geojson_id = payload.selected_geojson_id
    if not geojson_id:
        raise ValueError("Map requires selected_geojson_id")

    geojson = get_object_or_404(GeoJSON, id=geojson_id)

    # Get warehouse client and build query
    warehouse = charts_service.get_warehouse_client(org_warehouse)
    query_builder = build_map_query(payload)

    # Execute query
    logger.info(f"Executing map query for {chart_id_str}")
    dict_results = charts_service.execute_query(warehouse, query_builder)
    logger.info(f"Map query results for {chart_id_str}: {len(dict_results)} rows")

    # Transform for map
    map_data = transform_data_for_map(
        dict_results,
        geojson.geojson_data,
        payload.geographic_column,
        payload.value_column,
        payload.aggregate_func or "sum",
        payload.customizations,
    )

    # Generate map config
    echarts_config = EChartsConfigGenerator.generate_map_config(map_data, payload.customizations)

    logger.info(f"Successfully generated map data and config for {chart_id_str}")

    return {"data": map_data, "echarts_config": echarts_config}


@charts_router.get("/", response=List[ChartResponse])
def list_charts(request):
    """List all charts for the organization"""
    orguser = request.orguser
    charts = Chart.objects.filter(org=orguser.org).order_by("-updated_at")

    # Get org warehouse once for all charts
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.id}")

    # Generate render_config for each chart
    chart_responses = []
    for chart in charts:
        chart_dict = {
            "id": chart.id,
            "title": chart.title,
            "description": chart.description,
            "chart_type": chart.chart_type,
            "computation_type": chart.computation_type,
            "schema_name": chart.schema_name,
            "table_name": chart.table_name,
            "extra_config": chart.extra_config,
            "render_config": (
                generate_chart_render_config(chart, org_warehouse) if org_warehouse else {}
            ),
            "created_at": chart.created_at,
            "updated_at": chart.updated_at,
        }
        chart_responses.append(ChartResponse(**chart_dict))

    return chart_responses


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

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Use the common function to generate data and config
    try:
        result = generate_chart_data_and_config(payload, org_warehouse)
        return ChartDataResponse(data=result["data"], echarts_config=result["echarts_config"])
    except ValueError as e:
        raise HttpError(400, str(e))
    except Exception as e:
        logger.error(f"Error generating chart data: {str(e)}")
        raise HttpError(500, "Error generating chart data")


@charts_router.post("/chart-data-preview/", response=DataPreviewResponse)
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


# Map-specific endpoints - Must come before /{chart_id}/ routes to avoid conflicts


@charts_router.get("/geojsons/", response=List[GeoJSONListResponse])
def list_available_geojsons(request, country_code: str = "IND", layer_level: int = 1):
    """List available GeoJSONs for a country and layer"""
    orguser = request.orguser

    from ddpui.core.charts.maps_service import get_available_geojsons

    geojsons = get_available_geojsons(country_code, layer_level, orguser.org.id)

    return [GeoJSONListResponse(**geojson) for geojson in geojsons]


@charts_router.get("/geojsons/{geojson_id}/", response=GeoJSONDetailResponse)
def get_geojson_data(request, geojson_id: int):
    """Get specific GeoJSON data"""
    orguser = request.orguser

    from ddpui.models.geojson import GeoJSON

    geojson = get_object_or_404(GeoJSON, id=geojson_id)

    # Check access permissions
    if not geojson.is_default and geojson.org_id != orguser.org.id:
        raise HttpError(403, "Access denied to GeoJSON")

    return GeoJSONDetailResponse(
        id=geojson.id,
        name=geojson.name,
        display_name=geojson.display_name,
        geojson_data=geojson.geojson_data,
        properties_key=geojson.properties_key,
    )


@charts_router.get("/{chart_id}/", response=ChartResponse)
def get_chart(request, chart_id: int):
    """Get a specific chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.id}")

    # Build response with render_config
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        "render_config": (
            generate_chart_render_config(chart, org_warehouse) if org_warehouse else {}
        ),
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.get("/{chart_id}/data/", response=ChartDataResponse)
def get_chart_data_by_id(request, chart_id: int):
    """Get chart data using saved chart configuration"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Build payload from chart config
    extra_config = chart.extra_config
    payload = ChartDataPayload(
        chart_type=chart.chart_type,
        computation_type=chart.computation_type,
        schema_name=chart.schema_name,
        table_name=chart.table_name,
        x_axis=extra_config.get("x_axis_column"),
        y_axis=extra_config.get("y_axis_column"),
        dimension_col=extra_config.get("dimension_column"),
        aggregate_col=extra_config.get("aggregate_column"),
        aggregate_func=extra_config.get("aggregate_function"),
        extra_dimension=extra_config.get("extra_dimension_column"),
        customizations=extra_config.get("customizations", {}),
        offset=0,
        limit=100,
    )

    # Use the common function to generate data and config
    try:
        result = generate_chart_data_and_config(payload, org_warehouse, chart_id=chart.id)
        return ChartDataResponse(data=result["data"], echarts_config=result["echarts_config"])
    except ValueError as e:
        raise HttpError(400, str(e))
    except Exception as e:
        logger.error(f"Error generating chart data for chart {chart.id}: {str(e)}")
        raise HttpError(500, "Error generating chart data")


@charts_router.post("/", response=ChartResponse)
def create_chart(request, payload: ChartCreate):
    """Create a new chart"""
    orguser = request.orguser

    # Validate chart configuration using ChartValidator
    is_valid, error_message = ChartValidator.validate_chart_config(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        extra_config=payload.extra_config,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
    )

    if not is_valid:
        raise HttpError(400, error_message)

    chart = Chart.objects.create(
        title=payload.title,
        description=payload.description,
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        extra_config=payload.extra_config,
        created_by=orguser,
        last_modified_by=orguser,
        org=orguser.org,
    )

    # Get org warehouse for render_config generation
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.id}")

    # Build response with render_config
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        "render_config": (
            generate_chart_render_config(chart, org_warehouse) if org_warehouse else {}
        ),
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.put("/{chart_id}/", response=ChartResponse)
def update_chart(request, chart_id: int, payload: ChartUpdate):
    """Update a chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)

    # Prepare the updated values
    updated_chart_type = payload.chart_type if payload.chart_type is not None else chart.chart_type
    updated_computation_type = (
        payload.computation_type if payload.computation_type is not None else chart.computation_type
    )
    updated_extra_config = (
        payload.extra_config if payload.extra_config is not None else chart.extra_config
    )
    updated_schema_name = (
        payload.schema_name if payload.schema_name is not None else chart.schema_name
    )
    updated_table_name = payload.table_name if payload.table_name is not None else chart.table_name

    # Validate the updated configuration
    is_valid, error_message = ChartValidator.validate_for_update(
        existing_chart_type=chart.chart_type,
        new_chart_type=payload.chart_type,
        new_computation_type=payload.computation_type,
        extra_config=updated_extra_config,
        schema_name=updated_schema_name,
        table_name=updated_table_name,
    )

    if not is_valid:
        raise HttpError(400, error_message)

    # Apply updates
    if payload.title is not None:
        chart.title = payload.title
    if payload.description is not None:
        chart.description = payload.description
    if payload.chart_type is not None:
        chart.chart_type = payload.chart_type
    if payload.computation_type is not None:
        chart.computation_type = payload.computation_type
    if payload.schema_name is not None:
        chart.schema_name = payload.schema_name
    if payload.table_name is not None:
        chart.table_name = payload.table_name
    if payload.extra_config is not None:
        chart.extra_config = payload.extra_config

    # Update last_modified_by
    chart.last_modified_by = orguser

    chart.save()

    # Get org warehouse for render_config generation
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.id}")

    # Build response with render_config
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        "render_config": (
            generate_chart_render_config(chart, org_warehouse) if org_warehouse else {}
        ),
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.delete("/{chart_id}/")
def delete_chart(request, chart_id: int):
    """Delete a chart"""
    orguser = request.orguser
    chart = get_object_or_404(Chart, id=chart_id, org=orguser.org)
    chart.delete()
    return {"success": True}
