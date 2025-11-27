"""Chart API endpoints"""

from typing import Optional, List, Dict, Any
import copy
import csv
from datetime import datetime
from io import StringIO

from ninja import Router, Schema, Field
from ninja.errors import HttpError
from django.shortcuts import get_object_or_404
from django.http import StreamingHttpResponse

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.dashboard import DashboardFilter
from ddpui.models.visualization import Chart
from ddpui.core.charts import charts_service
from ddpui.core.charts.echarts_config_generator import EChartsConfigGenerator
from ddpui.core.charts.chart_validator import ChartValidator
from ddpui.utils.custom_logger import CustomLogger
from ddpui.schemas.chart_schema import (
    ChartCreate,
    ChartMetric,
    ChartUpdate,
    ChartResponse,
    ChartDataPayload,
    ChartDataResponse,
    DataPreviewResponse,
    ExecuteChartQuery,
    TransformDataForChart,
    GeoJSONDetailResponse,
    GeoJSONListResponse,
    GeoJSONUpload,
)
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory

logger = CustomLogger("ddpui")

charts_router = Router()


# Schema for bulk delete
class BulkDeleteRequest(Schema):
    chart_ids: List[int]


# Map-related endpoints - Must come before parameterized routes
@charts_router.get("/regions/export-names/", response=List[dict])
@has_permission(["can_view_charts"])
def export_region_names(request, country_code: str, region_type: str):
    """Export region names as JSON for a specific country

    Args:
        country_code: ISO country code (e.g., 'IND', 'KEN')
        region_type: Type of region ('state' or 'district')

    Returns:
        JSON array with region data
    """
    from ddpui.core.charts.maps_service import export_region_names_json

    # Validate query parameters
    if not country_code:
        raise HttpError(400, "country_code is required")

    if not region_type:
        raise HttpError(400, "region_type is required")

    if region_type not in ["state", "district"]:
        raise HttpError(400, "region_type must be 'state' or 'district'")

    try:
        # Generate JSON data with backend filtering by country
        data = export_region_names_json(country_code, region_type)

        logger.info(f"Exported {len(data)} {region_type}s for country {country_code}")

        return data

    except ValueError as e:
        logger.error(f"Export error: {str(e)}")
        raise HttpError(404, str(e))
    except Exception as e:
        logger.error(f"Unexpected error during export: {str(e)}")
        raise HttpError(500, f"Failed to export region names: {str(e)}")


@charts_router.get("/regions/{region_id}/geojsons/", response=List[dict])
@has_permission(["can_view_charts"])
def get_geojsons_for_region(request, region_id: int):
    """Get available GeoJSONs for a specific region"""
    orguser = request.orguser

    from ddpui.core.charts.maps_service import get_available_geojsons_for_region

    geojsons = get_available_geojsons_for_region(region_id, orguser.org.id)
    return geojsons


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
            extra_dimension=extra_config.get("extra_dimension_column"),
            # Map-specific fields
            geographic_column=extra_config.get("geographic_column"),
            value_column=extra_config.get("value_column"),
            selected_geojson_id=extra_config.get("selected_geojson_id"),
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
    query_builder = charts_service.build_chart_query(payload, org_warehouse)
    logger.debug(f"Query built for {chart_id_str}: {query_builder}")

    execute_payload = ExecuteChartQuery(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        x_axis=payload.x_axis,
        y_axis=payload.y_axis,
        dimension_col=payload.dimension_col,
        extra_dimension=payload.extra_dimension,
        metrics=payload.metrics,
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
        extra_dimension=payload.extra_dimension,
        customizations=payload.customizations,
        metrics=payload.metrics,
    )
    chart_data = charts_service.transform_data_for_chart(dict_results, transform_payload)

    # Table charts don't need ECharts config, just return the data
    if payload.chart_type == "table":
        logger.info(f"Successfully generated data for table {chart_id_str}")
        return {"data": chart_data, "echarts_config": {}}

    # Generate ECharts config for other chart types
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
    query_builder = build_map_query(payload, org_warehouse=org_warehouse)

    # Execute query
    logger.info(f"Executing map query for {chart_id_str}")
    dict_results = charts_service.execute_query(warehouse, query_builder)
    logger.info(f"Map query results for {chart_id_str}: {len(dict_results)} rows")

    # Transform for map - support both multiple metrics and legacy single metric
    selected_metric_index = (
        payload.customizations.get("selected_metric_index", 0) if payload.customizations else 0
    )
    map_data = transform_data_for_map(
        dict_results,
        geojson.geojson_data,
        payload.geographic_column,
        payload.value_column,
        None,  # aggregate_func removed - using metrics
        payload.customizations,
        payload.metrics,
        selected_metric_index,
    )

    # Generate map config
    echarts_config = EChartsConfigGenerator.generate_map_config(map_data, payload.customizations)

    logger.info(f"Successfully generated map data and config for {chart_id_str}")

    return {"data": map_data, "echarts_config": echarts_config}


class ChartListResponse(Schema):
    """Paginated chart list response"""

    data: List[ChartResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


@charts_router.get("/", response=ChartListResponse)
@has_permission(["can_view_charts"])
def list_charts(
    request, page: int = 1, page_size: int = 10, search: str = None, chart_type: str = None
):
    """List charts for the organization with pagination and filtering"""
    orguser: OrgUser = request.orguser

    # Validate pagination parameters
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10

    # Start with all charts for the organization
    queryset = Chart.objects.filter(org=orguser.org)

    # Apply search filter
    if search:
        from django.db.models import Q

        queryset = queryset.filter(
            Q(title__icontains=search)
            | Q(description__icontains=search)
            | Q(schema_name__icontains=search)
            | Q(table_name__icontains=search)
        )

    # Apply chart type filter
    if chart_type and chart_type != "all":
        queryset = queryset.filter(chart_type=chart_type)

    # Order by updated_at descending
    queryset = queryset.order_by("-updated_at")

    # Get total count before pagination
    total = queryset.count()
    total_pages = (total + page_size - 1) // page_size  # Ceiling division

    # Apply pagination
    offset = (page - 1) * page_size
    charts = queryset[offset : offset + page_size]

    # Get org warehouse once for all charts
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.slug}")

    # Build response for each chart
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
            # render_config removed - charts fetch fresh config via /data endpoint
            "created_at": chart.created_at,
            "updated_at": chart.updated_at,
        }
        chart_responses.append(ChartResponse(**chart_dict))

    return ChartListResponse(
        data=chart_responses, total=total, page=page, page_size=page_size, total_pages=total_pages
    )


# New endpoints for separated data fetching (place before parametrized routes)


@charts_router.get("/available-layers/", response=List[dict])
@has_permission(["can_view_charts"])
def list_available_layers(request, layer_type: str = "country"):
    """Get available layers (countries, states, districts, etc.) dynamically from the database"""
    try:
        from ddpui.models.georegion import GeoRegion

        # Get regions of specified type
        regions = GeoRegion.objects.filter(type=layer_type).values(
            "id", "region_code", "name", "display_name", "parent_id"
        )

        result = []
        for region in regions:
            result.append(
                {
                    "id": region["id"],
                    "code": region["region_code"],
                    "name": region["name"],
                    "display_name": region["display_name"],
                    "type": layer_type,
                    "parent_id": region["parent_id"],
                }
            )

        return result
    except Exception as e:
        logger.error(f"Error fetching {layer_type} layers: {str(e)}")
        # Fallback based on layer type
        if layer_type == "country":
            return [
                {
                    "id": 1,
                    "code": "IND",
                    "name": "India",
                    "display_name": "India",
                    "type": "country",
                    "parent_id": None,
                }
            ]
        return []


class MapDataOverlayPayload(Schema):
    schema_name: str
    table_name: str
    geographic_column: str
    value_column: str
    metrics: List[ChartMetric]
    filters: Dict[str, Any] = Field(default_factory=dict)  # Drill-down filters (key-value pairs)
    dashboard_filters: Optional[dict[str, Any]] = Field(
        default_factory=dict
    )  # Dashboard-level filters (dictionary of filter objects)
    extra_config: Optional[Dict[str, Any]] = Field(
        default_factory=dict
    )  # Additional configuration including chart-level filters, pagination, sorting, etc.


@charts_router.post("/map-data-overlay/", response=dict)
@has_permission(["can_view_warehouse_data"])
def get_map_data_overlay(request, payload: MapDataOverlayPayload):
    """Get map data overlay (separate from GeoJSON) for data visualization"""
    orguser = request.orguser

    logger.info(f"Map data overlay request: {payload}")

    try:
        # Get org warehouse
        org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        if not org_warehouse:
            raise HttpError(404, "Warehouse not configured")

        warehouse_client = charts_service.get_warehouse_client(org_warehouse)

        # Extract required fields from payload
        schema_name = payload.schema_name
        table_name = payload.table_name
        geographic_column = payload.geographic_column
        value_column = payload.value_column
        # Use first metric for map overlay
        filters = payload.filters

        # Validate required fields
        if not all([schema_name, table_name, geographic_column, value_column]):
            raise HttpError(
                400,
                "Missing required fields: schema_name, table_name, geographic_column, value_column",
            )

        # Validate metrics exist and are non-empty
        if not payload.metrics:
            raise HttpError(400, "Missing metrics - at least one metric is required")

        # Build payload for standard chart query (same as other charts)
        # Make a deep copy to avoid mutating the original payload
        # extra_config already contains chart-level filters in extra_config.filters
        extra_config = copy.deepcopy(payload.extra_config or {})

        # Use metrics from payload directly
        metrics = payload.metrics
        dashboard_filters = payload.dashboard_filters

        # Resolve dashboard filters if provided (same logic as regular charts)
        resolved_dashboard_filters = None
        if dashboard_filters:
            try:
                # Import dashboard filter model
                from ddpui.models.dashboard import DashboardFilter

                resolved_filters = []

                for filter_id, filter_value in dashboard_filters.items():
                    if filter_id and filter_value is not None:
                        try:
                            # Get the filter configuration from the database
                            dashboard_filter = DashboardFilter.objects.get(id=filter_id)

                            # Only apply this filter if it applies to the same table as the chart
                            if warehouse_client.column_exists(
                                schema_name, table_name, dashboard_filter.column_name
                            ):
                                resolved_filters.append(
                                    {
                                        "filter_id": filter_id,
                                        "column": dashboard_filter.column_name,
                                        "type": dashboard_filter.filter_type,
                                        "value": filter_value,
                                        "settings": dashboard_filter.settings,
                                    }
                                )
                        except DashboardFilter.DoesNotExist:
                            logger.warning(f"Dashboard filter {filter_id} not found")

                resolved_dashboard_filters = resolved_filters

            except Exception as e:
                logger.error(f"Error resolving dashboard filters: {str(e)}")
                resolved_dashboard_filters = None

        chart_payload = ChartDataPayload(
            chart_type="bar",  # We use bar chart query logic for aggregated data
            computation_type="aggregated",
            schema_name=schema_name,
            table_name=table_name,
            dimension_col=geographic_column,
            metrics=metrics,
            dashboard_filters=resolved_dashboard_filters,
            extra_config=extra_config,
        )

        # Get warehouse client and build query using standard chart service
        query_builder = charts_service.build_chart_query(chart_payload, org_warehouse)

        # Add filters if provided with case-insensitive matching
        if filters:
            from sqlalchemy import column, func

            for filter_column, filter_value in filters.items():
                # Use case-insensitive matching for string filters
                # Convert both database column and filter value to uppercase for comparison
                query_builder.where_clause(
                    func.upper(column(filter_column)) == str(filter_value).upper()
                )

        # Execute query using standard chart service
        execute_payload = ExecuteChartQuery(
            chart_type="map",
            computation_type="aggregated",
            dimension_col=geographic_column,
            metrics=metrics,
        )

        dict_results = charts_service.execute_chart_query(
            warehouse_client, query_builder, execute_payload
        )

        logger.info(f"Map data overlay query returned {len(dict_results)} rows")

        # Transform results for map visualization with proper case normalization
        # The standard chart query returns data with dimension and aggregate columns
        map_data = []
        for row in dict_results:
            # Get the dimension value (geographic region name)
            region_name = row.get(geographic_column)
            # Get the aggregated value using the metric alias
            metric_alias = metrics[0].alias or f"{metrics[0].aggregation}_{metrics[0].column}"
            value = row.get(metric_alias)

            if region_name and value is not None:
                # Normalize region name to proper case for frontend compatibility
                # Convert "MAHARASHTRA" -> "Maharashtra", "gujarat" -> "Gujarat"
                normalized_name = str(region_name).strip().title()
                map_data.append({"name": normalized_name, "value": float(value)})

        return {"success": True, "data": map_data, "count": len(map_data)}

    except Exception as e:
        logger.error(f"Error generating map data overlay: {str(e)}")
        raise HttpError(500, f"Error generating map data overlay: {str(e)}")


@charts_router.post("/chart-data/", response=ChartDataResponse)
@has_permission(["can_view_charts"])
def get_chart_data(request, payload: ChartDataPayload):
    """Get chart data with ECharts configuration"""
    orguser = request.orguser

    # Log the incoming payload for debugging
    logger.info(
        f"Chart data request - Type: {payload.computation_type}, Schema: {payload.schema_name}, Table: {payload.table_name}"
    )
    logger.info(
        f"Columns - x_axis: {payload.x_axis}, y_axis: {payload.y_axis}, dimension_col: {payload.dimension_col}, metrics: {payload.metrics}"
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
@has_permission(["can_view_charts"])
def get_chart_data_preview(
    request,
    payload: ChartDataPayload,
    page: int = 0,
    limit: int = 100,
    dashboard_filters: Optional[str] = None,
):
    """Get paginated data preview for chart using the same query as chart data"""
    import json

    orguser = request.orguser

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access denied to schema")

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Parse and resolve dashboard filters if provided (same logic as chart data endpoint)
    resolved_dashboard_filters = None
    if dashboard_filters:
        try:
            filter_values = json.loads(dashboard_filters)
            logger.info(f"Applying dashboard filters to chart data preview: {filter_values}")

            # Resolve filter configurations to get column information

            # Get warehouse client to check column existence
            warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)
            resolved_filters = []

            for filter_id, filter_value in filter_values.items():
                if filter_value is not None:
                    try:
                        # Get the filter configuration from the database
                        dashboard_filter = DashboardFilter.objects.get(id=filter_id)

                        # Only apply this filter if it applies to the same table as the chart
                        if warehouse_client.column_exists(
                            payload.schema_name, payload.table_name, dashboard_filter.column_name
                        ):
                            resolved_filters.append(
                                {
                                    "filter_id": filter_id,
                                    "column": dashboard_filter.column_name,
                                    "type": dashboard_filter.filter_type,
                                    "value": filter_value,
                                    "settings": dashboard_filter.settings,
                                }
                            )
                    except DashboardFilter.DoesNotExist:
                        logger.warning(f"Dashboard filter {filter_id} not found")

            resolved_dashboard_filters = resolved_filters

        except json.JSONDecodeError:
            logger.error(f"Invalid dashboard_filters JSON: {dashboard_filters}")
            resolved_dashboard_filters = None

    # Create a modified payload with dashboard filters
    modified_payload = ChartDataPayload(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        x_axis=payload.x_axis,
        y_axis=payload.y_axis,
        dimension_col=payload.dimension_col,
        extra_dimension=payload.extra_dimension,
        metrics=payload.metrics,
        geographic_column=payload.geographic_column,
        value_column=payload.value_column,
        selected_geojson_id=payload.selected_geojson_id,
        customizations=payload.customizations,
        offset=payload.offset,
        limit=payload.limit,
        extra_config=payload.extra_config,
        dashboard_filters=resolved_dashboard_filters,  # Add resolved dashboard filters
    )

    # Get table preview using the same query builder as chart data
    # This ensures preview shows exactly what will be used for the chart
    preview_data = charts_service.get_chart_data_table_preview(
        org_warehouse, modified_payload, page, limit
    )

    return DataPreviewResponse(
        columns=preview_data["columns"],
        column_types=preview_data["column_types"],
        data=preview_data["data"],
        page=preview_data["page"],
        limit=preview_data["limit"],
    )


@charts_router.post("/chart-data-preview/total-rows/", response=int)
@has_permission(["can_view_charts"])
def get_chart_data_preview_total_rows(
    request, payload: ChartDataPayload, dashboard_filters: Optional[str] = None
):
    """Get total rows for chart data preview"""
    import json

    orguser = request.orguser

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access denied to schema")

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Parse and resolve dashboard filters if provided (same logic as chart data endpoint)
    resolved_dashboard_filters = None
    if dashboard_filters:
        try:
            filter_values = json.loads(dashboard_filters)
            logger.info(
                f"Applying dashboard filters to chart data preview total rows: {filter_values}"
            )

            # Resolve filter configurations to get column information
            from ddpui.models.dashboard import DashboardFilter

            # Get warehouse client to check column existence
            warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)
            resolved_filters = []

            for filter_id, filter_value in filter_values.items():
                if filter_value is not None:
                    try:
                        # Get the filter configuration from the database
                        dashboard_filter = DashboardFilter.objects.get(id=filter_id)

                        # Only apply this filter if it applies to the same table as the chart
                        if warehouse_client.column_exists(
                            payload.schema_name, payload.table_name, dashboard_filter.column_name
                        ):
                            resolved_filters.append(
                                {
                                    "filter_id": filter_id,
                                    "column": dashboard_filter.column_name,
                                    "type": dashboard_filter.filter_type,
                                    "value": filter_value,
                                    "settings": dashboard_filter.settings,
                                }
                            )
                    except DashboardFilter.DoesNotExist:
                        logger.warning(f"Dashboard filter {filter_id} not found")

            resolved_dashboard_filters = resolved_filters

        except json.JSONDecodeError:
            logger.error(f"Invalid dashboard_filters JSON: {dashboard_filters}")
            resolved_dashboard_filters = None

    # Create a modified payload with dashboard filters
    modified_payload = ChartDataPayload(
        chart_type=payload.chart_type,
        computation_type=payload.computation_type,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        x_axis=payload.x_axis,
        y_axis=payload.y_axis,
        dimension_col=payload.dimension_col,
        extra_dimension=payload.extra_dimension,
        metrics=payload.metrics,
        geographic_column=payload.geographic_column,
        value_column=payload.value_column,
        selected_geojson_id=payload.selected_geojson_id,
        customizations=payload.customizations,
        offset=payload.offset,
        limit=payload.limit,
        extra_config=payload.extra_config,
        dashboard_filters=resolved_dashboard_filters,  # Add resolved dashboard filters
    )

    # Get total rows using the same query builder as chart data
    total_rows = charts_service.get_chart_data_total_rows(org_warehouse, modified_payload)

    return total_rows


# Map-specific endpoints - Must come before /{chart_id}/ routes to avoid conflicts


@charts_router.get("/regions/", response=List[dict])
@has_permission(["can_view_charts"])
def list_available_regions(request, country_code: str = "IND", region_type: str = None):
    """List available regions for a country"""
    from ddpui.core.charts.maps_service import get_available_regions

    regions = get_available_regions(country_code, region_type)
    return regions


@charts_router.get("/regions/{region_id}/children/", response=List[dict])
@has_permission(["can_view_charts"])
def get_child_regions(request, region_id: int):
    """Get child regions for a parent region"""
    from ddpui.core.charts.maps_service import get_child_regions

    children = get_child_regions(region_id)
    return children


@charts_router.post("/geojsons/upload/", response=GeoJSONDetailResponse)
@has_permission(["can_create_charts"])
def upload_geojson(request, payload: GeoJSONUpload):
    """Upload a custom GeoJSON for an organization"""
    orguser = request.orguser

    from ddpui.models.geojson import GeoJSON
    from ddpui.models.georegion import GeoRegion
    import json

    # Validate that the region exists
    region = get_object_or_404(GeoRegion, id=payload.region_id)

    # Validate GeoJSON format
    try:
        if not isinstance(payload.geojson_data, dict):
            raise HttpError(400, "Invalid GeoJSON format: must be a JSON object")

        if payload.geojson_data.get("type") != "FeatureCollection":
            raise HttpError(400, "Invalid GeoJSON format: must be a FeatureCollection")

        features = payload.geojson_data.get("features", [])
        if not features:
            raise HttpError(400, "Invalid GeoJSON: no features found")

        # Validate that all features have the specified properties_key
        for i, feature in enumerate(features):
            properties = feature.get("properties", {})
            if payload.properties_key not in properties:
                raise HttpError(
                    400, f"Feature {i+1} missing required property: {payload.properties_key}"
                )

    except Exception as e:
        logger.error(f"GeoJSON validation error: {str(e)}")
        raise HttpError(400, f"Invalid GeoJSON: {str(e)}")

    # Create the GeoJSON record
    geojson = GeoJSON.objects.create(
        region=region,
        geojson_data=payload.geojson_data,
        properties_key=payload.properties_key,
        is_default=False,  # Custom uploads are never default
        org=orguser.org,
        name=payload.name,
        description=payload.description,
    )

    logger.info(f"Created custom GeoJSON {geojson.id} for org {orguser.org.id}")

    return GeoJSONDetailResponse(
        id=geojson.id,
        name=geojson.name,
        display_name=f"{geojson.name} ({geojson.description or 'No description'})",
        geojson_data=geojson.geojson_data,
        properties_key=geojson.properties_key,
    )


@charts_router.get("/geojsons/{geojson_id}/", response=GeoJSONDetailResponse)
@has_permission(["can_view_charts"])
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
        display_name=f"{geojson.name} ({geojson.description or 'No description'})",
        geojson_data=geojson.geojson_data,
        properties_key=geojson.properties_key,
    )


@charts_router.post("/map-data/", response=dict)
@has_permission(["can_view_charts"])
def generate_map_chart_data(request, payload: ChartDataPayload):
    """Generate map chart data - simplified for basic map functionality"""
    orguser = request.orguser

    # Import map services
    from ddpui.core.charts.maps_service import build_map_query, transform_data_for_map
    from ddpui.models.geojson import GeoJSON

    # Get GeoJSON data
    geojson_id = payload.selected_geojson_id
    if not geojson_id:
        raise HttpError(400, "Map requires selected_geojson_id")

    geojson = get_object_or_404(GeoJSON, id=geojson_id)

    # Check access permissions
    if not geojson.is_default and geojson.org_id != orguser.org.id:
        raise HttpError(403, "Access denied to GeoJSON")

    # Get warehouse and build query using existing service
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(400, "No warehouse configured for this organization")

    # Get warehouse client
    warehouse = charts_service.get_warehouse_client(org_warehouse)

    # Build query using existing service
    query_builder = build_map_query(payload, org_warehouse=org_warehouse)

    # Execute query using existing service method
    logger.info(f"Executing map query for geojson_id: {geojson_id}")
    dict_results = charts_service.execute_query(warehouse, query_builder)
    logger.info(f"Map query results: {len(dict_results)} rows")

    # Transform data for map visualization - support both multiple metrics and legacy single metric
    selected_metric_index = (
        payload.customizations.get("selected_metric_index", 0) if payload.customizations else 0
    )
    map_data = transform_data_for_map(
        dict_results,
        geojson.geojson_data,
        payload.geographic_column,
        payload.value_column,
        None,  # aggregate_func removed - using metrics
        payload.customizations or {},
        payload.metrics,
        selected_metric_index,
    )

    logger.info(
        f"Map data transformed successfully with {map_data['matched_regions']}/{map_data['total_regions']} regions matched"
    )

    return {
        "data": map_data["data"],
        "geojson": map_data["geojson"],
        "min_value": map_data["min_value"],
        "max_value": map_data["max_value"],
        "matched_regions": map_data["matched_regions"],
        "total_regions": map_data["total_regions"],
    }


def stream_chart_data_csv(org_warehouse, payload: ChartDataPayload, page_size=5000):
    """
    Common function to stream chart data as CSV

    This function is used by both authenticated and public CSV download endpoints.
    It generates CSV data in chunks by paginating through the chart data.

    Args:
        org_warehouse: OrgWarehouse instance for database connection
        payload: ChartDataPayload containing chart configuration and filters
        page_size: Number of rows to fetch per page (default 5000)

    Yields:
        CSV data chunks as strings
    """
    page = 0
    output = StringIO()

    try:
        # Fetch first page
        preview_data = charts_service.get_chart_data_table_preview(
            org_warehouse, payload, page=page, limit=page_size
        )
        data = preview_data["data"]
        columns = preview_data["columns"]

        if not columns:
            logger.warning("No columns found in chart data")
            return

        # Create CSV writer and write headers immediately
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()

        # Yield header
        yield output.getvalue()
        output.truncate(0)
        output.seek(0)

        # Stream pages until no more data
        while len(data) > 0:
            logger.info(f"Streaming chart data page {page} with {len(data)} rows")

            for row in data:
                writer.writerow(row)

            # Yield current chunk
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)

            # Fetch next page
            page += 1
            preview_data = charts_service.get_chart_data_table_preview(
                org_warehouse, payload, page=page, limit=page_size
            )
            data = preview_data["data"]

    except Exception as error:
        logger.exception(
            f"Error streaming chart data for schema {payload.schema_name}.{payload.table_name}"
        )
        raise HttpError(500, "Internal server error") from error
    finally:
        output.close()


@charts_router.post("/download-csv/")
@has_permission(["can_view_charts"])
def download_chart_data_csv(request, payload: ChartDataPayload):
    """Stream and download chart data as CSV with all filters/aggregations applied (authenticated)"""

    orguser: OrgUser = request.orguser

    # Validate user has access to schema/table
    if not has_schema_access(request, payload.schema_name):
        raise HttpError(403, "Access to schema denied")

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()

    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    # Generate filename from chart configuration
    chart_type = payload.chart_type or "chart"
    table_name = payload.table_name or "data"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{chart_type}_{table_name}_{timestamp}.csv"

    # Stream response using common function
    response = StreamingHttpResponse(
        stream_chart_data_csv(org_warehouse, payload, page_size=5000),
        content_type="application/octet-stream",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'

    return response


@charts_router.get("/{chart_id}/", response=ChartResponse)
@has_permission(["can_view_charts"])
def get_chart(request, chart_id: int):
    """Get a specific chart"""
    orguser: OrgUser = request.orguser
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        logger.warning(f"No warehouse configured for org {orguser.org.slug}")

    # ✅ DEBUG: Log what we're returning
    logger.info(f"📤 [GET-CHART] Returning chart {chart_id}: {chart.title}")
    logger.info(f"📤 [GET-CHART] Chart type: {chart.chart_type}")
    logger.info(
        f"📤 [GET-CHART] Extra config keys: {list(chart.extra_config.keys()) if chart.extra_config else 'None'}"
    )
    logger.info(
        f"📤 [GET-CHART] Drill-down config: {chart.extra_config.get('drill_down_config') if chart.extra_config else 'None'}"
    )

    # Build response
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        # render_config removed - charts fetch fresh config via /data endpoint
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.get("/{chart_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_charts"])
def get_chart_data_by_id(request, chart_id: int, dashboard_filters: Optional[str] = None):
    """Get chart data using saved chart configuration with optional dashboard filters"""
    import json

    orguser = request.orguser
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # warehouse client
    warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)

    # Build payload from chart config
    extra_config = chart.extra_config.copy() if chart.extra_config else {}

    # Parse and resolve dashboard filters if provided
    resolved_dashboard_filters = None
    if dashboard_filters:
        try:
            filter_values = json.loads(dashboard_filters)
            logger.info(f"Applying dashboard filters to chart {chart_id}: {filter_values}")

            # Resolve filter configurations to get column information
            from ddpui.models.dashboard import DashboardFilter

            resolved_filters = []

            for filter_id, filter_value in filter_values.items():
                if filter_value is not None:
                    try:
                        # Get the filter configuration from the database
                        dashboard_filter = DashboardFilter.objects.get(id=filter_id)

                        # Only apply this filter if it applies to the same table as the chart
                        if warehouse_client.column_exists(
                            chart.schema_name, chart.table_name, dashboard_filter.column_name
                        ):
                            resolved_filters.append(
                                {
                                    "filter_id": filter_id,
                                    "column": dashboard_filter.column_name,
                                    "type": dashboard_filter.filter_type,
                                    "value": filter_value,
                                    "settings": dashboard_filter.settings,
                                }
                            )
                    except DashboardFilter.DoesNotExist:
                        logger.warning(f"Dashboard filter {filter_id} not found")

            resolved_dashboard_filters = resolved_filters

        except json.JSONDecodeError:
            logger.error(f"Invalid dashboard_filters JSON: {dashboard_filters}")
            resolved_dashboard_filters = None

    # Get existing customizations and add chart title
    customizations = extra_config.get("customizations", {})
    customizations["title"] = chart.title  # Add chart title to customizations

    payload = ChartDataPayload(
        chart_type=chart.chart_type,
        computation_type=chart.computation_type,
        schema_name=chart.schema_name,
        table_name=chart.table_name,
        x_axis=extra_config.get("x_axis_column"),
        y_axis=extra_config.get("y_axis_column"),
        dimension_col=extra_config.get("dimension_column"),
        extra_dimension=extra_config.get("extra_dimension_column"),
        # Multiple metrics support - CRITICAL FIX for dashboard charts
        metrics=extra_config.get("metrics"),
        # Map-specific fields
        geographic_column=extra_config.get("geographic_column"),
        value_column=extra_config.get("value_column"),
        selected_geojson_id=extra_config.get("selected_geojson_id"),
        customizations=extra_config.get("customizations", {}),
        offset=0,
        limit=100,
        extra_config=extra_config,
        dashboard_filters=resolved_dashboard_filters,  # Pass resolved dashboard filters
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


@charts_router.post("/{chart_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_charts"])
def get_chart_data_with_drilldown(request, chart_id: int, payload: ChartDataPayload):
    """Get chart data with drill-down support - POST version that accepts drill_down_level and drill_down_path"""
    orguser = request.orguser
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    # Build payload from chart config and merge with request payload
    extra_config = chart.extra_config.copy() if chart.extra_config else {}

    # Check if drill-down is configured
    drill_down_config = extra_config.get("drill_down_config", {})
    drill_down_enabled = drill_down_config.get("enabled", False)

    # Process drill-down if enabled
    if drill_down_enabled and (payload.drill_down_level > 0 or payload.drill_down_path):
        logger.info(
            f"Processing drill-down for chart {chart_id} at level {payload.drill_down_level}"
        )

        # Convert drill-down path to filters
        drill_down_filters = []
        if payload.drill_down_path:
            for step in payload.drill_down_path:
                drill_down_filters.append(
                    {"column": step["column"], "operator": "equals", "value": step["value"]}
                )
                logger.debug(f"Added drill-down filter: {step['column']} = {step['value']}")

        # Merge drill-down filters with existing filters
        existing_filters = extra_config.get("filters", [])
        all_filters = drill_down_filters + existing_filters
        extra_config["filters"] = all_filters

        # Determine current level configuration
        hierarchy = drill_down_config.get("hierarchy", [])
        if payload.drill_down_level < len(hierarchy):
            level_config = hierarchy[payload.drill_down_level]

            # For aggregated drill-down levels, modify the dimension column
            if level_config.get("aggregation_columns"):
                extra_config["dimension_column"] = level_config["column"]

                # Set metrics from aggregation columns if specified
                if not payload.metrics:
                    aggregation_columns = level_config.get("aggregation_columns", [])
                    metrics = [
                        {"column": col, "aggregation": "sum", "alias": col}
                        for col in aggregation_columns
                    ]
                    extra_config["metrics"] = metrics
        else:
            # At leaf level or beyond, show raw data with filters applied
            # No aggregation needed
            pass

    # Get existing customizations and add chart title
    customizations = extra_config.get("customizations", {})
    customizations["title"] = chart.title

    # Build the full payload
    full_payload = ChartDataPayload(
        chart_type=chart.chart_type,
        computation_type=chart.computation_type,
        schema_name=chart.schema_name,
        table_name=chart.table_name,
        x_axis=extra_config.get("x_axis_column"),
        y_axis=extra_config.get("y_axis_column"),
        dimension_col=extra_config.get("dimension_column"),
        extra_dimension=extra_config.get("extra_dimension_column"),
        metrics=extra_config.get("metrics") or payload.metrics,
        geographic_column=extra_config.get("geographic_column"),
        value_column=extra_config.get("value_column"),
        selected_geojson_id=extra_config.get("selected_geojson_id"),
        customizations=customizations,
        offset=payload.offset,
        limit=payload.limit,
        extra_config=extra_config,
        dashboard_filters=payload.dashboard_filters,
        drill_down_level=payload.drill_down_level,
        drill_down_path=payload.drill_down_path,
    )

    # Use the common function to generate data and config
    try:
        result = generate_chart_data_and_config(full_payload, org_warehouse, chart_id=chart.id)
        return ChartDataResponse(data=result["data"], echarts_config=result["echarts_config"])
    except ValueError as e:
        raise HttpError(400, str(e))
    except Exception as e:
        logger.error(f"Error generating drill-down data for chart {chart.id}: {str(e)}")
        logger.error(
            f"Drill-down level: {payload.drill_down_level}, path: {payload.drill_down_path}"
        )
        raise HttpError(500, f"Error generating drill-down data: {str(e)}")


@charts_router.post("/", response=ChartResponse)
@has_permission(["can_create_charts"])
def create_chart(request, payload: ChartCreate):
    """Create a new chart"""
    orguser = request.orguser

    # ✅ DEBUG: Log what we received
    logger.info(f"📥 [CREATE-CHART] Received payload for chart: {payload.title}")
    logger.info(f"📥 [CREATE-CHART] Chart type: {payload.chart_type}")
    logger.info(
        f"📥 [CREATE-CHART] Extra config keys: {list(payload.extra_config.keys()) if payload.extra_config else 'None'}"
    )
    logger.info(
        f"📥 [CREATE-CHART] Drill-down config: {payload.extra_config.get('drill_down_config') if payload.extra_config else 'None'}"
    )

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

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()

    # Build response
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        # render_config removed - charts fetch fresh config via /data endpoint
        "render_config": (
            generate_chart_render_config(chart, org_warehouse) if org_warehouse else {}
        ),
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.put("/{chart_id}/", response=ChartResponse)
@has_permission(["can_edit_charts"])
def update_chart(request, chart_id: int, payload: ChartUpdate):
    """Update a chart"""
    orguser = request.orguser
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

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

    # Build response
    chart_dict = {
        "id": chart.id,
        "title": chart.title,
        "description": chart.description,
        "chart_type": chart.chart_type,
        "computation_type": chart.computation_type,
        "schema_name": chart.schema_name,
        "table_name": chart.table_name,
        "extra_config": chart.extra_config,
        # render_config removed - charts fetch fresh config via /data endpoint
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }

    return ChartResponse(**chart_dict)


@charts_router.delete("/{chart_id}/")
@has_permission(["can_delete_charts"])
def delete_chart(request, chart_id: int):
    """Delete a chart"""
    orguser = request.orguser
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

    # Only allow deletion if the current user is the creator
    if chart.created_by != orguser:
        raise HttpError(403, "You can only delete charts you created.")

    chart.delete()
    return {"success": True}


@charts_router.post("/bulk-delete/")
@has_permission(["can_delete_charts"])
def bulk_delete_charts(request, payload: BulkDeleteRequest):
    """Delete multiple charts"""
    orguser = request.orguser

    if not payload.chart_ids:
        raise HttpError(400, "No chart IDs provided")

    try:
        # Get charts that belong to this org
        charts = Chart.objects.filter(id__in=payload.chart_ids, org=orguser.org)
        found_ids = list(charts.values_list("id", flat=True))

        # Check if all requested charts were found
        missing_ids = set(payload.chart_ids) - set(found_ids)
        if missing_ids:
            logger.warning(f"Charts not found or not accessible: {missing_ids}")

        # Delete the charts
        deleted_count = charts.delete()[0]

        return {
            "success": True,
            "deleted_count": deleted_count,
            "requested_count": len(payload.chart_ids),
            "missing_ids": list(missing_ids),
        }
    except Exception as e:
        logger.error(f"Error in bulk delete: {str(e)}")
        raise HttpError(500, f"Error deleting charts: {str(e)}")


@charts_router.get("/{chart_id}/dashboards/", response=List[dict])
@has_permission(["can_view_charts"])
def get_chart_dashboards(request, chart_id: int):
    """Get list of dashboards that use this chart"""
    orguser = request.orguser

    # Verify chart exists and belongs to org
    try:
        chart = Chart.objects.get(id=chart_id, org=orguser.org)
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")

    # Import here to avoid circular imports
    from ddpui.models.dashboard import Dashboard

    # Find dashboards that have this chart in their components
    dashboards_with_chart = []
    dashboards = Dashboard.objects.filter(org=orguser.org)

    for dashboard in dashboards:
        if dashboard.components:
            for component_id, component in dashboard.components.items():
                if (
                    component.get("type") == "chart"
                    and component.get("config", {}).get("chartId") == chart_id
                ):
                    dashboards_with_chart.append(
                        {
                            "id": dashboard.id,
                            "title": dashboard.title,
                            "dashboard_type": dashboard.dashboard_type,
                        }
                    )
                    break  # Found chart in this dashboard, no need to check other components

    return dashboards_with_chart
