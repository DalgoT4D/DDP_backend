"""Public API endpoints - no authentication required"""

import json
from typing import Optional, List
import copy

from ninja import Router, Schema
from django.utils import timezone
from django.db.models import F

from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.models.dashboard import Dashboard
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.visualization import Chart
from ddpui.models.org import OrgWarehouse
from ddpui.api.charts_api import MapDataOverlayPayload
from ddpui.core.charts import charts_service

from ddpui.api.dashboard_native_api import (
    DashboardResponse,
    FilterOptionsResponse,
    get_dashboard_response,
)
from ddpui.api.filter_api import (
    FilterPreviewResponse,
    FilterOptionResponse as AuthFilterOptionResponse,
)
from ddpui.schemas.chart_schema import ChartDataResponse

logger = CustomLogger("ddpui")

public_router = Router()


# Enhanced public schemas that extend existing ones with public-specific fields
class PublicDashboardResponse(DashboardResponse):
    """Extended dashboard response for public access with additional public fields"""

    org_name: str
    is_valid: bool = True

    # Remove fields not needed in public view
    last_modified_by: Optional[str] = None
    is_locked: bool = False
    locked_by: Optional[str] = None


class PublicChartDataResponse(ChartDataResponse):
    """Extended chart data response for public access"""

    is_valid: bool = True


class PublicFilterOptionsResponse(FilterOptionsResponse):
    """Extended filter options response for public access"""

    is_valid: bool = True


class PublicFilterPreviewResponse(FilterPreviewResponse):
    """Extended filter preview response for public access - matches authenticated API"""

    is_valid: bool = True


class PublicValidationResponse(Schema):
    """Schema for public validation response"""

    is_valid: bool
    title: Optional[str] = None


class PublicErrorResponse(Schema):
    """Schema for public error response"""

    error: str
    is_valid: bool = False


@public_router.get(
    "/dashboards/{token}/", response={200: PublicDashboardResponse, 404: PublicErrorResponse}
)
def get_public_dashboard(request, token: str):
    """Get public dashboard by token - reuses authenticated dashboard logic"""
    try:
        # Find dashboard by token and ensure it's public
        dashboard = Dashboard.objects.select_related("org", "created_by__user").get(
            public_share_token=token, is_public=True
        )

        # Update access analytics
        Dashboard.objects.filter(id=dashboard.id).update(
            public_access_count=F("public_access_count") + 1, last_public_accessed=timezone.now()
        )

        # Reuse the authenticated dashboard response generation logic
        dashboard_data = get_dashboard_response(dashboard)

        # Enhance with public-specific fields and remove sensitive information
        public_response_data = {
            **dashboard_data,
            "org_name": dashboard.org.name,
            "is_valid": True,
            # Remove sensitive fields for public access
            "last_modified_by": None,
            "is_locked": False,
            "locked_by": None,
        }

        # Log public access
        ip_address = request.META.get("REMOTE_ADDR", "unknown")
        user_agent = request.META.get("HTTP_USER_AGENT", "unknown")[:100]
        logger.info(
            f"Public dashboard {dashboard.id} ({dashboard.title}) accessed by {ip_address}, org: {dashboard.org.name}, user_agent: {user_agent}"
        )

        return PublicDashboardResponse(**public_response_data)

    except Dashboard.DoesNotExist:
        logger.warning(f"Public dashboard access failed - token not found: {token}")
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public dashboard access error: {str(e)}")
        return 404, PublicErrorResponse(error="Dashboard not accessible", is_valid=False)


@public_router.get(
    "/dashboards/{token}/charts/{chart_id}/",
    response={200: dict, 404: PublicErrorResponse},
)
def get_public_chart_metadata(request, token: str, chart_id: int):
    """
    Get public chart metadata - ESSENTIAL for public dashboards

    PURPOSE: In public mode, the frontend has no access to the private 'chart' object.
    This endpoint provides the chart metadata needed for:
    - Chart type detection (map, table, bar, etc.)
    - Chart configuration (extra_config with layers, metrics, etc.)
    - Schema/table information for data queries
    - Conditional rendering logic in the frontend

    WITHOUT THIS: Public dashboards cannot determine chart types or configurations,
    breaking all specialized chart rendering (maps, tables, etc.)
    """
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Import required modules
        from ddpui.models.visualization import Chart

        # Get the chart and ensure it belongs to the dashboard's org
        chart = Chart.objects.filter(id=chart_id, org=dashboard.org).first()
        if not chart:
            raise Exception("Chart not found in dashboard's organization")

        return {
            "id": chart.id,
            "title": chart.title,
            "chart_type": chart.chart_type,
            "computation_type": chart.computation_type,
            "schema_name": chart.schema_name,
            "table_name": chart.table_name,
            "extra_config": chart.extra_config,
            "description": chart.description,
            "is_valid": True,
        }

    except Dashboard.DoesNotExist:
        logger.warning(
            f"Public chart metadata access failed - dashboard not found for token: {token}"
        )
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public chart metadata error for chart {chart_id}: {str(e)}")
        return 404, PublicErrorResponse(error="Chart metadata unavailable", is_valid=False)


@public_router.get(
    "/dashboards/{token}/charts/{chart_id}/data/",
    response={200: PublicChartDataResponse, 404: PublicErrorResponse},
)
def get_public_chart_data(request, token: str, chart_id: int):
    """
    Get public chart data - OPTIONAL/REDUNDANT for most chart types

    PURPOSE: Provides chart data for regular charts (bar, line, pie, etc.) in public mode.

    STATUS: Most regular charts already work with existing public infrastructure.
    This endpoint was created for completeness but may not be strictly necessary.

    KEEP IT: Doesn't hurt to have it, provides a consistent API pattern.
    It handles edge cases where the existing infrastructure might not work.
    """
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Get dashboard filters from query params
        filters = {}
        filters_param = request.GET.get("dashboard_filters")
        if filters_param:
            try:
                filters = json.loads(filters_param)
            except json.JSONDecodeError:
                logger.warning(f"Invalid dashboard filters JSON: {filters_param}")
                pass

        # Import required modules from authenticated charts API
        from ddpui.models.visualization import Chart
        from ddpui.models.org import OrgWarehouse
        from ddpui.api.charts_api import generate_chart_data_and_config
        from ddpui.schemas.chart_schema import ChartDataPayload

        # Get the chart and org warehouse
        chart = Chart.objects.filter(id=chart_id, org=dashboard.org).first()
        if not chart:
            raise Exception("Chart not found in dashboard's organization")

        org_warehouse = OrgWarehouse.objects.filter(org=dashboard.org).first()
        if not org_warehouse:
            raise Exception("No warehouse configured for organization")

        # Build payload from chart config - reuse exact logic from authenticated endpoint
        extra_config = chart.extra_config.copy() if chart.extra_config else {}

        # Parse and resolve dashboard filters if provided - same logic as authenticated API
        resolved_dashboard_filters = None
        if filters:
            # Resolve filter configurations to get column information - same as authenticated API
            from ddpui.models.dashboard import DashboardFilter

            resolved_filters = []

            for filter_id, value in filters.items():
                if value is not None:  # Skip filters with no value
                    try:
                        dashboard_filter = DashboardFilter.objects.get(
                            id=int(filter_id), dashboard=dashboard
                        )
                        # Only apply this filter if it applies to the same table as the chart (same as authenticated API)
                        if (
                            dashboard_filter.schema_name == chart.schema_name
                            and dashboard_filter.table_name == chart.table_name
                        ):
                            resolved_filters.append(
                                {
                                    "filter_id": filter_id,
                                    "column": dashboard_filter.column_name,  # Use "column" key as expected
                                    "type": dashboard_filter.filter_type,  # Use "type" key as expected
                                    "value": value,
                                    "settings": dashboard_filter.settings
                                    or {},  # Use actual settings
                                }
                            )
                    except (DashboardFilter.DoesNotExist, ValueError):
                        logger.warning(f"Public API: Dashboard filter {filter_id} not found")

            resolved_dashboard_filters = resolved_filters

        # Add chart title to customizations like authenticated API
        customizations = extra_config.get("customizations", {})
        customizations["title"] = chart.title  # Add chart title to customizations

        # Use the exact same payload structure as authenticated API
        payload = ChartDataPayload(
            schema_name=chart.schema_name,
            table_name=chart.table_name,
            chart_type=chart.chart_type,
            computation_type=chart.computation_type,
            x_axis=extra_config.get("x_axis_column"),  # Match authenticated API field names
            y_axis=extra_config.get("y_axis_column"),
            dimension_col=extra_config.get("dimension_column"),
            extra_dimension=extra_config.get("extra_dimension_column"),
            # Multiple metrics support - CRITICAL FIX for public API charts
            metrics=extra_config.get("metrics"),
            # Map-specific fields for consistency
            geographic_column=extra_config.get("geographic_column"),
            value_column=extra_config.get("value_column"),
            selected_geojson_id=extra_config.get("selected_geojson_id"),
            customizations=customizations,
            offset=0,
            limit=100,
            extra_config=extra_config,
            dashboard_filters=resolved_dashboard_filters,  # Pass resolved dashboard filters
        )

        # Generate chart data and config using exact same function as authenticated API
        chart_data = generate_chart_data_and_config(payload, org_warehouse, chart_id)

        # Build response with same structure as authenticated API + public fields
        response_data = {
            **chart_data,  # Include all fields from authenticated response
            "is_valid": True,
        }

        return PublicChartDataResponse(**response_data)

    except Dashboard.DoesNotExist:
        logger.warning(f"Public chart access failed - dashboard not found for token: {token}")
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public chart data error for chart {chart_id}: {str(e)}")
        return 404, PublicErrorResponse(error="Chart data unavailable", is_valid=False)


@public_router.get(
    "/dashboards/{token}/filters/preview/",
    response={200: PublicFilterPreviewResponse, 404: PublicErrorResponse},
)
def get_public_filter_preview(
    request,
    token: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    filter_type: str,
    limit: int = 100,
):
    """Get public filter preview - identical to authenticated filter preview API"""
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Get org warehouse
        from ddpui.models.org import OrgWarehouse
        from ddpui.api.filter_api import get_filter_preview

        org_warehouse = OrgWarehouse.objects.filter(org=dashboard.org).first()
        if not org_warehouse:
            raise Exception("No warehouse configured for organization")

        # Create a mock request with orguser to reuse the authenticated filter preview logic
        class MockRequest:
            def __init__(self, org):
                self.orguser = type("MockOrgUser", (), {"org": org})()

        mock_request = MockRequest(dashboard.org)

        # Use the exact same function as authenticated API
        from ddpui.core.charts.charts_service import get_warehouse_client
        from ddpui.datainsights.query_builder import AggQueryBuilder
        from ddpui.core.charts.charts_service import execute_query
        from sqlalchemy import func, column, distinct, cast, Float, Date

        warehouse_client = get_warehouse_client(org_warehouse)

        if filter_type == "value":
            # Get distinct values with counts for categorical filter - same logic as authenticated API
            query_builder = AggQueryBuilder()
            query_builder.add_column(column(column_name).label("value"))
            query_builder.add_aggregate_column(None, "count", alias="count")
            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))
            query_builder.group_cols_by(column_name)
            query_builder.order_cols_by([("count", "desc"), ("value", "asc")])
            query_builder.limit_rows(limit)

            # Execute query using charts_service function
            results = execute_query(warehouse_client, query_builder)

            options = [
                AuthFilterOptionResponse(
                    label=str(row["value"]) if row["value"] is not None else "NULL",
                    value=str(row["value"]) if row["value"] is not None else "",
                    count=int(row["count"]),
                )
                for row in results
            ]

            response_data = {"options": options, "stats": None, "is_valid": True}
            return PublicFilterPreviewResponse(**response_data)

        elif filter_type == "numerical":
            # Get numerical statistics - same logic as authenticated API
            query_builder = AggQueryBuilder()
            query_builder.add_aggregate_column(column_name, "min", alias="min_value")
            query_builder.add_aggregate_column(column_name, "max", alias="max_value")
            query_builder.add_column(func.avg(cast(column(column_name), Float)).label("avg_value"))
            query_builder.add_aggregate_column(
                column_name, "count_distinct", alias="distinct_count"
            )
            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))

            results = execute_query(warehouse_client, query_builder)
            row = results[0]

            stats = {
                "min_value": float(row["min_value"]) if row["min_value"] is not None else 0.0,
                "max_value": float(row["max_value"]) if row["max_value"] is not None else 100.0,
                "avg_value": float(row["avg_value"]) if row["avg_value"] is not None else 50.0,
                "distinct_count": (
                    int(row["distinct_count"]) if row["distinct_count"] is not None else 0
                ),
            }

            response_data = {"options": None, "stats": stats, "is_valid": True}
            return PublicFilterPreviewResponse(**response_data)

        elif filter_type == "datetime":
            # Get date range - same logic as authenticated API
            query_builder = AggQueryBuilder()

            if org_warehouse.wtype == "postgres":
                query_builder.add_column(
                    func.min(cast(column(column_name), Date)).label("min_date")
                )
                query_builder.add_column(
                    func.max(cast(column(column_name), Date)).label("max_date")
                )
                query_builder.add_column(
                    func.count(distinct(func.date(column(column_name)))).label("distinct_days")
                )
            elif org_warehouse.wtype == "bigquery":
                query_builder.add_column(func.min(func.date(column(column_name))).label("min_date"))
                query_builder.add_column(func.max(func.date(column(column_name))).label("max_date"))
                query_builder.add_column(
                    func.count(distinct(func.date(column(column_name)))).label("distinct_days")
                )
            else:
                query_builder.add_aggregate_column(column_name, "min", alias="min_date")
                query_builder.add_aggregate_column(column_name, "max", alias="max_date")
                query_builder.add_aggregate_column(
                    column_name, "count_distinct", alias="distinct_days"
                )

            query_builder.add_aggregate_column(None, "count", alias="total_records")
            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))

            results = execute_query(warehouse_client, query_builder)
            row = results[0]

            stats = {
                "min_date": row["min_date"].isoformat() if row["min_date"] else None,
                "max_date": row["max_date"].isoformat() if row["max_date"] else None,
                "distinct_days": int(row["distinct_days"]) if row["distinct_days"] else 0,
                "total_records": int(row["total_records"]) if row["total_records"] else 0,
            }

            response_data = {"options": None, "stats": stats, "is_valid": True}
            return PublicFilterPreviewResponse(**response_data)

        else:
            return 404, PublicErrorResponse(
                error=f"Invalid filter type: {filter_type}", is_valid=False
            )

    except Dashboard.DoesNotExist:
        logger.warning(
            f"Public filter preview access failed - dashboard not found for token: {token}"
        )
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public filter preview error: {str(e)}")
        return 404, PublicErrorResponse(error="Filter preview unavailable", is_valid=False)


@public_router.get(
    "/dashboards/{token}/validate/",
    response={200: PublicValidationResponse, 404: PublicValidationResponse},
)
def validate_public_dashboard(request, token: str):
    """Lightweight validation - check if token is valid"""
    try:
        dashboard = Dashboard.objects.only("title").get(public_share_token=token, is_public=True)
        return PublicValidationResponse(is_valid=True, title=dashboard.title)
    except Dashboard.DoesNotExist:
        return 404, PublicValidationResponse(is_valid=False, title=None)


@public_router.post(
    "/dashboards/{token}/charts/{chart_id}/data-preview/",
    response={200: dict, 404: PublicErrorResponse},
)
def get_public_chart_data_preview(request, token: str, chart_id: int):
    """
    Get public chart data preview - ESSENTIAL for table charts

    PURPOSE: Table charts use specialized data fetching with:
    - Pagination (page size, offset)
    - Sorting (column sorting, direction)
    - Filtering (search within table data)
    - Raw data display (not aggregated like regular charts)

    This is completely different from regular chart data endpoints which return
    aggregated data for visualization. Table charts need the raw tabular data
    with pagination controls.

    WITHOUT THIS: Table charts in public dashboards cannot display data
    or handle pagination/sorting functionality.
    """
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Import required modules
        from ddpui.models.visualization import Chart
        from ddpui.models.org import OrgWarehouse
        from ddpui.core.charts import charts_service
        from ddpui.schemas.chart_schema import ChartDataPayload

        # Get the chart and org warehouse
        chart = Chart.objects.filter(id=chart_id, org=dashboard.org).first()
        if not chart:
            raise Exception("Chart not found in dashboard's organization")

        org_warehouse = OrgWarehouse.objects.filter(org=dashboard.org).first()
        if not org_warehouse:
            raise Exception("No warehouse configured for organization")

        # Get payload from request body
        import json

        payload = json.loads(request.body) if request.body else {}

        # Convert payload to ChartDataPayload
        chart_payload = ChartDataPayload(**payload)

        # Get table preview using same function as authenticated API
        preview_data = charts_service.get_chart_data_table_preview(org_warehouse, chart_payload)

        return {
            "columns": preview_data["columns"],
            "column_types": preview_data["column_types"],
            "data": preview_data["data"],
            "total_rows": preview_data["total_rows"],
            "page": preview_data["page"],
            "page_size": preview_data["page_size"],
            "is_valid": True,
        }

    except Dashboard.DoesNotExist:
        logger.warning(f"Public table data access failed - dashboard not found for token: {token}")
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public table data error for chart {chart_id}: {str(e)}")
        return 404, PublicErrorResponse(error="Table data unavailable", is_valid=False)


@public_router.get(
    "/dashboards/{token}/geojsons/{geojson_id}/",
    response={200: dict, 404: PublicErrorResponse},
)
def get_public_geojson_data(request, token: str, geojson_id: int):
    """
    Get public geojson data - ESSENTIAL for map charts

    PURPOSE: Map charts require geographic boundary data (GeoJSON) to render:
    - Country/state/district boundaries
    - Polygon coordinates for map regions
    - Geographic feature properties

    This data is completely separate from chart data - it defines the map shapes
    while chart data provides the values to overlay on those shapes.

    WITHOUT THIS: Map charts in public dashboards show no geographic boundaries,
    appearing as blank/empty maps with no visual regions to display data on.
    """
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Import required modules
        from ddpui.models.geojson import GeoJSON
        from django.shortcuts import get_object_or_404

        # Get geojson with same logic as authenticated API
        geojson = get_object_or_404(GeoJSON, id=geojson_id)

        # Check access permissions (allow default geojsons or org-specific ones)
        if not geojson.is_default and geojson.org_id != dashboard.org.id:
            raise Exception("Access denied to GeoJSON")

        return {
            "id": geojson.id,
            "name": geojson.name,
            "display_name": f"{geojson.name} ({geojson.description or 'No description'})",
            "geojson_data": geojson.geojson_data,
            "properties_key": geojson.properties_key,
            "is_valid": True,
        }

    except Dashboard.DoesNotExist:
        logger.warning(f"Public geojson access failed - dashboard not found for token: {token}")
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public geojson error for geojson {geojson_id}: {str(e)}")
        return 404, PublicErrorResponse(error="GeoJSON data unavailable", is_valid=False)


@public_router.post(
    "/dashboards/{token}/charts/{chart_id}/map-data/",
    response={200: dict, 404: PublicErrorResponse},
)
def get_public_map_data_overlay(request, token: str, chart_id: int):
    """
    Get public map data overlay - ESSENTIAL for map charts

    PURPOSE: Map charts need data values to overlay on geographic regions:
    - Aggregated data per geographic region (state, district, etc.)
    - Values to colorize/visualize on the map
    - Geographic column mapping (which column contains region names)
    - Drill-down filtering support

    This endpoint performs specialized geographic data aggregation that's completely
    different from regular chart data processing. It groups data by geographic
    regions and applies proper aggregation functions.

    Key difference from regular charts: Uses 'value' alias (not original column names)
    to match ECharts map visualization requirements.

    WITHOUT THIS: Map charts show geographic boundaries but no data overlay,
    appearing as blank maps with no population/value visualization.
    """
    try:
        # Verify dashboard is public
        dashboard = Dashboard.objects.get(public_share_token=token, is_public=True)

        # Get the chart and org warehouse
        chart = Chart.objects.filter(id=chart_id, org=dashboard.org).first()
        if not chart:
            raise Exception("Chart not found in dashboard's organization")

        org_warehouse = OrgWarehouse.objects.filter(org=dashboard.org).first()
        if not org_warehouse:
            raise Exception("No warehouse configured for organization")

        warehouse_client = WarehouseFactory.get_warehouse_client(org_warehouse)

        payload = json.loads(request.body) if request.body else {}

        # Add metrics from chart configuration if not provided
        # IMPORTANT: Always use 'value' as alias to match private API behavior
        if "metrics" not in payload and chart.extra_config.get("metrics"):
            # Transform metrics to use 'value' alias (same as private API)
            original_metrics = chart.extra_config["metrics"]
            payload["metrics"] = [
                {
                    "column": original_metrics[0]["column"],
                    "aggregation": original_metrics[0]["aggregation"],
                    "alias": "value",  # Force alias to 'value' like private API
                }
            ]
        elif "metrics" not in payload:
            # Fallback: create a metric from value_column and aggregate_function
            payload["metrics"] = [
                {
                    "column": payload.get("value_column"),
                    "aggregation": payload.get("aggregate_function", "sum"),
                    "alias": "value",  # Force alias to 'value' like private API
                }
            ]

        # Convert payload to MapDataOverlayPayload
        map_payload = MapDataOverlayPayload(**payload)

        # Validate required fields
        if not all(
            [
                map_payload.schema_name,
                map_payload.table_name,
                map_payload.geographic_column,
                map_payload.value_column,
            ]
        ):
            raise Exception("Missing required fields for map data")

        if not map_payload.metrics:
            raise Exception("Missing metrics - at least one metric is required")

        # Use same logic as authenticated API
        extra_config = copy.deepcopy(map_payload.extra_config or {})

        # Handle dashboard filters (same logic as private API)
        resolved_dashboard_filters = None
        if map_payload.dashboard_filters:
            try:
                # Import dashboard filter model
                from ddpui.models.dashboard import DashboardFilter

                resolved_filters = []

                for filter_id, filter_value in map_payload.dashboard_filters.items():
                    if filter_id and filter_value is not None:
                        try:
                            dashboard_filter = DashboardFilter.objects.get(
                                id=filter_id, dashboard__org=dashboard.org
                            )
                            # Only apply this filter if it applies to the same table as the chart
                            if warehouse_client.column_exists(
                                map_payload.schema_name,
                                map_payload.table_name,
                                dashboard_filter.column_name,
                            ):
                                resolved_filters.append(
                                    {
                                        "filter_id": filter_id,
                                        "column": dashboard_filter.column_name,
                                        "type": dashboard_filter.filter_type,
                                        "value": filter_value,
                                        "settings": dashboard_filter.settings or {},
                                    }
                                )
                        except DashboardFilter.DoesNotExist:
                            logger.warning(f"Dashboard filter {filter_id} not found")

                resolved_dashboard_filters = resolved_filters

            except Exception as e:
                logger.error(f"Error resolving dashboard filters: {str(e)}")
                resolved_dashboard_filters = None

        # Build chart payload for map data query (same logic as private API)
        from ddpui.schemas.chart_schema import ChartDataPayload, ExecuteChartQuery

        chart_payload = ChartDataPayload(
            chart_type="map",
            computation_type="aggregated",
            schema_name=map_payload.schema_name,
            table_name=map_payload.table_name,
            dimension_col=map_payload.geographic_column,
            metrics=map_payload.metrics,
            dashboard_filters=resolved_dashboard_filters,
            extra_config=extra_config,
        )

        # Get warehouse client and build query using standard chart service
        warehouse = charts_service.get_warehouse_client(org_warehouse)
        query_builder = charts_service.build_chart_query(chart_payload, org_warehouse)

        # Add filters if provided (drill-down filters) with case-insensitive matching
        if map_payload.filters:
            from sqlalchemy import column, func

            for filter_column, filter_value in map_payload.filters.items():
                # Use case-insensitive matching for string filters (same as private API)
                query_builder.where_clause(
                    func.upper(column(filter_column)) == str(filter_value).upper()
                )

        # Execute query using standard chart service
        execute_payload = ExecuteChartQuery(
            chart_type="map",
            computation_type="aggregated",
            dimension_col=map_payload.geographic_column,
            metrics=map_payload.metrics,
        )

        dict_results = charts_service.execute_chart_query(warehouse, query_builder, execute_payload)

        logger.info(f"Public map data overlay query returned {len(dict_results)} rows")

        # Transform results for map visualization (same logic as private API)
        map_data = []
        for row in dict_results:
            # Get the dimension value (geographic region name)
            region_name = row.get(map_payload.geographic_column)
            # Get the aggregated value using 'value' alias (same as private API)
            value = row.get("value")

            if region_name and value is not None:
                # Normalize region name to proper case for frontend compatibility (same as private API)
                # Convert "MAHARASHTRA" -> "Maharashtra", "gujarat" -> "Gujarat"
                normalized_name = str(region_name).strip().title()
                map_data.append({"name": normalized_name, "value": float(value)})

        return {"data": map_data, "is_valid": True, "count": len(map_data)}

    except Dashboard.DoesNotExist:
        logger.warning(f"Public map data access failed - dashboard not found for token: {token}")
        return 404, PublicErrorResponse(
            error="Dashboard not found or no longer public", is_valid=False
        )
    except Exception as e:
        logger.error(f"Public map data error for chart {chart_id}: {str(e)}")
        return 404, PublicErrorResponse(error="Map data unavailable", is_valid=False)


@public_router.get("/regions/", response=List[dict])
def get_public_regions(request, country_code: str = "IND", region_type: str = None):
    """List available regions for a country - PUBLIC VERSION"""
    from ddpui.core.charts.maps_service import get_available_regions

    try:
        regions = get_available_regions(country_code, region_type)
        return regions
    except Exception as e:
        logger.error(f"Public regions list error: {str(e)}")
        return []


@public_router.get("/regions/{region_id}/children/", response=List[dict])
def get_public_child_regions(request, region_id: int):
    """Get child regions for a parent region - PUBLIC VERSION"""
    from ddpui.core.charts.maps_service import get_child_regions

    try:
        children = get_child_regions(region_id)
        return children
    except Exception as e:
        logger.error(f"Public child regions error for region {region_id}: {str(e)}")
        return []


@public_router.get("/regions/{region_id}/geojsons/", response=List[dict])
def get_public_region_geojsons(request, region_id: int):
    """Get available GeoJSONs for a region - PUBLIC VERSION"""
    from ddpui.core.charts.maps_service import get_available_geojsons_for_region

    try:
        # Use org_id=None for public access to get default GeoJSONs only
        geojsons = get_available_geojsons_for_region(region_id, org_id=None)
        return geojsons
    except Exception as e:
        logger.error(f"Public region geojsons error for region {region_id}: {str(e)}")
        return []


@public_router.get("/geojsons/{geojson_id}/", response=dict)
def get_public_geojson_detail(request, geojson_id: int):
    """Get GeoJSON detail - PUBLIC VERSION"""
    try:
        from ddpui.models.geojson import GeoJSON

        # Only allow access to default (public) GeoJSONs
        geojson = GeoJSON.objects.get(id=geojson_id, is_default=True)

        return {
            "id": geojson.id,
            "region_id": geojson.region.id,
            "geojson_data": geojson.geojson_data,
            "properties_key": geojson.properties_key,
            "name": geojson.name,
            "description": geojson.description,
        }
    except GeoJSON.DoesNotExist:
        logger.warning(f"Public GeoJSON access failed - not found or not public: {geojson_id}")
        return 404, {"error": "GeoJSON not found or not public"}
    except Exception as e:
        logger.error(f"Public GeoJSON detail error for {geojson_id}: {str(e)}")
        return 404, {"error": "GeoJSON unavailable"}


@public_router.get("/regions/", response=List[dict])
def list_available_regions_public(request, country_code: str = "IND", region_type: str = None):
    """List available regions for a country - public access"""
    try:
        from ddpui.core.charts.maps_service import get_available_regions

        regions = get_available_regions(country_code, region_type)
        logger.info(
            f"Public regions fetched for {country_code}, type: {region_type}, count: {len(regions)}"
        )
        return regions
    except Exception as e:
        logger.error(f"Public regions error for {country_code}: {str(e)}")
        return []


@public_router.get("/regions/{region_id}/children/", response=List[dict])
def get_child_regions_public(request, region_id: int):
    """Get child regions for a parent region - public access"""
    try:
        from ddpui.core.charts.maps_service import get_child_regions

        children = get_child_regions(region_id)
        logger.info(f"Public child regions fetched for parent {region_id}, count: {len(children)}")
        return children
    except Exception as e:
        logger.error(f"Public child regions error for {region_id}: {str(e)}")
        return []


@public_router.get("/regions/{region_id}/geojsons/", response=List[dict])
def get_region_geojsons_public(request, region_id: int):
    """Get available geojsons for a region - public access"""
    try:
        from ddpui.models.geojson import GeoJSON

        # Only return public/default geojsons
        geojsons = GeoJSON.objects.filter(region_id=region_id, is_default=True).values(
            "id", "name", "description", "properties_key"
        )

        geojson_list = list(geojsons)
        logger.info(f"Public geojsons fetched for region {region_id}, count: {len(geojson_list)}")
        return geojson_list
    except Exception as e:
        logger.error(f"Public region geojsons error for {region_id}: {str(e)}")
        return []
