"""Public API endpoints - no authentication required"""

import json
from typing import Optional, List
from datetime import datetime

from ninja import Router, Schema
from ninja.errors import HttpError
from django.utils import timezone
from django.db.models import F

from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.org import Org
from ddpui.utils.custom_logger import CustomLogger

# Import schemas and helper functions from authenticated APIs for consistency
from ddpui.api.dashboard_native_api import (
    DashboardResponse,
    DashboardFilterResponse,
    FilterOptionResponse,
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
    "/dashboards/{token}/charts/{chart_id}/data/",
    response={200: PublicChartDataResponse, 404: PublicErrorResponse},
)
def get_public_chart_data(request, token: str, chart_id: int):
    """Get public chart data - reuses authenticated chart data logic"""
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
            aggregate_col=extra_config.get("aggregate_column"),
            aggregate_func=extra_config.get("aggregate_function"),
            extra_dimension=extra_config.get("extra_dimension_column"),
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
                "distinct_count": int(row["distinct_count"])
                if row["distinct_count"] is not None
                else 0,
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
