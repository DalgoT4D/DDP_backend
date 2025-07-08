from ninja import Router, Schema
from ninja.errors import HttpError
from typing import List, Optional, Any, Dict
from sqlalchemy.sql.expression import column

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.visualization import Chart
from ddpui.visualization.charts.schema import (
    RawChartQueryRequest,
    ChartCreateRequest,
    ChartUpdateRequest,
    ChartResponse,
    ChartQueryResponse,
    ChartDataRequest,
    GenerateChartConfigRequest,
)
from ddpui.visualization.charts.core import generate_chart_config
from ddpui.core.visualizationfunctions import generate_chart_data
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

visualization_router = Router()


@visualization_router.post("/charts/generate/")
@has_permission(["can_view_warehouse_data"])
def post_generate_chart_config(request, payload: GenerateChartConfigRequest):
    """Generates the chart config with the data to render on the frontend."""
    orguser: OrgUser = request.orguser
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        return {"chart_config": generate_chart_config(org_warehouse, payload)}

    except Exception as e:
        logger.error(f"Failed to generate chart config: {e}")
        raise HttpError(500, f"Failed to generate chart config: {str(e)}")


@visualization_router.post("/generate_chart/")
@has_permission(["can_view_warehouse_data"])
def post_generate_chart_data(request, payload: RawChartQueryRequest):
    """
    Generate chart data by running a custom query on the warehouse.
    Does not save any chart entry to the DB.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    data = generate_chart_data(
        org_warehouse=org_warehouse,
        schema_name=payload.schema_name,
        table_name=payload.table_name,
        xaxis_col=payload.xaxis_col,
        yaxis_col=payload.yaxis_col,
        offset=payload.offset,
        limit=payload.limit,
    )

    return {"data": data}


@visualization_router.post("/charts/", response=ChartResponse)
@has_permission(["can_view_warehouse_data"])
def create_chart(request, payload: ChartCreateRequest):
    """
    Create a new chart configuration.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        chart = Chart.objects.create(
            org=org,
            title=payload.title,
            description=payload.description,
            chart_type=payload.chart_type,
            schema=payload.schema_name,
            table=payload.table,
            config=payload.config,
            created_by=orguser,
        )

        return ChartResponse(
            id=chart.id,
            title=chart.title,
            description=chart.description,
            chart_type=chart.chart_type,
            schema_name=chart.schema,
            table=chart.table,
            config=chart.config,
            created_at=chart.created_at.isoformat(),
            updated_at=chart.updated_at.isoformat(),
        )
    except Exception as e:
        logger.error(f"Failed to create chart: {e}")
        raise HttpError(500, f"Failed to create chart: {str(e)}")


@visualization_router.put("/charts/{chart_id}/", response=ChartResponse)
@has_permission(["can_view_warehouse_data"])
def update_chart(request, chart_id: int, payload: ChartUpdateRequest):
    """
    Update an existing chart configuration.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        chart = Chart.objects.filter(id=chart_id, org=org).first()
        if not chart:
            raise HttpError(404, "Chart not found")

        # Update only provided fields
        if payload.title is not None:
            chart.title = payload.title
        if payload.description is not None:
            chart.description = payload.description
        if payload.chart_type is not None:
            chart.chart_type = payload.chart_type
        if payload.schema_name is not None:
            chart.schema = payload.schema_name
        if payload.table is not None:
            chart.table = payload.table
        if payload.config is not None:
            chart.config = payload.config

        chart.save()

        return ChartResponse(
            id=chart.id,
            title=chart.title,
            description=chart.description,
            chart_type=chart.chart_type,
            schema_name=chart.schema,
            table=chart.table,
            config=chart.config,
            created_at=chart.created_at.isoformat(),
            updated_at=chart.updated_at.isoformat(),
        )
    except Chart.DoesNotExist:
        raise HttpError(404, "Chart not found")
    except Exception as e:
        logger.error(f"Failed to update chart: {e}")
        raise HttpError(500, f"Failed to update chart: {str(e)}")


@visualization_router.delete("/charts/{chart_id}/")
@has_permission(["can_view_warehouse_data"])
def delete_chart(request, chart_id: int):
    """
    Delete a chart configuration.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        chart = Chart.objects.filter(id=chart_id, org=org).first()
        if not chart:
            raise HttpError(404, "Chart not found")

        chart.delete()
        return {"success": True, "message": "Chart deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete chart: {e}")
        raise HttpError(500, f"Failed to delete chart: {str(e)}")


@visualization_router.get("/charts/", response=List[ChartResponse])
@has_permission(["can_view_warehouse_data"])
def list_charts(request):
    """
    Get all charts for the current organization.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        charts = Chart.objects.filter(org=org).order_by("-created_at")

        return [
            ChartResponse(
                id=chart.id,
                title=chart.title,
                description=chart.description,
                chart_type=chart.chart_type,
                schema_name=chart.schema,
                table=chart.table,
                config=chart.config,
                created_at=chart.created_at.isoformat(),
                updated_at=chart.updated_at.isoformat(),
            )
            for chart in charts
        ]
    except Exception as e:
        logger.error(f"Failed to list charts: {e}")
        raise HttpError(500, f"Failed to list charts: {str(e)}")


@visualization_router.get("/charts/{chart_id}/", response=ChartResponse)
@has_permission(["can_view_warehouse_data"])
def get_chart(request, chart_id: int):
    """
    Get a specific chart by ID.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        chart = Chart.objects.filter(id=chart_id, org=org).first()
        if not chart:
            raise HttpError(404, "Chart not found")

        return ChartResponse(
            id=chart.id,
            title=chart.title,
            description=chart.description,
            chart_type=chart.chart_type,
            schema_name=chart.schema,
            table=chart.table,
            config=chart.config,
            created_at=chart.created_at.isoformat(),
            updated_at=chart.updated_at.isoformat(),
        )
    except Exception as e:
        logger.error(f"Failed to get chart: {e}")
        raise HttpError(500, f"Failed to get chart: {str(e)}")


@visualization_router.post("/charts/{chart_id}/data/", response=ChartQueryResponse)
@has_permission(["can_view_warehouse_data"])
def post_chart_data(request, chart_id: int, payload: ChartDataRequest):
    """
    Get chart data for a saved chart using its configuration.
    """
    orguser: OrgUser = request.orguser
    org = orguser.org
    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        chart = Chart.objects.filter(id=chart_id, org=org).first()
        if not chart:
            raise HttpError(404, "Chart not found")

        data = generate_chart_data(
            org_warehouse=org_warehouse,
            schema_name=chart.schema,
            table_name=chart.table,
            xaxis_col=payload.xaxis_col,
            yaxis_col=payload.yaxis_col,
            offset=payload.offset,
            limit=payload.limit,
        )

        return {"data": data}
    except Exception as e:
        logger.error(f"Failed to get chart data: {e}")
        raise HttpError(500, f"Failed to get chart data: {str(e)}")
