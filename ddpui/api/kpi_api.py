"""KPI API endpoints"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.schemas.kpi_schema import (
    KPICreate,
    KPIUpdate,
    KPIResponse,
    KPIListResponse,
)
from ddpui.schemas.chart_schema import ChartDataResponse
from ddpui.schemas.metric_schema import MetricResponse
from ddpui.services.kpi_service import (
    KPIService,
    KPINotFoundError,
    KPIValidationError,
)
from ddpui.services.metric_service import MetricNotFoundError
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

kpi_router = Router()


def _kpi_to_response(kpi) -> KPIResponse:
    """Convert a KPI model instance to KPIResponse."""
    m = kpi.metric
    return KPIResponse(
        id=kpi.id,
        name=kpi.name,
        metric=MetricResponse(
            id=m.id,
            name=m.name,
            description=m.description,
            schema_name=m.schema_name,
            table_name=m.table_name,
            column=m.column,
            aggregation=m.aggregation,
            column_expression=m.column_expression,
            created_at=m.created_at,
            updated_at=m.updated_at,
        ),
        target_value=kpi.target_value,
        direction=kpi.direction,
        green_threshold_pct=kpi.green_threshold_pct,
        amber_threshold_pct=kpi.amber_threshold_pct,
        time_grain=kpi.time_grain,
        time_dimension_column=kpi.time_dimension_column,
        trend_periods=kpi.trend_periods,
        metric_type_tag=kpi.metric_type_tag,
        program_tags=kpi.program_tags,
        display_order=kpi.display_order,
        created_at=kpi.created_at,
        updated_at=kpi.updated_at,
    )


@kpi_router.get("/", response=KPIListResponse)
@has_permission(["can_view_kpis"])
def list_kpis(
    request,
    page: int = 1,
    page_size: int = 10,
    search: str = None,
    program_tag: str = None,
    metric_type: str = None,
):
    """List KPIs for the organization"""
    orguser: OrgUser = request.orguser

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10

    kpis, total = KPIService.list_kpis(
        org=orguser.org,
        page=page,
        page_size=page_size,
        search=search,
        program_tag=program_tag,
        metric_type=metric_type,
    )

    total_pages = (total + page_size - 1) // page_size

    return KPIListResponse(
        data=[_kpi_to_response(kpi) for kpi in kpis],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@kpi_router.post("/", response=KPIResponse)
@has_permission(["can_create_kpis"])
def create_kpi(request, payload: KPICreate):
    """Create a new KPI"""
    orguser: OrgUser = request.orguser

    try:
        kpi = KPIService.create_kpi(payload, orguser)
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None
    except KPIValidationError as e:
        raise HttpError(400, e.message) from None

    return _kpi_to_response(kpi)


@kpi_router.get("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_view_kpis"])
def get_kpi(request, kpi_id: int):
    """Get a specific KPI"""
    orguser: OrgUser = request.orguser

    try:
        kpi = KPIService.get_kpi(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None

    return _kpi_to_response(kpi)


@kpi_router.put("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_edit_kpis"])
def update_kpi(request, kpi_id: int, payload: KPIUpdate):
    """Update a KPI"""
    orguser: OrgUser = request.orguser

    try:
        kpi = KPIService.update_kpi(kpi_id, orguser.org, orguser, payload)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None
    except KPIValidationError as e:
        raise HttpError(400, e.message) from None

    return _kpi_to_response(kpi)


@kpi_router.delete("/{kpi_id}/")
@has_permission(["can_delete_kpis"])
def delete_kpi(request, kpi_id: int):
    """Delete a KPI"""
    orguser: OrgUser = request.orguser

    try:
        KPIService.delete_kpi(kpi_id, orguser.org, orguser)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None

    return {"success": True}


@kpi_router.get("/{kpi_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_kpis"])
def get_kpi_data(request, kpi_id: int):
    """Get KPI chart data + echarts config (same pattern as chart data endpoint)"""
    orguser: OrgUser = request.orguser

    try:
        result = KPIService.get_kpi_data(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None

    return ChartDataResponse(
        data=result["data"],
        echarts_config=result["echarts_config"],
    )
