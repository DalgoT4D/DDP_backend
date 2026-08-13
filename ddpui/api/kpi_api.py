"""KPI API endpoints"""

from typing import Optional

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.access.access_control import get_user_access, get_user_access_map
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import AccessLevel, AccessRequest, ResourceShare, ResourceType
from ddpui.schemas.kpi_schema import (
    KPICreate,
    KPIUpdate,
    AnnotationEntryCreate,
    AnnotationEntryUpdate,
    AnnotationEntryResponse,
    KPIResponse,
    KPIListResponse,
)
from ddpui.schemas.chart_schemas import ChartDataResponse
from ddpui.core.kpi.kpi_service import (
    KPIService,
    KPINotFoundError,
    KPIValidationError,
    KPIPermissionError,
)
from ddpui.core.metric.metric_service import MetricNotFoundError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import api_response
from ddpui.core.audit_log_service import create_audit_log
from ddpui.models.audit_log import AuditLogAction, AuditLogResourceType
import json

logger = CustomLogger("ddpui")

kpi_router = Router()


@kpi_router.get("/program-tags/", response=list)
@has_permission(["can_view_kpis"])
def list_program_tags(request):
    """Get all unique program tags across KPIs for the organization."""
    orguser: OrgUser = request.orguser
    return KPIService.get_all_program_tags(orguser.org)


@kpi_router.get("/", response=KPIListResponse)
@has_permission(["can_view_kpis"])
def list_kpis(
    request,
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    program_tag: Optional[str] = None,
    metric_type: Optional[str] = None,
):
    """List KPIs for the organization"""
    orguser: OrgUser = request.orguser

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10

    kpis, total = KPIService.list_kpis(
        org=orguser.org,
        orguser=orguser,
        page=page,
        page_size=page_size,
        search=search,
        program_tag=program_tag,
        metric_type=metric_type,
    )

    total_pages = (total + page_size - 1) // page_size

    access_map = get_user_access_map(orguser, ResourceType.KPI, kpis)

    return KPIListResponse(
        data=[KPIService.kpi_to_response(kpi, access_level=access_map.get(kpi.id)) for kpi in kpis],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@kpi_router.get("/summary/", response=list)
@has_permission(["can_view_kpis"])
def get_kpi_summary(request):
    """Batch compute all KPIs with current values + RAG for the KPI page."""
    orguser: OrgUser = request.orguser
    return KPIService.get_kpi_summary(orguser.org)


@kpi_router.post("/", response=KPIResponse)
@has_permission(["can_create_kpis"])
def create_kpi(request, payload: KPICreate):
    """Create a new KPI"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        kpi = KPIService.create_kpi(payload, orguser)

        resource_fields = payload.model_dump(exclude={"metric_id"})
        resource_fields[
            "name"
        ] = kpi.name  # actual resolved name, not the raw (possibly None) payload value
        resource_fields["metric"] = kpi.metric.name

        create_audit_log(
            org=org,
            orguser=orguser,
            resource_type=AuditLogResourceType.KPI,
            resource_id=str(kpi.id),
            action=AuditLogAction.CREATE,
            resource_fields=resource_fields,
        )
    except MetricNotFoundError:
        raise HttpError(404, "Metric not found") from None
    except KPIValidationError as e:
        raise HttpError(400, e.message) from None

    return KPIService.kpi_to_response(kpi, access_level=AccessLevel.EDIT)


@kpi_router.get("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_view_kpis"])
def get_kpi(request, kpi_id: int):
    """Get a specific KPI"""
    orguser: OrgUser = request.orguser

    try:
        kpi = KPIService.get_kpi(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None

    access = get_user_access(orguser, ResourceType.KPI, kpi_id)
    if access is None:
        raise HttpError(403, "you do not have access to this KPI")

    return KPIService.kpi_to_response(kpi, access_level=access)


@kpi_router.put("/{kpi_id}/", response=KPIResponse)
@has_permission(["can_edit_kpis"])
def update_kpi(request, kpi_id: int, payload: KPIUpdate):
    """Update a KPI"""

    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        kpi = KPIService.update_kpi(kpi_id, org, orguser, payload)

        # KPIUpdate is a genuine partial patch — only fields actually present
        # in this request are logged, matching the exact criterion
        # KPIService itself uses (model_dump(exclude_unset=True)) to decide
        # what to touch. A field missing here means "not touched", not "cleared".
        # "name" is the exception: always included (current value) so the row
        # stays self-identifying without a separate name column.
        touched = payload.model_dump(exclude_unset=True)
        resource_fields = {k: v for k, v in touched.items() if k != "metric_id"}
        if "metric_id" in touched:
            resource_fields["metric"] = kpi.metric.name
        resource_fields["name"] = kpi.name

        create_audit_log(
            org=org,
            orguser=orguser,
            resource_type=AuditLogResourceType.KPI,
            resource_id=str(kpi_id),
            action=AuditLogAction.UPDATE,
            resource_fields=resource_fields,
        )
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None
    except KPIValidationError as e:
        raise HttpError(400, e.message) from None

    return KPIService.kpi_to_response(
        kpi, access_level=get_user_access(orguser, ResourceType.KPI, kpi_id)
    )


@kpi_router.delete("/{kpi_id}/")
@has_permission(["can_delete_kpis"])
def delete_kpi(request, kpi_id: int):
    """Delete a KPI"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    try:
        kpi_name = KPIService.delete_kpi(kpi_id, org, orguser)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None
    except KPIValidationError as e:
        raise HttpError(400, e.message) from None
    except KPIPermissionError as e:
        raise HttpError(403, e.message) from None

    ResourceShare.objects.filter(
        org=org, resource_type=ResourceType.KPI, resource_id=str(kpi_id)
    ).delete()
    AccessRequest.objects.filter(
        org=org, resource_type=ResourceType.KPI, resource_id=str(kpi_id)
    ).delete()

    create_audit_log(
        org=org,
        orguser=orguser,
        resource_type=AuditLogResourceType.KPI,
        resource_id=str(kpi_id),
        action=AuditLogAction.DELETE,
        resource_fields={"name": kpi_name},
    )

    return api_response(success=True)


@kpi_router.get("/{kpi_id}/dashboards/", response=list)
@has_permission(["can_view_kpis"])
def get_kpi_dashboards(request, kpi_id: int):
    """Get list of dashboards that use this KPI."""
    orguser: OrgUser = request.orguser
    try:
        return KPIService.get_kpi_dashboards(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None


@kpi_router.get("/{kpi_id}/consumers/")
@has_permission(["can_view_kpis"])
def get_kpi_consumers(request, kpi_id: int):
    """List dashboards and alerts that reference this KPI (for the consumers UI)."""
    orguser: OrgUser = request.orguser
    try:
        return KPIService.get_kpi_consumers(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None


@kpi_router.get("/{kpi_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_kpis"])
def get_kpi_data(
    request,
    kpi_id: int,
    time_grain: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    dashboard_filters: Optional[str] = None,
):
    """Get KPI chart data + echarts config.

    Optional query params:
      - time_grain: override the KPI's default time grain (daily/weekly/monthly/quarterly/yearly)
      - date_from: filter trend data from this date (ISO format, e.g. 2025-01-01)
      - date_to: filter trend data up to this date (ISO format, e.g. 2026-01-01)
      - dashboard_filters: JSON-encoded {filter_id: value} dict from dashboard
    """
    orguser: OrgUser = request.orguser

    # Parse dashboard filters JSON
    parsed_dashboard_filters = None
    if dashboard_filters:
        try:
            parsed_dashboard_filters = json.loads(dashboard_filters)
        except json.JSONDecodeError:
            logger.error(f"Invalid dashboard_filters JSON: {dashboard_filters}")

    try:
        result = KPIService.get_kpi_data(
            kpi_id,
            orguser.org,
            time_grain_override=time_grain,
            date_from=date_from,
            date_to=date_to,
            dashboard_filters=parsed_dashboard_filters,
        )
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None

    return ChartDataResponse(
        data=result["data"],
        echarts_config=result["echarts_config"],
    )


# ── Annotation Endpoints ──────────────────────────────────────────────


@kpi_router.get("/{kpi_id}/notes/", response=list[AnnotationEntryResponse])
@has_permission(["can_view_kpis"])
def list_annotations(request, kpi_id: int):
    """List all annotation entries for a KPI."""
    orguser: OrgUser = request.orguser
    try:
        return KPIService.list_annotations(kpi_id, orguser.org)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None


@kpi_router.post("/{kpi_id}/notes/", response=AnnotationEntryResponse)
@has_permission(["can_edit_kpis"])
def create_annotation(request, kpi_id: int, payload: AnnotationEntryCreate):
    """Create an annotation entry."""
    orguser: OrgUser = request.orguser
    try:
        return KPIService.create_annotation(kpi_id, orguser.org, orguser, payload)
    except KPINotFoundError:
        raise HttpError(404, "KPI not found") from None


@kpi_router.put("/{kpi_id}/notes/{entry_id}/", response=AnnotationEntryResponse)
@has_permission(["can_edit_kpis"])
def update_annotation(request, kpi_id: int, entry_id: int, payload: AnnotationEntryUpdate):
    """Update an annotation entry."""
    orguser: OrgUser = request.orguser
    try:
        return KPIService.update_annotation(kpi_id, entry_id, orguser.org, orguser, payload)
    except KPINotFoundError:
        raise HttpError(404, "Not found") from None


@kpi_router.delete("/{kpi_id}/notes/{entry_id}/")
@has_permission(["can_edit_kpis"])
def delete_annotation(request, kpi_id: int, entry_id: int):
    """Delete an annotation entry."""
    orguser: OrgUser = request.orguser
    try:
        KPIService.delete_annotation(kpi_id, entry_id, orguser.org, orguser)
    except KPINotFoundError:
        raise HttpError(404, "Not found") from None
    except KPIValidationError as e:
        raise HttpError(403, e.message) from None
    return api_response(success=True)
