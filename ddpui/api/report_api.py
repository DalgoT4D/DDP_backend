"""Report API endpoints"""

import secrets
from typing import List

from django.http import HttpResponse
from django.utils import timezone
from ninja import Router
from ninja.errors import HttpError

from ddpui.api.filter_api import determine_filter_type_from_column, get_table_columns
from ddpui.auth import has_permission
from ddpui.core.charts.charts_service import get_warehouse_client
from ddpui.core.reports.exceptions import (
    SnapshotNotFoundError,
    SnapshotPermissionError,
    SnapshotValidationError,
)
from ddpui.core.reports.pdf_export_service import PdfExportService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.visualization import Chart
from ddpui.schemas.dashboard_schema import ShareResponse, ShareStatus, ShareToggle
from ddpui.schemas.report_schema import (
    DatetimeColumnResponse,
    SnapshotCreate,
    SnapshotDeleteResponse,
    SnapshotListResponse,
    SnapshotUpdate,
    SnapshotUpdateResponse,
    SnapshotViewResponse,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.report_api")

report_router = Router()


@report_router.get("/", response=ApiResponse[list[SnapshotListResponse]])
@has_permission(["can_view_dashboards"])
def list_snapshots(
    request,
    search: str = None,
    dashboard_title: str = None,
    created_by: str = None,
):
    """List all snapshots for the organization"""
    orguser: OrgUser = request.orguser
    snapshots = ReportService.list_snapshots(
        orguser.org,
        search=search,
        dashboard_title=dashboard_title,
        created_by_email=created_by,
    )
    return api_response(
        success=True,
        data=[SnapshotListResponse.from_model(s) for s in snapshots]
    )


@report_router.post("/", response=ApiResponse[SnapshotListResponse])
@has_permission(["can_create_dashboards"])
def create_snapshot(request, payload: SnapshotCreate):
    """Create a new snapshot from a dashboard"""
    orguser: OrgUser = request.orguser
    try:
        s = ReportService.create_snapshot(
            title=payload.title,
            dashboard_id=payload.dashboard_id,
            date_column=payload.date_column.dict(),
            period_end=payload.period_end,
            orguser=orguser,
            period_start=payload.period_start,
        )
        return api_response(
            success=True,
            data=SnapshotListResponse.from_model(s),
            message="Snapshot created successfully"
        )
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Unexpected error creating snapshot: {e}", exc_info=True)
        raise HttpError(500, "Failed to create snapshot") from e


@report_router.get("/{snapshot_id}/view/", response=ApiResponse[SnapshotViewResponse])
@has_permission(["can_view_dashboards"])
def get_snapshot_view(request, snapshot_id: int):
    """Get snapshot view data for rendering"""
    orguser: OrgUser = request.orguser
    try:
        view_data = ReportService.get_snapshot_view_data(snapshot_id, orguser.org)
        return api_response(
            success=True,
            data=SnapshotViewResponse(**view_data)
        )
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err


@report_router.put("/{snapshot_id}/", response=ApiResponse[SnapshotUpdateResponse])
@has_permission(["can_edit_dashboards"])
def update_snapshot(request, snapshot_id: int, payload: SnapshotUpdate):
    """Update a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        snapshot = ReportService.update_snapshot(
            snapshot_id, orguser.org, **payload.dict(exclude_none=True)
        )
        return api_response(
            success=True,
            data=SnapshotUpdateResponse(summary=snapshot.summary),
            message="Snapshot updated successfully"
        )
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err


@report_router.delete("/{snapshot_id}/", response=ApiResponse[SnapshotDeleteResponse])
@has_permission(["can_delete_dashboards"])
def delete_snapshot(request, snapshot_id: int):
    """Delete a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        ReportService.delete_snapshot(snapshot_id, orguser.org, orguser)
        return api_response(
            success=True,
            data=SnapshotDeleteResponse(),
            message="Snapshot deleted successfully"
        )
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotPermissionError as err:
        raise HttpError(403, str(err)) from err


# ===== PDF Export =====


@report_router.post("/{snapshot_id}/export/pdf/", response={200: None})
@has_permission(["can_view_dashboards"])
def export_report_pdf(request, snapshot_id: int):
    """Generate PDF of report via Playwright and return as download.

    Uses an X-Render-Secret header (injected by Playwright route
    interception) so the public report endpoints serve data without
    the snapshot needing is_public=True.  No public state is toggled.
    """
    orguser: OrgUser = request.orguser

    try:
        snapshot = ReportService.get_snapshot(snapshot_id, orguser.org)
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err

    try:
        # Ensure the snapshot has a share token (needed for the URL).
        # This does NOT make the report publicly accessible — Playwright
        # authenticates via the render secret header, not is_public.
        if not snapshot.public_share_token:
            snapshot.public_share_token = secrets.token_urlsafe(48)
            snapshot.save(update_fields=["public_share_token"])

        pdf_bytes = PdfExportService.generate_pdf(
            snapshot_id, snapshot.public_share_token
        )

        safe_title = "".join(
            c for c in snapshot.title if c.isalnum() or c in " -_"
        ).strip()
        filename = f"{safe_title or 'report'}.pdf"

        response = HttpResponse(pdf_bytes, content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    except Exception as e:
        logger.error(f"PDF export failed for snapshot {snapshot_id}: {e}", exc_info=True)
        raise HttpError(500, "Failed to generate PDF") from e


# ===== Datetime Column Discovery =====


@report_router.get(
    "/dashboards/{dashboard_id}/datetime-columns/",
    response=ApiResponse[List[DatetimeColumnResponse]],
)
@has_permission(["can_view_dashboards"])
def list_dashboard_datetime_columns(request, dashboard_id: int):
    """Discover datetime columns from all tables used by a dashboard's charts.

    Warehouse introspection (get_table_columns + determine_filter_type_from_column)
    is needed because dashboard filters only cover columns the user has already
    configured. A chart's underlying table may have additional datetime columns
    (e.g. updated_at, shipped_at) that aren't dashboard filters yet but are
    valid choices for filtering a report by date range.
    """
    orguser: OrgUser = request.orguser

    try:
        dashboard = Dashboard.objects.prefetch_related("filters").get(
            id=dashboard_id, org=orguser.org
        )
    except Dashboard.DoesNotExist:
        raise HttpError(404, f"Dashboard {dashboard_id} not found")

    # Extract unique (schema_name, table_name) from chart components
    components = dashboard.components or {}
    chart_ids = []
    for comp_id, component in components.items():
        if component.get("type") == "chart":
            chart_id = component.get("config", {}).get("chartId")
            if chart_id:
                chart_ids.append(chart_id)

    charts = Chart.objects.filter(id__in=chart_ids, org=orguser.org)
    table_refs = set()
    for chart in charts:
        if chart.schema_name and chart.table_name:
            table_refs.add((chart.schema_name, chart.table_name))

    if not table_refs:
        return api_response(success=True, data=[])

    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = get_warehouse_client(org_warehouse)
    except Exception as e:
        logger.error(f"Error connecting to warehouse: {e}", exc_info=True)
        raise HttpError(500, "Error connecting to warehouse") from e

    # Collect existing dashboard datetime filter keys for flagging
    dashboard_filter_keys = set()
    for f in dashboard.filters.filter(filter_type="datetime"):
        dashboard_filter_keys.add((f.schema_name, f.table_name, f.column_name))

    # Discover datetime columns from each table
    seen = set()
    datetime_columns = []

    for schema_name, table_name in table_refs:
        try:
            columns = get_table_columns(
                warehouse_client, org_warehouse, schema_name, table_name
            )
            for col in columns:
                filter_type = determine_filter_type_from_column(col["data_type"])
                if filter_type == "datetime":
                    key = (schema_name, table_name, col["column_name"])
                    if key not in seen:
                        seen.add(key)
                        datetime_columns.append(
                            DatetimeColumnResponse(
                                schema_name=schema_name,
                                table_name=table_name,
                                column_name=col["column_name"],
                                data_type=col["data_type"],
                                is_dashboard_filter=key in dashboard_filter_keys,
                            )
                        )
        except Exception as e:
            logger.warning(
                f"Error fetching columns for {schema_name}.{table_name}: {e}",
                exc_info=True
            )

    # Also include existing dashboard datetime filters not already discovered
    for f in dashboard.filters.filter(filter_type="datetime"):
        key = (f.schema_name, f.table_name, f.column_name)
        if key not in seen:
            seen.add(key)
            datetime_columns.append(
                DatetimeColumnResponse(
                    schema_name=f.schema_name,
                    table_name=f.table_name,
                    column_name=f.column_name,
                    data_type="datetime",
                    is_dashboard_filter=True,
                )
            )

    return api_response(success=True, data=datetime_columns)


# ===== Report Sharing Endpoints (same pattern as Dashboard) =====


@report_router.put("/{snapshot_id}/share/", response=ShareResponse)
@has_permission(["can_share_dashboards"])
def toggle_report_sharing(request, snapshot_id: int, payload: ShareToggle):
    """Toggle public sharing for a report snapshot"""
    orguser: OrgUser = request.orguser

    try:
        snapshot = ReportService.get_snapshot(snapshot_id, orguser.org)
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err

    # Check permissions - only snapshot creator can modify sharing
    if snapshot.created_by != orguser:
        raise HttpError(403, "Only report creators can modify sharing settings")

    is_public = payload.is_public

    if is_public:
        if not snapshot.public_share_token:
            snapshot.public_share_token = secrets.token_urlsafe(48)
        snapshot.public_shared_at = timezone.now()
        snapshot.public_disabled_at = None
    else:
        snapshot.public_disabled_at = timezone.now()

    snapshot.is_public = is_public
    snapshot.save()

    # Build response
    response_data = {
        "is_public": snapshot.is_public,
        "message": f'Report {"made public" if is_public else "made private"}',
    }

    if snapshot.is_public and snapshot.public_share_token:
        from django.conf import settings

        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(
            settings, "FRONTEND_URL", "http://localhost:3001"
        )
        response_data["public_url"] = (
            f"{frontend_url}/share/report/{snapshot.public_share_token}"
        )
        response_data["public_share_token"] = snapshot.public_share_token

    logger.info(
        f"Report {snapshot_id} sharing {'enabled' if is_public else 'disabled'} "
        f"by user {orguser.user.email}, token: {snapshot.public_share_token}"
    )

    return ShareResponse(**response_data)


@report_router.get("/{snapshot_id}/share/", response=ShareStatus)
@has_permission(["can_view_dashboards"])
def get_report_sharing_status(request, snapshot_id: int):
    """Get report sharing status"""
    orguser: OrgUser = request.orguser

    try:
        snapshot = ReportService.get_snapshot(snapshot_id, orguser.org)
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err

    if snapshot.created_by != orguser:
        raise HttpError(403, "Only report creators can view sharing settings")

    response_data = {
        "is_public": snapshot.is_public,
        "public_access_count": snapshot.public_access_count,
        "last_public_accessed": snapshot.last_public_accessed,
        "public_shared_at": snapshot.public_shared_at,
    }

    if snapshot.is_public and snapshot.public_share_token:
        from django.conf import settings

        FRONTEND_URL_V2 = getattr(settings, "FRONTEND_URL_V2", None)
        frontend_url = FRONTEND_URL_V2 or getattr(
            settings, "FRONTEND_URL", "http://localhost:3001"
        )
        response_data["public_url"] = (
            f"{frontend_url}/share/report/{snapshot.public_share_token}"
        )

    return ShareStatus(**response_data)
