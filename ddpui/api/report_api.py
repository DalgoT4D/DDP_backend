"""Report API endpoints"""

import secrets
from typing import List

from django.http import HttpResponse
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.reports.exceptions import (
    SnapshotNotFoundError,
    SnapshotPermissionError,
    SnapshotValidationError,
    SnapshotExternalServiceError,
)
from ddpui.core.reports.pdf_export_service import PdfExportService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.org_user import OrgUser
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
            data=SnapshotViewResponse.from_view_data(view_data)
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
            data=SnapshotUpdateResponse.from_model(snapshot),
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
    """Discover datetime columns from all tables used by a dashboard's charts."""
    orguser: OrgUser = request.orguser
    try:
        columns = ReportService.discover_datetime_columns(dashboard_id, orguser.org)
        return api_response(success=True, data=columns)
    except SnapshotValidationError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotExternalServiceError as err:
        raise HttpError(502, str(err)) from err


# ===== Report Sharing Endpoints (same pattern as Dashboard) =====


@report_router.put("/{snapshot_id}/share/", response=ApiResponse[ShareResponse])
@has_permission(["can_share_dashboards"])
def toggle_report_sharing(request, snapshot_id: int, payload: ShareToggle):
    """Toggle public sharing for a report snapshot"""
    orguser: OrgUser = request.orguser
    try:
        snapshot = ReportService.toggle_sharing(
            snapshot_id, orguser.org, orguser, payload.is_public
        )
        response_data = ReportService.build_share_response(snapshot)
        return api_response(success=True, data=ShareResponse(**response_data))
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotPermissionError as err:
        raise HttpError(403, str(err)) from err


@report_router.get("/{snapshot_id}/share/", response=ApiResponse[ShareStatus])
@has_permission(["can_view_dashboards"])
def get_report_sharing_status(request, snapshot_id: int):
    """Get report sharing status"""
    orguser: OrgUser = request.orguser
    try:
        status_data = ReportService.get_sharing_status(
            snapshot_id, orguser.org, orguser
        )
        return api_response(success=True, data=ShareStatus(**status_data))
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotPermissionError as err:
        raise HttpError(403, str(err)) from err
