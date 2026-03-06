"""Report API endpoints"""

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.reports.report_service import ReportService
from ddpui.core.reports.exceptions import (
    SnapshotNotFoundError,
    SnapshotValidationError,
)
from ddpui.schemas.report_schema import (
    SnapshotCreate,
    SnapshotUpdate,
    SnapshotListResponse,
    SnapshotViewResponse,
)

logger = CustomLogger("ddpui.report_api")

report_router = Router()


@report_router.get("/", response=list[SnapshotListResponse])
@has_permission(["can_view_dashboards"])
def list_snapshots(request, search: str = None):
    """List all snapshots for the organization"""
    orguser: OrgUser = request.orguser
    snapshots = ReportService.list_snapshots(orguser.org, search=search)
    return [
        SnapshotListResponse(
            id=s.id,
            title=s.title,
            dashboard_title=s.frozen_dashboard.get("title") if s.frozen_dashboard else None,
            date_column=s.date_column or None,
            period_start=s.period_start,
            period_end=s.period_end,
            status=s.status,
            summary=s.summary,
            created_by=s.created_by.user.email if s.created_by else None,
            created_at=s.created_at,
        )
        for s in snapshots
    ]


@report_router.post("/", response=SnapshotListResponse)
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
        return SnapshotListResponse(
            id=s.id,
            title=s.title,
            dashboard_title=s.frozen_dashboard.get("title") if s.frozen_dashboard else None,
            date_column=s.date_column or None,
            period_start=s.period_start,
            period_end=s.period_end,
            status=s.status,
            summary=s.summary,
            created_by=orguser.user.email,
            created_at=s.created_at,
        )
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Error creating snapshot: {e}")
        raise HttpError(500, "Failed to create snapshot") from e


@report_router.get("/{snapshot_id}/view/", response=SnapshotViewResponse)
@has_permission(["can_view_dashboards"])
def get_snapshot_view(request, snapshot_id: int):
    """Get snapshot view data for rendering"""
    orguser: OrgUser = request.orguser
    try:
        view_data = ReportService.get_snapshot_view_data(snapshot_id, orguser.org)
        return SnapshotViewResponse(**view_data)
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err


@report_router.put("/{snapshot_id}/")
@has_permission(["can_edit_dashboards"])
def update_snapshot(request, snapshot_id: int, payload: SnapshotUpdate):
    """Update a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        snapshot = ReportService.update_snapshot(
            snapshot_id, orguser.org, **payload.dict(exclude_none=True)
        )
        return {"success": True, "summary": snapshot.summary}
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err


@report_router.delete("/{snapshot_id}/")
@has_permission(["can_delete_dashboards"])
def delete_snapshot(request, snapshot_id: int):
    """Delete a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        ReportService.delete_snapshot(snapshot_id, orguser.org)
        return {"success": True}
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
