"""Report API endpoints"""

import json
from typing import List, Optional

from django.http import HttpResponse
from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.reports.comment_service import CommentService
from ddpui.core.reports.exceptions import (
    CommentNotFoundError,
    CommentPermissionError,
    CommentValidationError,
    SnapshotNotFoundError,
    SnapshotPermissionError,
    SnapshotValidationError,
    SnapshotExternalServiceError,
)
from ddpui.core.reports.pdf_export_service import PdfExportService
from ddpui.core.reports.report_service import ReportService
from ddpui.models.org_user import OrgUser
from ddpui.schemas.chart_schema import ChartDataResponse
from ddpui.schemas.dashboard_schema import ShareResponse, ShareStatus, ShareToggle
from ddpui.celeryworkers.report_tasks import send_report_email_task
from ddpui.schemas.report_schema import (
    CommentCreate,
    CommentResponse,
    CommentStatesResponse,
    CommentUpdate,
    DatetimeColumnResponse,
    MarkReadRequest,
    MentionableUserResponse,
    ReportShareViaEmailRequest,
    ReportShareViaEmailResponse,
    SnapshotCreate,
    SnapshotDeleteResponse,
    SnapshotResponse,
    SnapshotUpdate,
    SnapshotUpdateResponse,
    SnapshotViewResponse,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.report_api")

report_router = Router()


@report_router.get("/", response=ApiResponse[list[SnapshotResponse]])
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
    return api_response(success=True, data=[SnapshotResponse.from_model(s) for s in snapshots])


@report_router.post("/", response=ApiResponse[SnapshotResponse])
@has_permission(["can_create_dashboards"])
def create_snapshot(request, payload: SnapshotCreate):
    """Create a new snapshot from a dashboard"""
    orguser: OrgUser = request.orguser
    try:
        s = ReportService.create_snapshot(
            title=payload.title,
            dashboard_id=payload.dashboard_id,
            orguser=orguser,
            date_column=payload.date_column.model_dump() if payload.date_column else {},
            period_end=payload.period_end,
            period_start=payload.period_start,
        )
        return api_response(
            success=True,
            data=SnapshotResponse.from_model(s),
            message="Snapshot created successfully",
        )
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Unexpected error creating snapshot: {e}", exc_info=True)
        raise HttpError(500, "Failed to create snapshot") from e


@report_router.get("/mentionable-users/", response=ApiResponse[List[MentionableUserResponse]])
@has_permission(["can_view_dashboards"])
def get_mentionable_users(request):
    """List org users available for @mention"""
    orguser: OrgUser = request.orguser

    users = CommentService.get_mentionable_users(orguser.org)
    return api_response(
        success=True,
        data=[MentionableUserResponse.from_orguser(u) for u in users],
    )


@report_router.get("/{snapshot_id}/view/", response=ApiResponse[SnapshotViewResponse])
@has_permission(["can_view_dashboards"])
def get_snapshot_view(request, snapshot_id: int):
    """Get snapshot view data for rendering"""
    orguser: OrgUser = request.orguser
    try:
        view_data = ReportService.get_snapshot_view_data(snapshot_id, orguser.org)
        return api_response(success=True, data=SnapshotViewResponse.from_view_data(view_data))
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err


@report_router.get("/{snapshot_id}/charts/{chart_id}/data/", response=ChartDataResponse)
@has_permission(["can_view_dashboards"])
def get_report_chart_data(
    request, snapshot_id: int, chart_id: int, dashboard_filters: Optional[str] = None
):
    """Get chart data for a specific chart in a report snapshot."""
    orguser: OrgUser = request.orguser
    try:
        parsed_filters = None
        if dashboard_filters:
            try:
                parsed_filters = json.loads(dashboard_filters)
            except json.JSONDecodeError:
                logger.error(f"Invalid dashboard_filters JSON: {dashboard_filters}")

        result = ReportService.get_report_chart_data(
            snapshot_id, chart_id, orguser.org, parsed_filters
        )
        return ChartDataResponse(data=result["data"], echarts_config=result["echarts_config"])
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotValidationError as err:
        raise HttpError(400, str(err)) from err
    except SnapshotExternalServiceError as err:
        raise HttpError(502, str(err)) from err
    except Exception as e:
        logger.error(f"Error getting report chart data: {e}", exc_info=True)
        raise HttpError(500, "Failed to get chart data") from e


@report_router.put("/{snapshot_id}/", response=ApiResponse[SnapshotUpdateResponse])
@has_permission(["can_edit_dashboards"])
def update_snapshot(request, snapshot_id: int, payload: SnapshotUpdate):
    """Update a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        snapshot = ReportService.update_snapshot(snapshot_id, orguser.org, payload)
        return api_response(
            success=True,
            data=SnapshotUpdateResponse.from_model(snapshot),
            message="Snapshot updated successfully",
        )
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err


@report_router.delete("/{snapshot_id}/", response=ApiResponse[SnapshotDeleteResponse])
@has_permission(["can_delete_dashboards"])
def delete_snapshot(request, snapshot_id: int):
    """Delete a snapshot"""
    orguser: OrgUser = request.orguser
    try:
        ReportService.delete_snapshot(snapshot_id, orguser.org, orguser)
        return api_response(
            success=True, data=SnapshotDeleteResponse(), message="Snapshot deleted successfully"
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
        share_token = ReportService.ensure_share_token(snapshot)
        pdf_bytes = PdfExportService.generate_pdf(snapshot_id, share_token)

        safe_title = "".join(c for c in snapshot.title if c.isalnum() or c in " -_").strip()
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
        status_data = ReportService.get_sharing_status(snapshot_id, orguser.org, orguser)
        return api_response(success=True, data=ShareStatus(**status_data))
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except SnapshotPermissionError as err:
        raise HttpError(403, str(err)) from err


# ===== Share via Email =====


@report_router.post(
    "/{snapshot_id}/share/email/",
    response=ApiResponse[ReportShareViaEmailResponse],
)
@has_permission(["can_share_dashboards"])
def share_report_via_email(request, snapshot_id: int, payload: ReportShareViaEmailRequest):
    """Send the report as a PDF attachment to the given email addresses."""
    orguser: OrgUser = request.orguser

    try:
        snapshot = ReportService.get_snapshot(snapshot_id, orguser.org)
    except SnapshotNotFoundError as err:
        raise HttpError(404, str(err)) from err

    send_report_email_task.delay(
        snapshot_id=snapshot.id,
        orguser_id=orguser.id,
        recipient_emails=payload.recipient_emails,
        subject=payload.subject,
    )

    return api_response(
        success=True,
        data=ReportShareViaEmailResponse(
            recipients_count=len(payload.recipient_emails),
            message="Emails are being sent",
        ),
    )


# ===== Comment Endpoints (nested under /{snapshot_id}/comments/) =====


@report_router.get("/{snapshot_id}/comments/states/", response=ApiResponse[CommentStatesResponse])
@has_permission(["can_view_dashboards"])
def get_comment_states(request, snapshot_id: int):
    """Get icon states for all targets in a snapshot"""
    orguser: OrgUser = request.orguser

    try:
        states = CommentService.get_comment_states(
            snapshot_id=snapshot_id,
            org=orguser.org,
            orguser=orguser,
        )
        return api_response(
            success=True,
            data=CommentStatesResponse(states=states),
        )
    except CommentValidationError as err:
        raise HttpError(400, str(err)) from err


@report_router.post("/{snapshot_id}/comments/mark-read/", response=ApiResponse)
@has_permission(["can_view_dashboards"])
def mark_as_read(request, snapshot_id: int, payload: MarkReadRequest):
    """Mark a target's comments as read"""
    orguser: OrgUser = request.orguser

    try:
        CommentService.mark_as_read(
            snapshot_id=snapshot_id,
            orguser=orguser,
            target_type=payload.target_type,
            chart_id=payload.chart_id,
        )
        return api_response(success=True, message="Marked as read")
    except CommentValidationError as err:
        raise HttpError(400, str(err)) from err


@report_router.get("/{snapshot_id}/comments/", response=ApiResponse[List[CommentResponse]])
@has_permission(["can_view_dashboards"])
def list_comments(
    request,
    snapshot_id: int,
    target_type: str,
    chart_id: int = None,
):
    """List comments for a report target"""
    orguser: OrgUser = request.orguser

    try:
        comments = CommentService.list_comments(
            snapshot_id=snapshot_id,
            org=orguser.org,
            target_type=target_type,
            chart_id=chart_id,
            orguser=orguser,
        )
        return api_response(
            success=True,
            data=[CommentResponse.from_model(c) for c in comments],
        )
    except CommentValidationError as err:
        raise HttpError(400, str(err)) from err


@report_router.post("/{snapshot_id}/comments/", response=ApiResponse[CommentResponse])
@has_permission(["can_edit_dashboards"])
def create_comment(request, snapshot_id: int, payload: CommentCreate):
    """Create a comment on a report snapshot"""
    orguser: OrgUser = request.orguser

    try:
        comment = CommentService.create_comment(
            snapshot_id=snapshot_id,
            org=orguser.org,
            orguser=orguser,
            target_type=payload.target_type,
            content=payload.content,
            chart_id=payload.chart_id,
            mentioned_emails=payload.mentioned_emails,
        )
        return api_response(
            success=True,
            data=CommentResponse.from_model(comment),
            message="Comment created",
        )
    except CommentValidationError as err:
        raise HttpError(400, str(err)) from err
    except Exception as e:
        logger.error(f"Error creating comment: {e}", exc_info=True)
        raise HttpError(500, "Failed to create comment") from e


@report_router.put("/{snapshot_id}/comments/{comment_id}/", response=ApiResponse[CommentResponse])
@has_permission(["can_edit_dashboards"])
def update_comment(request, snapshot_id: int, comment_id: int, payload: CommentUpdate):
    """Update a comment (author-only)"""
    orguser: OrgUser = request.orguser

    try:
        comment = CommentService.update_comment(
            comment_id=comment_id,
            org=orguser.org,
            orguser=orguser,
            content=payload.content,
            mentioned_emails=payload.mentioned_emails,
        )
        return api_response(
            success=True,
            data=CommentResponse.from_model(comment),
            message="Comment updated",
        )
    except CommentNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except CommentPermissionError as err:
        raise HttpError(403, str(err)) from err


@report_router.delete("/{snapshot_id}/comments/{comment_id}/", response=ApiResponse)
@has_permission(["can_edit_dashboards"])
def delete_comment(request, snapshot_id: int, comment_id: int):
    """Delete a comment (author-only)"""
    orguser: OrgUser = request.orguser

    try:
        CommentService.delete_comment(
            comment_id=comment_id,
            org=orguser.org,
            orguser=orguser,
        )
        return api_response(success=True, message="Comment deleted")
    except CommentNotFoundError as err:
        raise HttpError(404, str(err)) from err
    except CommentPermissionError as err:
        raise HttpError(403, str(err)) from err
