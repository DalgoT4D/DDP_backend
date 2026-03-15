"""Comment API endpoints for report comments"""

from typing import List

from ninja import Router
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.core.comments.comment_service import CommentService
from ddpui.core.comments.exceptions import (
    CommentNotFoundError,
    CommentValidationError,
    CommentPermissionError,
)
from ddpui.models.org_user import OrgUser
from ddpui.schemas.comment_schema import (
    CommentCreate,
    CommentUpdate,
    CommentResponse,
    CommentStatesResponse,
    MarkReadRequest,
    MentionableUserResponse,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.response_wrapper import ApiResponse, api_response

logger = CustomLogger("ddpui.comments_api")

comments_router = Router()


# --- Named routes MUST come before /{comment_id}/ to avoid 405 ---


@comments_router.get("/states/", response=ApiResponse[CommentStatesResponse])
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


@comments_router.get(
    "/mentionable-users/", response=ApiResponse[List[MentionableUserResponse]]
)
@has_permission(["can_view_dashboards"])
def get_mentionable_users(request):
    """List org users available for @mention"""
    orguser: OrgUser = request.orguser

    users = CommentService.get_mentionable_users(orguser.org)
    return api_response(
        success=True,
        data=[MentionableUserResponse.from_orguser(u) for u in users],
    )


@comments_router.post("/mark-read/", response=ApiResponse)
@has_permission(["can_view_dashboards"])
def mark_as_read(request, payload: MarkReadRequest):
    """Mark a target's comments as read"""
    orguser: OrgUser = request.orguser

    try:
        CommentService.mark_as_read(
            snapshot_id=payload.snapshot_id,
            org=orguser.org,
            orguser=orguser,
            target_type=payload.target_type,
            chart_id=payload.chart_id,
        )
        return api_response(success=True, message="Marked as read")
    except CommentValidationError as err:
        raise HttpError(400, str(err)) from err


# --- Root and parameterized routes ---


@comments_router.get("/", response=ApiResponse[List[CommentResponse]])
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


@comments_router.post("/", response=ApiResponse[CommentResponse])
@has_permission(["can_view_dashboards"])
def create_comment(request, payload: CommentCreate):
    """Create a comment on a report snapshot"""
    orguser: OrgUser = request.orguser

    try:
        comment = CommentService.create_comment(
            snapshot_id=payload.snapshot_id,
            org=orguser.org,
            orguser=orguser,
            target_type=payload.target_type,
            content=payload.content,
            chart_id=payload.chart_id,
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


@comments_router.put("/{comment_id}/", response=ApiResponse[CommentResponse])
@has_permission(["can_view_dashboards"])
def update_comment(request, comment_id: int, payload: CommentUpdate):
    """Update a comment (author-only)"""
    orguser: OrgUser = request.orguser

    try:
        comment = CommentService.update_comment(
            comment_id=comment_id,
            org=orguser.org,
            orguser=orguser,
            content=payload.content,
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


@comments_router.delete("/{comment_id}/", response=ApiResponse)
@has_permission(["can_view_dashboards"])
def delete_comment(request, comment_id: int):
    """Soft-delete a comment (author-only)"""
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
