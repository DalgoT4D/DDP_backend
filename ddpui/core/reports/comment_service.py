"""Comment service for report comments"""

from typing import Optional, List

from django.db import connection
from django.db.models import Q
from django.utils import timezone

from ddpui.models.comment import Comment, CommentReadStatus, CommentTargetType
from ddpui.models.report import ReportSnapshot
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

from .exceptions import (
    CommentNotFoundError,
    CommentValidationError,
    CommentPermissionError,
)
from .mention_service import MentionService

logger = CustomLogger("comments")

# Target types that require a target_id (entity-level comments)
_ENTITY_TARGET_TYPES = {CommentTargetType.CHART, CommentTargetType.KPI}


class CommentService:
    """Service class for report comment operations"""

    @staticmethod
    def list_comments(
        snapshot_id: int,
        org: Org,
        target_type: str,
        target_id: Optional[int] = None,
        orguser: Optional[OrgUser] = None,
    ) -> list:
        """List all comments (flat, chronological) for a target."""
        snapshot = CommentService._get_snapshot(snapshot_id, org)
        comments = CommentService._fetch_comments(snapshot, target_type, target_id)
        CommentService._annotate_is_new(comments, snapshot, target_type, target_id, orguser)
        return comments

    @staticmethod
    def _fetch_comments(
        snapshot: ReportSnapshot,
        target_type: str,
        target_id: Optional[int],
    ) -> list:
        """Fetch comments for a target, ordered chronologically."""
        query = Q(snapshot=snapshot, target_type=target_type)

        if target_type in _ENTITY_TARGET_TYPES:
            if target_id is not None:
                query &= Q(target_id=target_id)
            else:
                raise CommentValidationError(f"target_id is required for {target_type} comments")

        return list(
            Comment.objects.filter(query)
            .select_related("author", "author__user")
            .order_by("created_at")
        )

    @staticmethod
    def _annotate_is_new(
        comments: list,
        snapshot: ReportSnapshot,
        target_type: str,
        target_id: Optional[int],
        orguser: Optional[OrgUser],
    ) -> None:
        """Mark each comment with is_new based on the user's read cursor."""
        last_read_at = None
        if orguser:
            read_status = CommentReadStatus.objects.filter(
                user=orguser,
                snapshot=snapshot,
                target_type=target_type,
                target_id=target_id if target_type in _ENTITY_TARGET_TYPES else None,
            ).first()
            if read_status:
                last_read_at = read_status.last_read_at

        for comment in comments:
            if orguser and comment.author_id == orguser.id:
                comment.is_new = False
            else:
                comment.is_new = last_read_at is None or comment.created_at > last_read_at

    @staticmethod
    def create_comment(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
        target_type: str,
        content: str,
        target_id: Optional[int] = None,
        mentioned_emails: Optional[List[str]] = None,
    ) -> Comment:
        """Create a comment on a report snapshot."""
        snapshot = CommentService._get_snapshot(snapshot_id, org)

        try:
            CommentTargetType(target_type)
        except ValueError:
            raise CommentValidationError(f"Invalid target_type: {target_type}")

        if target_type in _ENTITY_TARGET_TYPES:
            if target_id is None:
                raise CommentValidationError(f"target_id is required for {target_type} comments")
            if str(target_id) not in (snapshot.frozen_chart_configs or {}):
                raise CommentValidationError(
                    f"{target_type.capitalize()} {target_id} not found in snapshot {snapshot_id}"
                )

        comment = Comment.objects.create(
            target_type=target_type,
            snapshot=snapshot,
            target_id=target_id if target_type in _ENTITY_TARGET_TYPES else None,
            content=content,
            author=orguser,
            org=org,
        )

        # Process mentions and create notifications
        MentionService.process_mentions(comment, org, orguser, mentioned_emails or [])

        logger.info(f"Created comment {comment.id} on {target_type} (snapshot {snapshot_id})")
        return comment

    @staticmethod
    def update_comment(
        comment_id: int,
        org: Org,
        orguser: OrgUser,
        content: str,
        mentioned_emails: Optional[List[str]] = None,
    ) -> Comment:
        """Update a comment. Author-only."""
        comment = CommentService._get_comment(comment_id, org)

        if comment.is_deleted:
            raise CommentValidationError("Cannot edit a deleted comment")

        if comment.author != orguser:
            raise CommentPermissionError("You can only edit your own comments")

        comment.content = content
        comment.mentioned_emails = []
        comment.save()

        MentionService.process_mentions(comment, org, orguser, mentioned_emails or [])

        logger.info(f"Updated comment {comment.id}")
        return comment

    @staticmethod
    def delete_comment(
        comment_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> None:
        """Delete a comment. Author-only.

        Hard-deletes if no other user has commented in the thread (same
        snapshot + target_type + target_id). Soft-deletes otherwise.
        """
        comment = CommentService._get_comment(comment_id, org)

        if comment.author != orguser:
            raise CommentPermissionError("You can only delete your own comments")

        thread_query = Q(
            snapshot=comment.snapshot,
            target_type=comment.target_type,
        )
        if comment.target_type in _ENTITY_TARGET_TYPES:
            thread_query &= Q(target_id=comment.target_id)

        has_other_authors = Comment.objects.filter(thread_query).exclude(author=orguser).exists()

        if has_other_authors:
            comment.is_deleted = True
            comment.content = ""
            comment.mentioned_emails = []
            comment.save()
            logger.info(f"Soft-deleted comment {comment_id}")
        else:
            comment.delete()
            logger.info(f"Hard-deleted comment {comment_id}")

    # language=SQL
    _COMMENT_STATES_SQL = """
        SELECT
            "comment".target_type,
            "comment".target_id,
            CASE
                WHEN bool_or(
                    (crs.last_read_at IS NULL
                     OR "comment".created_at > crs.last_read_at)
                    AND "comment".mentioned_emails::jsonb @> %s::jsonb
                ) THEN 'mentioned'
                WHEN bool_or(
                    crs.last_read_at IS NULL
                    OR "comment".created_at > crs.last_read_at
                ) THEN 'unread'
                ELSE 'read'
            END AS state
        FROM "comment"
        LEFT JOIN comment_read_status crs
            ON crs.snapshot_id = "comment".snapshot_id
            AND crs.target_type = "comment".target_type
            AND (crs.target_id = "comment".target_id
                 OR (crs.target_id IS NULL
                     AND "comment".target_id IS NULL))
            AND crs.user_id = %s
        WHERE "comment".snapshot_id = %s
            AND "comment".is_deleted = false
            AND "comment".target_type IN ('summary', 'chart', 'kpi')
        GROUP BY "comment".target_type, "comment".target_id
    """

    @staticmethod
    def get_comment_states(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> list:
        """Return icon state per target for the current user.

        State priority: mentioned > unread > read > none

        Returns a list of dicts with target_type, target_id, and state.
        """
        CommentService._get_snapshot(snapshot_id, org)

        user_email_json = f'"{orguser.user.email}"'

        with connection.cursor() as cursor:
            cursor.execute(
                CommentService._COMMENT_STATES_SQL,
                [user_email_json, orguser.id, snapshot_id],
            )
            rows = cursor.fetchall()

        return [
            {"target_type": target_type, "target_id": target_id, "state": state}
            for target_type, target_id, state in rows
        ]

    @staticmethod
    def mark_as_read(
        snapshot_id: int,
        orguser: OrgUser,
        target_type: str,
        target_id: Optional[int] = None,
    ) -> None:
        """Mark a target's comments as read by upserting CommentReadStatus."""
        CommentReadStatus.objects.update_or_create(
            user=orguser,
            snapshot_id=snapshot_id,
            target_type=target_type,
            target_id=target_id if target_type in _ENTITY_TARGET_TYPES else None,
            defaults={"last_read_at": timezone.now()},
        )

    @staticmethod
    def get_mentionable_users(org: Org) -> list:
        """Return org users available for @mention."""
        return list(OrgUser.objects.filter(org=org).select_related("user").order_by("user__email"))

    # --- Private helpers ---

    @staticmethod
    def _get_snapshot(snapshot_id: int, org: Org) -> ReportSnapshot:
        try:
            return ReportSnapshot.objects.get(id=snapshot_id, org=org)
        except ReportSnapshot.DoesNotExist:
            raise CommentValidationError(f"Snapshot {snapshot_id} not found")

    @staticmethod
    def _get_comment(comment_id: int, org: Org) -> Comment:
        try:
            return Comment.objects.get(id=comment_id, org=org)
        except Comment.DoesNotExist:
            raise CommentNotFoundError(comment_id)
