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


class CommentService:
    """Service class for report comment operations"""

    @staticmethod
    def list_comments(
        snapshot_id: int,
        org: Org,
        target_type: str,
        chart_id: Optional[int] = None,
        orguser: Optional[OrgUser] = None,
    ) -> list:
        """List all comments (flat, chronological) for a target.

        Returns comments with an `is_new` attribute based on the
        requesting user's last_read_at for this target.
        """
        snapshot = CommentService._get_snapshot(snapshot_id, org)
        comments = CommentService._fetch_comments(snapshot, target_type, chart_id)
        CommentService._annotate_is_new(comments, snapshot, target_type, chart_id, orguser)
        return comments

    @staticmethod
    def _fetch_comments(
        snapshot: ReportSnapshot,
        target_type: str,
        chart_id: Optional[int],
    ) -> list:
        """Fetch comments for a target, ordered chronologically."""
        query = Q(snapshot=snapshot, target_type=target_type)

        if target_type == CommentTargetType.CHART and chart_id is not None:
            query &= Q(snapshot_chart_id=chart_id)
        elif target_type == CommentTargetType.CHART:
            raise CommentValidationError("chart_id is required for chart comments")

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
        chart_id: Optional[int],
        orguser: Optional[OrgUser],
    ) -> None:
        """Mark each comment with is_new based on the user's read cursor."""
        last_read_at = None
        if orguser:
            read_status = CommentReadStatus.objects.filter(
                user=orguser,
                snapshot=snapshot,
                target_type=target_type,
                chart_id=chart_id if target_type == CommentTargetType.CHART else None,
            ).first()
            if read_status:
                last_read_at = read_status.last_read_at

        # Own comments are never "new" (you just wrote them).
        # For others: new if created after the user's last read cursor,
        # or if the user has never opened this thread (last_read_at is None).
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
        chart_id: Optional[int] = None,
        mentioned_emails: Optional[List[str]] = None,
    ) -> Comment:
        """Create a comment on a report snapshot."""
        snapshot = CommentService._get_snapshot(snapshot_id, org)

        try:
            CommentTargetType(target_type)
        except ValueError:
            raise CommentValidationError(f"Invalid target_type: {target_type}")

        if target_type == CommentTargetType.CHART:
            if chart_id is None:
                raise CommentValidationError("chart_id is required for chart comments")
            if str(chart_id) not in (snapshot.frozen_chart_configs or {}):
                raise CommentValidationError(
                    f"Chart {chart_id} not found in snapshot {snapshot_id}"
                )

        comment = Comment.objects.create(
            target_type=target_type,
            snapshot=snapshot,
            snapshot_chart_id=chart_id if target_type == CommentTargetType.CHART else None,
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

        # Re-process mentions — always notify all mentioned users on edit
        MentionService.process_mentions(comment, org, orguser, mentioned_emails or [])

        logger.info(f"Updated comment {comment.id}")
        return comment

    @staticmethod
    def delete_comment(
        comment_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> None:
        """Soft-delete a comment. Author-only."""
        comment = CommentService._get_comment(comment_id, org)

        if comment.author != orguser:
            raise CommentPermissionError("You can only delete your own comments")

        comment.is_deleted = True
        comment.content = ""
        comment.mentioned_emails = []
        comment.save()

        logger.info(f"Soft-deleted comment {comment_id}")

    # language=SQL
    _COMMENT_STATES_SQL = """
        SELECT
            "comment".target_type,
            "comment".snapshot_chart_id AS chart_id,
            COUNT(*) AS total_count,
            SUM(
                CASE
                    WHEN crs.last_read_at IS NULL
                         OR "comment".created_at > crs.last_read_at
                    THEN 1 ELSE 0
                END
            ) AS unread_count,
            SUM(
                CASE
                    WHEN (crs.last_read_at IS NULL
                          OR "comment".created_at > crs.last_read_at)
                         AND "comment".mentioned_emails::jsonb @> %s::jsonb
                    THEN 1 ELSE 0
                END
            ) AS unread_mentioned_count
        FROM "comment"
        LEFT JOIN comment_read_status crs
            ON crs.snapshot_id = "comment".snapshot_id
            AND crs.target_type = "comment".target_type
            AND (crs.chart_id = "comment".snapshot_chart_id
                 OR (crs.chart_id IS NULL
                     AND "comment".snapshot_chart_id IS NULL))
            AND crs.user_id = %s
        WHERE "comment".snapshot_id = %s
            AND "comment".is_deleted = false
            AND "comment".target_type IN ('summary', 'chart')
        GROUP BY "comment".target_type, "comment".snapshot_chart_id
    """

    @staticmethod
    def get_comment_states(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> list:
        """Return icon state and unread count per target for the current user.

        State priority: mentioned > unread > read > none

        Returns a list of dicts, each with target_type, chart_id, state, count,
        and unread_count fields.

        Uses a single raw SQL query with a LEFT JOIN to compute all counts
        in one pass.
        """
        # Validate snapshot belongs to org
        CommentService._get_snapshot(snapshot_id, org)

        user_email_json = f'"{orguser.user.email}"'

        with connection.cursor() as cursor:
            cursor.execute(
                CommentService._COMMENT_STATES_SQL,
                [user_email_json, orguser.id, snapshot_id],
            )
            rows = cursor.fetchall()

        states = []
        for target_type, chart_id, total_count, unread_count, unread_mentioned_count in rows:
            unread = unread_count or 0
            mentioned = unread_mentioned_count or 0

            if mentioned > 0:
                state = "mentioned"
            elif unread > 0:
                state = "unread"
            else:
                state = "read"

            states.append(
                {
                    "target_type": target_type,
                    "chart_id": chart_id,
                    "state": state,
                    "count": total_count,
                    "unread_count": unread,
                }
            )

        return states

    @staticmethod
    def mark_as_read(
        snapshot_id: int,
        orguser: OrgUser,
        target_type: str,
        chart_id: Optional[int] = None,
    ) -> None:
        """Mark a target's comments as read by upserting CommentReadStatus."""
        CommentReadStatus.objects.update_or_create(
            user=orguser,
            snapshot_id=snapshot_id,
            target_type=target_type,
            chart_id=chart_id if target_type == CommentTargetType.CHART else None,
            defaults={"last_read_at": timezone.now()},
        )

    @staticmethod
    def get_mentionable_users(org: Org) -> list:
        """Return org users available for @mention."""
        return list(OrgUser.objects.filter(org=org).select_related("user").order_by("user__email"))

    # --- Private helpers ---

    @staticmethod
    def _get_snapshot(snapshot_id: int, org: Org) -> ReportSnapshot:
        """Get a snapshot, ensuring it belongs to the org."""
        try:
            return ReportSnapshot.objects.get(id=snapshot_id, org=org)
        except ReportSnapshot.DoesNotExist:
            raise CommentValidationError(f"Snapshot {snapshot_id} not found")

    @staticmethod
    def _get_comment(comment_id: int, org: Org) -> Comment:
        """Get a comment, ensuring it belongs to the org."""
        try:
            return Comment.objects.get(id=comment_id, org=org)
        except Comment.DoesNotExist:
            raise CommentNotFoundError(comment_id)
