"""Comment service for report comments"""

from typing import Optional

from django.db.models import Q
from django.utils import timezone

from ddpui.models.comment import Comment, CommentReadStatus
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

logger = CustomLogger("ddpui.core.comments")


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

        query = Q(
            snapshot=snapshot,
            target_type=target_type,
        )

        if target_type == "chart" and chart_id is not None:
            query &= Q(snapshot_chart_id=chart_id)
        elif target_type == "chart":
            raise CommentValidationError("chart_id is required for chart comments")

        comments = list(
            Comment.objects.filter(query)
            .select_related("author", "author__user")
            .order_by("created_at")
        )

        # Batch-resolve mentioned emails to user names
        all_emails = set()
        for c in comments:
            all_emails.update(c.mentioned_emails or [])

        users_map = {}
        if all_emails:
            for ou in OrgUser.objects.filter(
                user__email__in=all_emails
            ).select_related("user"):
                users_map[ou.user.email] = ou

        for c in comments:
            c._mentioned_users_map = users_map

        # Determine last_read_at for is_new calculation
        last_read_at = None
        if orguser:
            read_status = CommentReadStatus.objects.filter(
                user=orguser,
                snapshot=snapshot,
                target_type=target_type,
                chart_id=chart_id if target_type == "chart" else None,
            ).first()
            if read_status:
                last_read_at = read_status.last_read_at

        # Annotate comments with is_new
        for comment in comments:
            comment.is_new = (
                last_read_at is None or comment.created_at > last_read_at
            )

        return comments

    @staticmethod
    def create_comment(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
        target_type: str,
        content: str,
        chart_id: Optional[int] = None,
    ) -> Comment:
        """Create a comment on a report snapshot."""
        snapshot = CommentService._get_snapshot(snapshot_id, org)

        if target_type not in ("report", "chart"):
            raise CommentValidationError(f"Invalid target_type: {target_type}")

        if target_type == "chart" and chart_id is None:
            raise CommentValidationError("chart_id is required for chart comments")

        # Validate chart_id exists in frozen_chart_configs
        if target_type == "chart" and chart_id is not None:
            if str(chart_id) not in (snapshot.frozen_chart_configs or {}):
                raise CommentValidationError(
                    f"Chart {chart_id} not found in snapshot {snapshot_id}"
                )

        comment = Comment.objects.create(
            target_type=target_type,
            snapshot=snapshot,
            snapshot_chart_id=chart_id if target_type == "chart" else None,
            content=content,
            author=orguser,
            org=org,
        )

        # Parse mentions and create notification
        MentionService.process_mentions(comment, org, orguser)

        logger.info(f"Created comment {comment.id} on {target_type} (snapshot {snapshot_id})")
        return comment

    @staticmethod
    def update_comment(
        comment_id: int,
        org: Org,
        orguser: OrgUser,
        content: str,
    ) -> Comment:
        """Update a comment. Author-only."""
        comment = CommentService._get_comment(comment_id, org)

        if comment.author != orguser:
            raise CommentPermissionError("You can only edit your own comments")

        comment.content = content
        comment.mentioned_emails = []
        comment.save(update_fields=["content", "updated_at", "mentioned_emails"])

        # Re-process mentions
        MentionService.process_mentions(comment, org, orguser)

        logger.info(f"Updated comment {comment.id}")
        return comment

    @staticmethod
    def delete_comment(
        comment_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> None:
        """Hard-delete a comment. Author-only."""
        comment = CommentService._get_comment(comment_id, org)

        if comment.author != orguser:
            raise CommentPermissionError("You can only delete your own comments")

        comment_id_log = comment.id
        comment.delete()

        logger.info(f"Deleted comment {comment_id_log}")

    @staticmethod
    def get_comment_states(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
    ) -> dict:
        """Return icon state and unread count per target for the current user.

        State priority: mentioned > unread > read > none

        Returns: {"report": {"state": "mentioned", "count": 3}, "42": {"state": "unread", "count": 1}}
        """
        snapshot = CommentService._get_snapshot(snapshot_id, org)

        # Get all comments for this snapshot
        comments = Comment.objects.filter(
            snapshot=snapshot,
        ).values_list("target_type", "snapshot_chart_id", "created_at", "id")

        if not comments:
            return {}

        # Get all read statuses for this user + snapshot
        read_statuses = {
            (rs.target_type, rs.chart_id): rs.last_read_at
            for rs in CommentReadStatus.objects.filter(
                user=orguser, snapshot=snapshot
            )
        }

        # Get comment IDs where this user is mentioned (via JSONField)
        user_email = orguser.user.email
        mentioned_comment_ids = set(
            Comment.objects.filter(
                snapshot=snapshot,
                mentioned_emails__contains=[user_email],
            ).values_list("id", flat=True)
        )

        # Group comments by target
        targets = {}  # target_key -> list of (created_at, comment_id)
        for target_type, chart_id, created_at, comment_id in comments:
            key = "report" if target_type == "report" else str(chart_id)
            if key not in targets:
                targets[key] = []
            targets[key].append((created_at, comment_id))

        # Compute state and count per target
        states = {}
        for target_key, comment_data in targets.items():
            if target_key == "report":
                rs_key = ("report", None)
            else:
                rs_key = ("chart", int(target_key))

            last_read = read_statuses.get(rs_key)

            # Count unread comments and check for mentions
            unread_count = 0
            has_unread_mention = False
            for created_at, comment_id in comment_data:
                is_unread = last_read is None or created_at > last_read
                if is_unread:
                    unread_count += 1
                    if comment_id in mentioned_comment_ids:
                        has_unread_mention = True

            if has_unread_mention:
                state = "mentioned"
            elif unread_count > 0:
                state = "unread"
            else:
                state = "read"

            states[target_key] = {"state": state, "count": unread_count}

        return states

    @staticmethod
    def mark_as_read(
        snapshot_id: int,
        org: Org,
        orguser: OrgUser,
        target_type: str,
        chart_id: Optional[int] = None,
    ) -> None:
        """Mark a target's comments as read by upserting CommentReadStatus."""
        snapshot = CommentService._get_snapshot(snapshot_id, org)

        CommentReadStatus.objects.update_or_create(
            user=orguser,
            snapshot=snapshot,
            target_type=target_type,
            chart_id=chart_id if target_type == "chart" else None,
            defaults={"last_read_at": timezone.now()},
        )

    @staticmethod
    def get_mentionable_users(org: Org) -> list:
        """Return org users available for @mention."""
        return list(
            OrgUser.objects.filter(org=org)
            .select_related("user")
            .order_by("user__email")
        )

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
