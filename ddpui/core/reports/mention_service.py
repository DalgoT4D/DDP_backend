"""Mention service for comment @mentions"""

from typing import Optional, List

from django.conf import settings

from ddpui.core.notifications.triggers.mention import notify_mentioned
from ddpui.models.comment import Comment, CommentTargetType
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("comments")


class MentionService:
    """Handles @mention storage and notification dispatch"""

    @staticmethod
    def process_mentions(
        comment: Comment,
        org: Org,
        author: OrgUser,
        mentioned_emails: List[str],
    ) -> list:
        """Validate mentioned emails, store them, and dispatch notifications.

        Args:
            comment: The comment containing @mentions
            org: The organization
            author: The comment author
            mentioned_emails: List of email addresses mentioned by the frontend

        Returns list of mentioned OrgUsers.
        """
        if not mentioned_emails:
            return []

        # Validate that mentioned emails belong to real org users
        mentioned_users = list(
            OrgUser.objects.filter(
                org=org,
                user__email__in=mentioned_emails,
            ).select_related("user")
        )
        if not mentioned_users:
            return []

        MentionService.store_mentioned_emails(comment, mentioned_users)

        MentionService.notify_mentioned_users(
            comment=comment,
            org=org,
            author=author,
            mentioned_users=mentioned_users,
        )

        return mentioned_users

    @staticmethod
    def store_mentioned_emails(comment: Comment, users: list) -> None:
        """Store mentioned user emails in the comment's JSONField."""
        comment.mentioned_emails = list(set(u.user.email for u in users))
        comment.save()

    @staticmethod
    def notify_mentioned_users(
        comment: Comment,
        org: Org,
        author: OrgUser,
        mentioned_users: list,
    ) -> None:
        """Shape the mention payload and hand it to the notifications trigger.

        The trigger (``triggers.mention.notify_mentioned``) owns the actual
        fan-out: in-app row per user + specialized email (respecting the
        recipient's ``enable_email_notifications`` preference).
        """
        excerpt = comment.content[:500]
        if len(comment.content) > 500:
            excerpt += "..."

        notify_mentioned(
            author=author,
            mentioned_users=mentioned_users,
            snapshot_title=comment.snapshot.title if comment.snapshot else "Report",
            report_url=MentionService._build_report_url(comment),
            comment_excerpt=excerpt,
            chart_name=MentionService._resolve_chart_name(comment),
            thread=MentionService._get_thread_context(comment),
        )

    @staticmethod
    def _build_report_url(comment: Comment) -> str:
        """Build a deep link URL that opens the correct comment panel."""
        frontend_url = (
            getattr(settings, "FRONTEND_URL_V2", None)
            or getattr(settings, "FRONTEND_URL", None)
            or "http://localhost:3001"
        )
        if (
            comment.target_type in (CommentTargetType.CHART, CommentTargetType.KPI)
            and comment.target_id is not None
        ):
            return (
                f"{frontend_url}/reports/{comment.snapshot_id}"
                f"?commentTarget={comment.target_type}&chartId={comment.target_id}"
            )
        return f"{frontend_url}/reports/{comment.snapshot_id}?commentTarget=summary"

    @staticmethod
    def _resolve_chart_name(comment: Comment) -> Optional[str]:
        """Resolve entity name from the snapshot's frozen config."""
        if (
            comment.target_type in (CommentTargetType.CHART, CommentTargetType.KPI)
            and comment.target_id is not None
            and comment.snapshot
        ):
            config = (comment.snapshot.frozen_chart_configs or {}).get(str(comment.target_id), {})
            return config.get("title")
        return None

    @staticmethod
    def _get_thread_context(comment: Comment, max_prior: int = 3) -> list:
        """Fetch recent comments on the same target before this comment.

        Returns list of dicts: [{"author_name": ..., "author_email": ..., "content": ...}]
        """
        query = Comment.objects.filter(
            snapshot=comment.snapshot,
            target_type=comment.target_type,
            is_deleted=False,
            created_at__lt=comment.created_at,
        ).select_related("author", "author__user")

        if comment.target_type in (CommentTargetType.CHART, CommentTargetType.KPI):
            query = query.filter(target_id=comment.target_id)

        # Get last N comments before this one, ordered oldest first
        prior_comments = list(query.order_by("-created_at")[:max_prior])
        prior_comments.reverse()

        thread = []
        for c in prior_comments:
            content = c.content[:200]
            if len(c.content) > 200:
                content += "..."
            thread.append(
                {
                    "author_name": c.author.user.email,
                    "author_email": c.author.user.email,
                    "content": content,
                }
            )
        return thread
