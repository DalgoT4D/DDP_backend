"""Mention service for comment @mentions"""

import re
from datetime import datetime
from typing import Optional

from django.conf import settings

from ddpui.models.comment import Comment, CommentTargetType
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.utils import timezone
from ddpui.utils.awsses import send_html_message
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.email_templates import render_mention_email

logger = CustomLogger("ddpui.core.comments.mention")

# Match @email patterns like @user@example.com
MENTION_REGEX = re.compile(r"@([\w.+-]+@[\w.-]+\.\w+)")


def _get_display_name(orguser: OrgUser) -> str:
    """Get display name for an OrgUser, falling back to email."""
    user = orguser.user
    parts = [user.first_name, user.last_name]
    name = " ".join(p for p in parts if p).strip()
    return name or user.email


class MentionService:
    """Handles @mention parsing, storage, and notification dispatch"""

    @staticmethod
    def process_mentions(
        comment: Comment,
        org: Org,
        author: OrgUser,
        previous_emails: Optional[list] = None,
    ) -> list:
        """Parse mentions from content, store emails, and dispatch notifications.

        Args:
            comment: The comment containing @mentions
            org: The organization
            author: The comment author
            previous_emails: If provided (on update), only NEW mentions trigger notifications

        Returns list of mentioned OrgUsers.
        """
        mentioned_users = MentionService.parse_mentions(comment.content, org)
        if not mentioned_users:
            return []

        MentionService.store_mentioned_emails(comment, mentioned_users)

        # Determine which users are NEW mentions (for notifications)
        if previous_emails is not None:
            previous_set = set(previous_emails)
            new_mentions = [u for u in mentioned_users if u.user.email not in previous_set]
        else:
            new_mentions = mentioned_users

        # Dispatch notifications for new mentions
        if new_mentions:
            MentionService.notify_mentioned_users(
                comment=comment,
                org=org,
                author=author,
                mentioned_users=new_mentions,
            )

        return mentioned_users

    @staticmethod
    def parse_mentions(content: str, org: Org) -> list:
        """Extract @email patterns and resolve to OrgUser records."""
        emails = MENTION_REGEX.findall(content)
        if not emails:
            return []

        return list(
            OrgUser.objects.filter(
                org=org,
                user__email__in=emails,
            ).select_related("user")
        )

    @staticmethod
    def store_mentioned_emails(comment: Comment, users: list) -> None:
        """Store mentioned user emails in the comment's JSONField."""
        comment.mentioned_emails = list(set(u.user.email for u in users))
        comment.save(update_fields=["mentioned_emails"])

    @staticmethod
    def notify_mentioned_users(
        comment: Comment,
        org: Org,
        author: OrgUser,
        mentioned_users: list,
    ) -> None:
        """Create in-app notifications and send emails for @mentioned users.

        Skips self-mentions. Respects UserPreferences.enable_email_notifications.
        Email failures are logged but do not block notification creation.
        """
        frontend_url = (
            getattr(settings, "FRONTEND_URL_V2", None)
            or getattr(settings, "FRONTEND_URL", None)
            or "http://localhost:3001"
        )
        report_url = f"{frontend_url}/reports/{comment.snapshot_id}"
        snapshot_title = comment.snapshot.title if comment.snapshot else "Report"
        author_name = _get_display_name(author)

        # Resolve chart name from frozen config
        chart_name = None
        if (
            comment.target_type == CommentTargetType.CHART
            and comment.snapshot_chart_id is not None
            and comment.snapshot
        ):
            chart_config = (comment.snapshot.frozen_chart_configs or {}).get(
                str(comment.snapshot_chart_id), {}
            )
            chart_name = chart_config.get("title")

        for mentioned_user in mentioned_users:
            # Skip self-mentions
            if mentioned_user.id == author.id:
                continue

            message = f'{author_name} mentioned you in a comment on "{snapshot_title}"'
            email_subject = f"You were mentioned in a comment on {snapshot_title}"

            notification = Notification.objects.create(
                author=author.user.email,
                message=message,
                email_subject=email_subject,
                urgent=False,
                sent_time=timezone.as_utc(datetime.now()),
            )

            NotificationRecipient.objects.create(
                notification=notification,
                recipient=mentioned_user,
            )

            # Send email if user has email notifications enabled
            user_pref, _ = UserPreferences.objects.get_or_create(orguser=mentioned_user)
            if user_pref.enable_email_notifications:
                try:
                    # Fetch recent thread context (up to 3 prior comments on same target)
                    thread = MentionService._get_thread_context(comment)

                    excerpt = comment.content[:500]
                    if len(comment.content) > 500:
                        excerpt += "..."

                    plain_text, html_body = render_mention_email(
                        author_name=author_name,
                        author_email=author.user.email,
                        comment_excerpt=excerpt,
                        snapshot_title=snapshot_title,
                        report_url=report_url,
                        thread=thread,
                        chart_name=chart_name,
                    )

                    send_html_message(
                        to_email=mentioned_user.user.email,
                        subject=email_subject,
                        text_body=plain_text,
                        html_body=html_body,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to send mention email to {mentioned_user.user.email}: {e}"
                    )

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

        if comment.target_type == CommentTargetType.CHART:
            query = query.filter(snapshot_chart_id=comment.snapshot_chart_id)

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
                    "author_name": _get_display_name(c.author),
                    "author_email": c.author.user.email,
                    "content": content,
                }
            )
        return thread
