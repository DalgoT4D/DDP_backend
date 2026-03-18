"""Mention service for comment @mentions and notifications"""

import re

from ddpui.models.comment import Comment
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.comments.mention")

# Match @email patterns like @user@example.com
MENTION_REGEX = re.compile(r"@([\w.+-]+@[\w.-]+\.\w+)")


class MentionService:
    """Handles @mention parsing, storage, and notifications"""

    @staticmethod
    def process_mentions(comment: Comment, org: Org, author: OrgUser) -> list:
        """Parse mentions from content, store emails, and send notifications.

        Returns list of mentioned OrgUsers.
        """
        mentioned_users = MentionService.parse_mentions(comment.content, org)
        if not mentioned_users:
            return []

        MentionService.store_mentioned_emails(comment, mentioned_users)

        # Don't notify the author if they mention themselves
        users_to_notify = [u for u in mentioned_users if u != author]
        if users_to_notify:
            MentionService.send_mention_notifications(comment, users_to_notify, author)

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
    def send_mention_notifications(
        comment: Comment, mentioned_users: list, author: OrgUser
    ) -> None:
        """Create notifications for mentioned users."""
        recipient_ids = [u.id for u in mentioned_users]
        author_name = author.user.email

        snapshot_title = comment.snapshot.title if comment.snapshot else "report"
        target_desc = (
            f"chart in '{snapshot_title}'"
            if comment.target_type == "chart"
            else f"report '{snapshot_title}'"
        )

        notification_data = NotificationDataSchema(
            author=author_name,
            message=f"{author_name} mentioned you in a comment on {target_desc}: \"{comment.content[:200]}\"",
            email_subject=f"You were mentioned in a comment on {target_desc}",
            urgent=False,
            scheduled_time=None,
            recipients=recipient_ids,
        )

        error, result = create_notification(notification_data)
        if error:
            logger.error(f"Failed to create mention notification: {error}")
