"""Mention service for comment @mentions"""

import re

from ddpui.models.comment import Comment
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.comments.mention")

# Match @email patterns like @user@example.com
MENTION_REGEX = re.compile(r"@([\w.+-]+@[\w.-]+\.\w+)")


class MentionService:
    """Handles @mention parsing and storage"""

    @staticmethod
    def process_mentions(comment: Comment, org: Org, author: OrgUser) -> list:
        """Parse mentions from content and store emails.

        Returns list of mentioned OrgUsers.
        """
        mentioned_users = MentionService.parse_mentions(comment.content, org)
        if not mentioned_users:
            return []

        MentionService.store_mentioned_emails(comment, mentioned_users)

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
