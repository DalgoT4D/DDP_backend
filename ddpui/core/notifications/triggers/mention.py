"""@-mention notification trigger.

Fires when a report/chart/KPI comment @-mentions one or more org users. Each
mentioned user gets an in-app bell row (also delivered to the org's Discord
webhook if configured) + a mention-specific HTML email respecting their
``UserPreferences.enable_email_notifications``.
"""

from typing import Optional

from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.core.notifications.templates import render_mention_email
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.awsses import send_html_message
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.mention")


def notify_mentioned(
    author: OrgUser,
    mentioned_users: list,
    *,
    snapshot_title: str,
    report_url: str,
    comment_excerpt: str,
    chart_name: Optional[str] = None,
    thread: Optional[list] = None,
) -> None:
    """For each mentioned OrgUser: create an in-app row (via ``create_notification``,
    so org-level Discord fires if enabled) + send the mention-specific HTML email.

    Both sides are best-effort — failures are logged, never raised.
    """
    author_email = author.user.email

    for mentioned_user in mentioned_users:
        message = f'{author_email} mentioned you in a comment on "{snapshot_title}"'
        email_subject = f"You were mentioned in a comment on {snapshot_title}"

        try:
            create_notification(
                NotificationDataSchema(
                    author=author_email,
                    message=message,
                    email_subject=email_subject,
                    urgent=False,
                    scheduled_time=None,
                    recipients=[mentioned_user.id],
                    skip_email=True,
                )
            )
        except Exception as err:
            logger.error(f"mention in-app notification failed for {mentioned_user.id}: {err}")

        user_pref, _ = UserPreferences.objects.get_or_create(orguser=mentioned_user)
        if not user_pref.enable_email_notifications:
            continue

        try:
            plain_text, html_body = render_mention_email(
                author_name=author_email,
                author_email=author_email,
                comment_excerpt=comment_excerpt,
                snapshot_title=snapshot_title,
                report_url=report_url,
                thread=thread or [],
                chart_name=chart_name,
            )
            send_html_message(
                to_email=mentioned_user.user.email,
                subject=email_subject,
                text_body=plain_text,
                html_body=html_body,
            )
        except Exception as err:
            logger.error(f"mention email failed to {mentioned_user.user.email}: {err}")
