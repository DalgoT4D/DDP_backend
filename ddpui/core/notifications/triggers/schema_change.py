"""Schema-change notification trigger.

Fires from ``celeryworkers/tasks.py::detect_schema_changes_for_org`` when Airbyte
reports a breaking / non-breaking-with-transforms schema change. Only orgusers
who opted in via ``UserPreferences.enable_schema_change_notifications`` receive
the notification — everyone else is silent.

Delivery goes through ``create_notification`` (in-app row + email + org Discord).
"""

from ddpui.core.notifications.notifications_functions import create_notification
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.schemas.notifications_api_schemas import NotificationDataSchema
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.schema_change")


def notify_schema_change(org: Org, message: str, email_subject: str) -> None:
    """Fan out a schema-change notification to opted-in orgusers only."""
    recipient_ids = list(
        OrgUser.objects.filter(
            org=org,
            preferences__enable_schema_change_notifications=True,
        ).values_list("id", flat=True)
    )
    if not recipient_ids:
        logger.info(f"no opted-in recipients for schema-change notification in {org.slug}")
        return

    error, _ = create_notification(
        NotificationDataSchema(
            author="Dalgo",
            message=message,
            email_subject=email_subject,
            recipients=recipient_ids,
        )
    )
    if error:
        logger.error(f"schema-change notification failed for {org.slug}: {error}")
