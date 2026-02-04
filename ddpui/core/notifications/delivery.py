import os
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service
from ddpui.auth import SUPER_ADMIN_ROLE
from ddpui.settings import PRODUCTION
from ddpui.core.notifications.notifications_functions import (
    create_notification,
    get_recipients,
    SentToEnum,
    NotificationDataSchema,
)
from ddpui.utils.discord import send_discord_notification

logger = CustomLogger("ddpui")


def generate_notification_email(orgname: str, flow_run_id: str, logmessages: list) -> str:
    """plantext notification email"""
    tag = " [STAGING]" if not PRODUCTION else ""
    email_body = f"""
To the admins of {orgname}{tag},

This is an automated notification from Dalgo{tag}.

Flow run id: {flow_run_id}
Logs:
"""
    email_body += "\n".join(logmessages)
    return email_body


def notify_org_managers(org: Org, message: str, email_subject: str):
    """send a notification to all users in the org"""
    error, recipients = get_recipients(
        SentToEnum.ALL_ORG_USERS, org.slug, None, manager_or_above=True
    )
    if error:
        logger.error(f"Error getting recipients: {error}")
        return
    error, response = create_notification(
        NotificationDataSchema(
            author="Dalgo", message=message, email_subject=email_subject, recipients=recipients
        )
    )
    if error:
        logger.error(f"Error creating notification: {error}")
        return
    logger.info(f"Notification created: {response}")


def notify_platform_admins(org: Org, flow_run_id: str, state: str):
    """send a notification to platform admins discord webhook"""
    prefect_url = os.getenv("PREFECT_URL_FOR_NOTIFICATIONS")
    airbyte_url = os.getenv("AIRBYTE_URL_FOR_NOTIFICATIONS")
    message = (
        f"Flow run for {org.slug} has failed with state {state}"
        "\n"
        f"\nBase plan: {org.base_plan() if org.base_plan() else 'Unknown'}"
        "\n"
        f"\n{prefect_url}/flow-runs/flow-run/{flow_run_id}"
        "\n"
        f"\nAirbyte workspace URL: {airbyte_url}/workspaces/{org.airbyte_workspace_id}"
    )
    if os.getenv("ADMIN_EMAIL"):
        send_text_message(
            os.getenv("ADMIN_EMAIL"), "Dalgo notification for platform admins", message
        )
    if os.getenv("ADMIN_DISCORD_WEBHOOK"):
        send_discord_notification(os.getenv("ADMIN_DISCORD_WEBHOOK"), message)
