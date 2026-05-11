import os
from ddpui.models.org import Org
from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger
from ddpui.auth import SUPER_ADMIN_ROLE
from ddpui.settings import PRODUCTION
from ddpui.core.notifications.notifications_functions import (
    create_notification,
    get_recipients,
    SentToEnum,
    NotificationDataSchema,
)
from ddpui.utils.discord import send_discord_embed, send_discord_notification

logger = CustomLogger("ddpui")

# Discord embed colors
_COLOR_RED = 0xE53935
_COLOR_ORANGE = 0xFB8C00


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


def notify_platform_admins(
    org: Org, flow_run_id: str, state: str, failed_step: str = "Unknown Step"
):
    """send a notification to platform admins discord webhook"""
    prefect_url = os.getenv("PREFECT_URL_FOR_NOTIFICATIONS")
    airbyte_url = os.getenv("AIRBYTE_URL_FOR_NOTIFICATIONS")
    environment = os.getenv("ENVIRONMENT", "staging")
    base_plan = org.base_plan() or "Unknown"

    prefect_run_url = f"{prefect_url}/flow-runs/flow-run/{flow_run_id}"
    airbyte_workspace_url = f"{airbyte_url}/workspaces/{org.airbyte_workspace_id}"

    plain_message = f"""Pipeline Failure Alert
Organization: {org.slug}
Failed step: {failed_step}
State: {state}
Base plan: {base_plan}
Environment: {environment}
Prefect flow run: {prefect_run_url}
Airbyte workspace URL: {airbyte_workspace_url}"""

    if os.getenv("ADMIN_EMAIL"):
        send_text_message(
            os.getenv("ADMIN_EMAIL"), "Dalgo notification for platform admins", plain_message
        )

    if os.getenv("ADMIN_DISCORD_WEBHOOK"):
        fields = [
            {"name": "Organization", "value": org.slug, "inline": True},
            {"name": "Environment", "value": environment, "inline": True},
            {"name": "Base Plan", "value": base_plan, "inline": True},
            {"name": "State", "value": state, "inline": True},
            {"name": "Failed Step", "value": failed_step, "inline": True},
            {"name": "Prefect Run", "value": prefect_run_url, "inline": False},
            {"name": "Airbyte Workspace", "value": airbyte_workspace_url, "inline": False},
        ]
        send_discord_embed(
            webhook_url=os.getenv("ADMIN_DISCORD_WEBHOOK"),
            title="Pipeline Failure Alert",
            description=f"A pipeline run has failed for **{org.name}**.",
            color=_COLOR_RED,
            fields=fields,
            footer=f"Dalgo · {environment}",
        )
