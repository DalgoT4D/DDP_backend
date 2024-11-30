import re
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.utils.awsses import send_text_message
from ddpui.ddpprefect import prefect_service
from ddpui.settings import PRODUCTION
from ddpui.auth import SUPER_ADMIN_ROLE
from ddpui.core.notifications_service import (
    create_notification,
    get_recipients,
    SentToEnum,
    NotificationDataSchema,
)
from ddpui.utils.discord import send_discord_notification

logger = CustomLogger("ddpui")

FLOW_RUN = "flow-run"
FLOW = "flow"
DEPLOYMENT = "deployment"


def get_message_type(message_object: dict) -> str | None:
    """identifies the message type if possible"""
    if message_object.get("state"):
        if message_object["state"].get("state_details"):
            if message_object["state"]["state_details"].get("flow_run_id") == message_object["id"]:
                return FLOW_RUN

    return None


def get_flowrun_id_and_state(message: str) -> tuple:
    """Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}"""
    match = re.search(
        "Flow run ([a-zA-Z0-9-]+) with id ([a-zA-Z0-9-]+) entered state ([a-zA-Z0-9]+)",
        message,
    )
    if match:
        flow_run_id = match.groups()[1]
        state_message = match.groups()[2]
        return flow_run_id, state_message
    return None, None


def get_org_from_flow_run(flow_run: dict) -> Org | None:
    """
    (**deprecated
    given a flow-run, inspect its parameters
    if it has parameters and one of them is `airbyte_connection`, then
    look up the OrgPrefectBlock by its prefect block-document id
    now we have the org
    )

    org_slug is embedded in the parameters of flow run now
    """
    if (
        "parameters" in flow_run
        and "config" in flow_run["parameters"]
        and "org_slug" in flow_run["parameters"]["config"]
    ):
        org = Org.objects.filter(slug=flow_run["parameters"]["config"]["org_slug"]).first()

        if org is not None:
            logger.info(f"found the org slug {org.slug} inside the webhook function")
            return org

    logger.error("didn't find the org slug inside the webhook function")

    return None


def generate_notification_email(orgname: str, flow_run_id: str, logmessages: list) -> str:
    """until we make a sendgrid template"""
    tag = " [STAGING]" if not PRODUCTION else ""
    email_body = f"""
To the admins of {orgname}{tag},

This is an automated notification from Dalgo{tag}.

Flow run id: {flow_run_id}
Logs:
"""
    email_body += "\n".join(logmessages)
    return email_body


def email_superadmins(org: Org, email_body: str):
    """sends a notificationemail to all OrgUsers"""
    tag = " [STAGING]" if not PRODUCTION else ""
    subject = f"Dalgo notification{tag}"
    for orguser in OrgUser.objects.filter(
        org=org,
        new_role__slug=SUPER_ADMIN_ROLE,
    ).all():
        logger.info(f"sending prefect-notification email to {orguser.user.email}")
        send_text_message(orguser.user.email, subject, email_body)


def email_orgusers_ses_whitelisted(org: Org, email_body: str):
    """sends a notificationemail to all OrgUsers"""
    if org.ses_whitelisted_email:
        tag = " [STAGING]" if not PRODUCTION else ""
        subject = f"Dalgo notification{tag}"
        logger.info(f"sending email to {org.ses_whitelisted_email}")
        send_text_message(
            org.ses_whitelisted_email,
            subject,
            email_body,
        )


def email_flowrun_logs_to_superadmins(org: Org, flow_run_id: str):
    """retrieves logs for a flow-run and emails them to all users for the org"""
    logs_arr = prefect_service.recurse_flow_run_logs(flow_run_id)
    logmessages = [x["message"] for x in logs_arr]
    email_body = generate_notification_email(org.name, flow_run_id, logmessages)
    email_superadmins(org, email_body)


def notify_org_managers(org: Org, message: str):
    """send a notification to all users in the org"""
    error, recipients = get_recipients(
        SentToEnum.ALL_ORG_USERS, org.slug, None, manager_or_above=True
    )
    if error:
        logger.error(f"Error getting recipients: {error}")
        return
    error, response = create_notification(
        NotificationDataSchema(author="Dalgo", message=message, recipients=recipients)
    )
    if error:
        logger.error(f"Error creating notification: {error}")
        return
    logger.info(f"Notification created: {response}")

    if hasattr(org, "preferences"):
        orgpreferences: OrgPreferences = org.preferences
        if orgpreferences.enable_discord_notifications and orgpreferences.discord_webhook:
            try:
                send_discord_notification(orgpreferences.discord_webhook, message)
            except Exception as e:
                logger.error(f"Error sending discord message: {e}")
