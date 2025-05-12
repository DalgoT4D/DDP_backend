import os
import re
from ninja.errors import HttpError
from django.db.models import F
from django.utils.dateparse import parse_datetime
from django.utils.timezone import now as timezone_now
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.awsses import send_text_message
from ddpui.models.tasks import (
    TaskLock,
)
from ddpui.ddpprefect import (
    prefect_service,
    MAP_FLOW_RUN_STATE_NAME_TO_TYPE,
    FLOW_RUN_CANCELLED_STATE_NAME,
    FLOW_RUN_CRASHED_STATE_NAME,
    FLOW_RUN_FAILED_STATE_NAME,
    FLOW_RUN_COMPLETED_STATE_NAME,
    FLOW_RUN_RUNNING_STATE_NAME,
    FLOW_RUN_PENDING_STATE_NAME,
)

from ddpui.settings import PRODUCTION
from ddpui.auth import SUPER_ADMIN_ROLE
from ddpui.core.notifications_service import (
    create_notification,
    get_recipients,
    SentToEnum,
    NotificationDataSchema,
)
from ddpui.utils.constants import SYSTEM_USER_EMAIL
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
    subject = f"Dalgo notification for platform admins{tag}"
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
    message = (
        f"Flow run for {org.slug} has failed with state {state}"
        "\n"
        f"\nBase plan: {org.base_plan() if org.base_plan() else 'Unknown'}"
        "\n"
        f"\nhttps://prefect.dalgo.org/flow-runs/flow-run/{flow_run_id}"
        "\n"
        f"\nAirbyte workspace URL: https://airbyte.dalgo.org/workspaces/{org.airbyte_workspace_id}"
    )
    if os.getenv("ADMIN_EMAIL"):
        send_text_message(
            os.getenv("ADMIN_EMAIL"), "Dalgo notification for platform admins", message
        )
    if os.getenv("ADMIN_DISCORD_WEBHOOK"):
        send_discord_notification(os.getenv("ADMIN_DISCORD_WEBHOOK"), message)


def get_flow_run_times(flow_run: dict) -> tuple:
    """get flow run times"""
    start_time_str = flow_run.get("start_time", "")
    if not start_time_str:
        start_time_str = flow_run.get("expected_start_time", "")
    if not start_time_str:
        start_time_str = ""
    start_time = parse_datetime(start_time_str) or timezone_now()
    expected_start_time_str = flow_run.get("expected_start_time", "")
    if not expected_start_time_str:
        expected_start_time_str = ""
    expected_start_time = parse_datetime(expected_start_time_str) or timezone_now()
    return start_time, expected_start_time


def create_or_update_flowrun(flow_run, deployment_id, state_name=""):
    """Create or update the flow run entry in database"""
    state_name = state_name if state_name else flow_run["state_name"]
    deployment_id = deployment_id if deployment_id else flow_run.get("deployment_id")

    start_time, expected_start_time = get_flow_run_times(flow_run)
    PrefectFlowRun.objects.update_or_create(
        flow_run_id=flow_run["id"],
        defaults={
            **({"deployment_id": deployment_id} if deployment_id else {}),
            "name": flow_run["name"],
            "start_time": start_time,
            "expected_start_time": expected_start_time,
            "total_run_time": flow_run["total_run_time"],
            "status": MAP_FLOW_RUN_STATE_NAME_TO_TYPE.get(
                state_name,
                MAP_FLOW_RUN_STATE_NAME_TO_TYPE.get("UNKNOWN", "unknown"),
            ),
            "state_name": state_name,
        },
    )


def lock_tasks_for_pending_deployment(deployment_id):
    """lock tasks for pending deployment"""
    system_user = OrgUser.objects.filter(user__email=SYSTEM_USER_EMAIL).first()
    if not system_user:
        logger.error(
            f"System User not found, ignoring creating locks for the deployment {deployment_id}"
        )
        return []
    locks = []
    try:
        locks = prefect_service.lock_tasks_for_deployment(deployment_id, system_user)
    except HttpError:
        logger.info("unable to lock tasks for deployment %s, ignoring", deployment_id)

    return locks


MAX_RETRIES_FOR_CRASHED_FLOW_RUNS = 1


def update_flow_run_for_deployment(deployment_id: str, state: str, flow_run: dict):
    """update flow run for deployment"""
    send_failure_notifications = True
    flow_run_id = flow_run["id"]

    if state in [
        FLOW_RUN_COMPLETED_STATE_NAME,
        FLOW_RUN_CANCELLED_STATE_NAME,
        FLOW_RUN_FAILED_STATE_NAME,
        FLOW_RUN_CRASHED_STATE_NAME,
    ]:  # terminal states
        TaskLock.objects.filter(flow_run_id=flow_run_id).delete()
        logger.info("updating the flow run in db")
        create_or_update_flowrun(flow_run, deployment_id, state)

        # retry flow run if infra went down
        if state == FLOW_RUN_CRASHED_STATE_NAME:
            prefect_flow_run = PrefectFlowRun.objects.filter(flow_run_id=flow_run_id).first()
            retry_crashed_flow_runs = os.getenv("PREFECT_RETRY_CRASHED_FLOW_RUNS", "0").lower() in [
                "1",
                "true",
            ]

            if (
                retry_crashed_flow_runs
                and prefect_flow_run
                and prefect_flow_run.retries < MAX_RETRIES_FOR_CRASHED_FLOW_RUNS
            ):
                # dont send notification right now, retry first
                try:
                    prefect_service.retry_flow_run(flow_run_id, 5)
                    PrefectFlowRun.objects.filter(flow_run_id=flow_run_id).update(
                        retries=F("retries") + 1
                    )
                    send_failure_notifications = False
                except Exception as err:
                    logger.error(
                        f"Something went wrong retrying the flow run {flow_run_id} that just crashed - {str(err)}"
                    )

    elif state == FLOW_RUN_PENDING_STATE_NAME:  # non-terminal states
        locks: list[TaskLock] = lock_tasks_for_pending_deployment(deployment_id)
        for tasklock in locks:
            logger.info("updating flow run id on locks")
            tasklock.flow_run_id = flow_run_id
            tasklock.save()

        create_or_update_flowrun(flow_run, deployment_id, state)

    elif state == FLOW_RUN_RUNNING_STATE_NAME:  # non-terminal states
        create_or_update_flowrun(flow_run, deployment_id, state)

    return send_failure_notifications


def send_failure_emails(org: Org, odf: OrgDataFlowv1 | None, flow_run_id: str, state: str):
    """send notification emails to org users"""
    name_of_deployment = odf.name if odf else "[no deployment name]"
    type_of_deployment = odf.dataflow_type if odf else "[no deployment type]"
    email_flowrun_logs_to_superadmins(org, flow_run_id)
    notify_platform_admins(org, flow_run_id, state)
    notify_org_managers(
        org,
        f"To the admins of {org.name},\n\nA job for \"{name_of_deployment}\" of type \"{type_of_deployment}\" has failed, please visit {os.getenv('FRONTEND_URL')} for more details",
        f"{org.name}: Job failure for {name_of_deployment}",
    )
    email_orgusers_ses_whitelisted(
        org,
        f'There is a problem with the pipeline "{name_of_deployment}"; we are working on a fix',
    )


def do_handle_prefect_webhook(flow_run_id: str, state: str):
    """
    this is the webhook handler for prefect flow runs
    we don't really care about the subflows inside our main flows which
    might have no deployment_id
    """
    send_failure_notifications = True
    flow_run = prefect_service.get_flow_run_poll(flow_run_id)
    logger.info("flow_run: %s", flow_run)

    deployment_id = flow_run.get("deployment_id")
    if deployment_id:
        send_failure_notifications = update_flow_run_for_deployment(deployment_id, state, flow_run)

    if state in [
        FLOW_RUN_FAILED_STATE_NAME,
        FLOW_RUN_CRASHED_STATE_NAME,
        FLOW_RUN_COMPLETED_STATE_NAME,
    ]:
        org = get_org_from_flow_run(flow_run)
        if org:
            if (
                state
                in [
                    FLOW_RUN_FAILED_STATE_NAME,
                    FLOW_RUN_CRASHED_STATE_NAME,
                ]
                and send_failure_notifications
            ):
                # odf might be None!
                odf = OrgDataFlowv1.objects.filter(org=org, deployment_id=deployment_id).first()
                send_failure_emails(org, odf, flow_run_id, state)

            elif state in [FLOW_RUN_COMPLETED_STATE_NAME]:
                email_orgusers_ses_whitelisted(org, "Your pipeline completed successfully")
