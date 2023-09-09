import os
import json
import re
from ninja import NinjaAPI

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.api.client.prefect_api import prefect_service
from ddpui.models.org import OrgPrefectBlock, Org
from ddpui.models.orgjobs import BlockLock
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.utils.sendgrid import send_text_message

webhookapi = NinjaAPI(urls_namespace="webhook")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")

FLOW_RUN = "flow-run"
FLOW = "flow"
DEPLOYMENT = "deployment"


def get_message_type(message_object: dict) -> str | None:
    """identifies the message type if possible"""
    if message_object.get("state"):
        if message_object["state"].get("state_details"):
            if (
                message_object["state"]["state_details"].get("flow_run_id")
                == message_object["id"]
            ):
                return FLOW_RUN

    return None


def get_flow_run_id_from_logs(message: str) -> str | None:
    """uses regex matching to try to get a flow-run id"""
    match = re.search("Flow run ID: (.*)", message)
    if match:
        flow_run_id = match.groups()[0]
        return flow_run_id
    return None


def get_state_message_from_logs(message: str) -> str | None:
    """uses regex matching to try to get a flow-run id"""
    match = re.search("State message: (.*)", message)
    if match:
        state_message = match.groups()[0]
        return state_message
    return None


def get_org_from_flow_run(flow_run: dict) -> Org | None:
    """
    given a flow-run, inspect its parameters
    if it has parameters and one of them is `airbyte_connection`, then
    look up the OrgPrefectBlock by its prefect block-document id
    now we have the org
    """
    opb = None
    if "parameters" in flow_run:
        parameters = flow_run["parameters"]

        if "block_name" in parameters:
            opb = OrgPrefectBlock.objects.filter(
                block_name=parameters["block_name"]
            ).first()

        elif "airbyte_connection" in parameters:
            block_id = parameters["airbyte_connection"]["_block_document_id"]
            opb = OrgPrefectBlock.objects.filter(block_id=block_id).first()

        elif "airbyte_blocks" in parameters:
            if len(parameters["airbyte_blocks"]) > 0:
                opb = OrgPrefectBlock.objects.filter(
                    block_name=parameters["airbyte_blocks"][0]["blockName"]
                ).first()

        elif "dbt_blocks" in parameters:
            if len(parameters["dbt_blocks"]) > 0:
                opb = OrgPrefectBlock.objects.filter(
                    block_name=parameters["dbt_blocks"][0]["blockName"]
                ).first()
    if opb:
        logger.info(opb)
        return opb.org
    return None


def generate_notification_email(
    orgname: str, flow_run_id: str, logmessages: list
) -> str:
    """until we make a sendgrid template"""
    email_body = f"""
To the admins of {orgname},

This is an automated notification from Prefect

Flow run id: {flow_run_id}
Logs:
"""
    email_body += "\n".join(logmessages)
    return email_body


def email_orgusers(org: Org, email_body: str):
    """sends a notificationemail to all OrgUsers"""
    for orguser in OrgUser.objects.filter(
        org=org,
        role__in=[
            OrgUserRole.ACCOUNT_MANAGER,
            OrgUserRole.PIPELINE_MANAGER,
        ],
    ):
        logger.info(f"sending prefect-notification email to {orguser.user.email}")
        send_text_message(orguser.user.email, "Prefect notification", email_body)


def email_flowrun_logs_to_orgusers(org: Org, flow_run_id: str):
    """retrieves logs for a flow-run and emails them to all users for the org"""
    logs = prefect_service.get_flow_run_logs(flow_run_id, 0)
    logmessages = [x["message"] for x in logs["logs"]]
    email_body = generate_notification_email(org.name, flow_run_id, logmessages)
    email_orgusers(org, email_body)


@webhookapi.post("/notification/")
def post_notification(request):  # pylint: disable=unused-argument
    """webhook endpoint for notifications"""
    if request.headers.get("X-Notification-Key") != os.getenv(
        "PREFECT_NOTIFICATIONS_WEBHOOK_KEY"
    ):
        raise HttpError(400, "unauthorized")
    notification = json.loads(request.body)
    message = notification["body"]
    logger.info(message)

    message_object = None
    try:
        message_object = json.loads(message)
    except ValueError:
        # not json, oh well
        pass
    if message_object is None and isinstance(message, dict):
        message_object = message

    flow_run_id = None
    if message_object:
        message_type = get_message_type(message_object)
        if message_type == FLOW_RUN:
            flow_run_id = message_object["id"]

    else:
        # <empty line>
        # Flow run return-dbt-test-failed/lush-elephant entered state `DBT_TEST_FAILED` at 2023-08-26T08:09:20.159919+00:00.
        # <empty line>
        # Flow ID: c7266896-10b0-4c65-a503-dce17d4de807
        # Flow run ID: acad1e73-4d23-4d38-9a1a-4ce36631971f
        # Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/acad1e73-4d23-4d38-9a1a-4ce36631971f
        # State message: WARNING: test failed

        flow_run_id = get_flow_run_id_from_logs(message)

    if flow_run_id:
        logger.info("found flow-run id %s, retrieving flow-run", flow_run_id)
        state = get_state_message_from_logs(message)
        if state in ["WARNING: test failed", "All states completed."]:
            BlockLock.objects.filter(flow_run_id=flow_run_id).delete()
        # flow_run = prefect_service.get_flow_run(flow_run_id)
        # logger.info(flow_run)
        # org = get_org_from_flow_run(flow_run)

    return {"status": "ok"}
