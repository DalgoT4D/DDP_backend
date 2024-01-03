import re
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import OrgPrefectBlock, Org
from ddpui.models.org_user import OrgUserRole, OrgUser
from ddpui.utils.sendgrid import send_text_message
from ddpui.api.prefect_api import prefect_service
from ddpui.settings import PRODUCTION

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

        if opb is None and "airbyte_connection" in parameters:
            block_id = parameters["airbyte_connection"]["_block_document_id"]
            opb = OrgPrefectBlock.objects.filter(block_id=block_id).first()

        if opb is None and "airbyte_blocks" in parameters:
            if len(parameters["airbyte_blocks"]) > 0:
                opb = OrgPrefectBlock.objects.filter(
                    block_name=parameters["airbyte_blocks"][0]["blockName"]
                ).first()

        if opb is None and "dbt_blocks" in parameters:
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
    tag = " [STAGING]" if not PRODUCTION else ""
    email_body = f"""
To the admins of {orgname}{tag},

This is an automated notification from Dalgo{tag}.

Flow run id: {flow_run_id}
Logs:
"""
    email_body += "\n".join(logmessages)
    return email_body


def email_orgusers(org: Org, email_body: str):
    """sends a notificationemail to all OrgUsers"""
    tag = " [STAGING]" if not PRODUCTION else ""
    subject = f"Prefect notification{tag}"
    for orguser in OrgUser.objects.filter(
        org=org,
        role__in=[
            OrgUserRole.ACCOUNT_MANAGER,
            OrgUserRole.PIPELINE_MANAGER,
        ],
    ):
        logger.info(f"sending prefect-notification email to {orguser.user.email}")
        send_text_message(orguser.user.email, subject, email_body)


def email_flowrun_logs_to_orgusers(org: Org, flow_run_id: str):
    """retrieves logs for a flow-run and emails them to all users for the org"""
    logs = prefect_service.get_flow_run_logs(flow_run_id, 0)
    logmessages = [x["message"] for x in logs["logs"]["logs"]]
    email_body = generate_notification_email(org.name, flow_run_id, logmessages)
    email_orgusers(org, email_body)
