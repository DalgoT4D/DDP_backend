import os
import json
import re
from ninja import NinjaAPI

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.api.client.prefect_api import prefect_service
from ddpui.models.org import OrgPrefectBlock, Org
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.utils.sendgrid import send_text_message

webhookapi = NinjaAPI(urls_namespace="webhook")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


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
    # <empty line>
    # Flow run return-dbt-test-failed/lush-elephant entered state `DBT_TEST_FAILED` at 2023-08-26T08:09:20.159919+00:00.
    # <empty line>
    # Flow ID: c7266896-10b0-4c65-a503-dce17d4de807
    # Flow run ID: acad1e73-4d23-4d38-9a1a-4ce36631971f
    # Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/acad1e73-4d23-4d38-9a1a-4ce36631971f
    # State message: WARNING: test failed
    notification_types = [
        {"pattern": "Flow run ID: (.*)", "name": "flow-run-id"},
        {"pattern": "State message: (.*)", "name": "state"},
    ]
    for notification_type in notification_types:
        match = re.search(notification_type["pattern"], message)

        if match:
            match_data = match.groups()[0]
            logger.info("found %s %s", notification_type["name"], match_data)

            if notification_type["name"] == "flow-run-id":
                flow_run_id = match_data
                flow_run = prefect_service.get_flow_run(flow_run_id)
                logger.info(flow_run)
                org = get_org_from_flow_run(flow_run)

                if org:
                    logs = prefect_service.get_flow_run_logs(flow_run_id, 0)
                    logmessages = [x["message"] for x in logs["logs"]]
                    email_body = generate_notification_email(
                        org.name, flow_run_id, logmessages
                    )
                    for orguser in OrgUser.objects.filter(
                        org=org,
                        role__in=[
                            OrgUserRole.ACCOUNT_MANAGER,
                            OrgUserRole.PIPELINE_MANAGER,
                        ],
                    ):
                        logger.info(
                            f"sending prefect-notification email to {orguser.user.email}"
                        )
                        send_text_message(
                            orguser.user.email, "Prefect notificatdion", email_body
                        )

    return {"status": "ok"}


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
