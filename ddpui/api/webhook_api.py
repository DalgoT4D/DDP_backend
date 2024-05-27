import os
import json
from ninja import NinjaAPI

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service
from ddpui.models.orgjobs import BlockLock
from ddpui.models.tasks import TaskLock
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.org import OrgDataFlowv1
from ddpui.utils.webhook_helpers import (
    get_message_type,
    get_flowrun_id_and_state,
    get_org_from_flow_run,
    email_flowrun_logs_to_orgusers,
    FLOW_RUN,
)


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
        # 'Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}'
        flow_run_id, state = get_flowrun_id_and_state(message)

    if flow_run_id:
        logger.info("found flow-run id %s, state %s", flow_run_id, state)

        if state in ["Cancelled", "Completed", "Failed", "Crashed"]:
            BlockLock.objects.filter(flow_run_id=flow_run_id).delete()

        elif state in ["Pending"]:
            flow_run = prefect_service.get_flow_run(flow_run_id)
            deployment_id = flow_run["deployment_id"]
            system_user = OrgUser.objects.filter(user__email="System User").first()
            try:
                prefect_service.lock_blocks_for_deployment(deployment_id, system_user)
            except HttpError:
                # silently ignore
                logger.info(
                    "failed to lock blocks for deployment %s, ignoring", deployment_id
                )

        # logger.info(flow_run)
        if state in ["Failed", "Crashed"]:
            flow_run = prefect_service.get_flow_run(flow_run_id)
            org = get_org_from_flow_run(flow_run)
            if org:
                email_flowrun_logs_to_orgusers(org, flow_run_id)

    return {"status": "ok"}


@webhookapi.post("/v1/notification/")
def post_notification_v1(request):  # pylint: disable=unused-argument
    """webhook endpoint for notifications"""
    if request.headers.get("X-Notification-Key") != os.getenv(
        "PREFECT_NOTIFICATIONS_WEBHOOK_KEY"
    ):
        raise HttpError(400, "unauthorized")
    notification = json.loads(request.body)
    logger.info(notification)
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
        # 'Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}'
        flow_run_id, state = get_flowrun_id_and_state(message)

    if flow_run_id:
        logger.info("found flow-run id %s, state %s", flow_run_id, state)
        flow_run = prefect_service.get_flow_run(flow_run_id)
        deployment_id = flow_run["deployment_id"]

        if deployment_id and state in ["Cancelled", "Completed", "Failed", "Crashed"]:
            logger.info("deleting the task locks")
            TaskLock.objects.filter(flow_run_id=flow_run_id).delete()
            if state in ["Completed", "Failed"]:
                PrefectFlowRun.objects.create(
                    deployment_id=deployment_id,
                    flow_run_id=flow_run["id"],
                    name=flow_run["name"],
                    start_time=flow_run["start_time"],
                    expected_start_time=flow_run["expected_start_time"],
                    total_run_time=flow_run["total_run_time"],
                    status=flow_run["status"],
                    state_name=flow_run["state_name"],
                )

        elif state in ["Pending"]:
            system_user = OrgUser.objects.filter(user__email="System User").first()
            try:
                prefect_service.lock_tasks_for_deployment(deployment_id, system_user)
            except HttpError:
                # silently ignore
                logger.info(
                    "failed to lock blocks for deployment %s, ignoring", deployment_id
                )

        # logger.info(flow_run)
        if state in ["Failed", "Crashed"]:
            org = get_org_from_flow_run(flow_run)
            if org:
                email_flowrun_logs_to_orgusers(org, flow_run_id)

    return {"status": "ok"}


# setting up the notification and customizing the message format
#
# 1. create the custom-webhook notification. not the notification block! just the notification,
#    from http://127.0.0.1:4200/notifications
#    parameters:
#      - url = http://localhost:8002/webhooks/notification/
#      - custom headers = {"X-Notification-Key": "<PREFECT_NOTIFICATIONS_WEBHOOK_KEY>"}
#      - json body = {"body": "{{body}}"}
# 2. requests.post('http://localhost:4200/api/flow_run_notification_policies/filter', json={}).json()
# 3. find the flor-run-notification-policy for the new notification in this list
# 4. save it in frnp = '<the id>'
# 5. requests.patch(f'http://localhost:4200/api/flow_run_notification_policies/{frnp}', json={
#      'message_template': 'Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}'
#    })
