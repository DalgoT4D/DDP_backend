import os
import json
from ninja import NinjaAPI

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service
from ddpui.models.tasks import TaskLock
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.webhook_helpers import (
    get_message_type,
    get_flowrun_id_and_state,
    get_org_from_flow_run,
    email_flowrun_logs_to_orgusers,
    FLOW_RUN,
)
from ddpui.utils.constants import SYSTEM_USER_EMAIL
from ddpui.ddpprefect import (
    FLOW_RUN_CANCELLED_STATE_NAME,
    FLOW_RUN_CRASHED_STATE_NAME,
    FLOW_RUN_FAILED_STATE_NAME,
    FLOW_RUN_COMPLETED_STATE_NAME,
    FLOW_RUN_RUNNING_STATE_NAME,
    FLOW_RUN_PENDING_STATE_NAME,
)


webhookapi = NinjaAPI(urls_namespace="webhook")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


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

    if not flow_run_id:
        return {"status": "ok"}

    logger.info("found flow-run id %s, state %s", flow_run_id, state)
    flow_run = prefect_service.get_flow_run(flow_run_id)
    deployment_id = flow_run.get("deployment_id")

    if deployment_id and state in [
        FLOW_RUN_CANCELLED_STATE_NAME,
        FLOW_RUN_COMPLETED_STATE_NAME,
        FLOW_RUN_FAILED_STATE_NAME,
        FLOW_RUN_CRASHED_STATE_NAME,
    ]:  # terminal states
        TaskLock.objects.filter(flow_run_id=flow_run["id"]).delete()
        logger.info("updating the flow run in db")
        create_or_update_flowrun(flow_run, deployment_id)

    elif state == FLOW_RUN_PENDING_STATE_NAME:  # non-terminal states
        # doesn't have start time yet
        locks: list[TaskLock] = lock_tasks_for_pending_deployment(deployment_id)
        for tasklock in locks:
            tasklock.flow_run_id = flow_run.get("id")
            tasklock.save()
        create_or_update_flowrun(flow_run, deployment_id)

    # # this might be triggered multiple times and we dont want to load our db for the same update again and again
    # # we also dont care so much about this state yet
    # # also this state doesn't have deployment_id for some reason
    # elif state == FLOW_RUN_RUNNING_STATE_NAME:  # non-terminal states
    #     create_or_update_flowrun(flow_run, deployment_id)

    if state in [FLOW_RUN_FAILED_STATE_NAME, FLOW_RUN_CRASHED_STATE_NAME]:
        email_logs_to_org_users(flow_run)

    return {"status": "ok"}


def create_or_update_flowrun(flow_run, deployment_id):
    """Create or update the flow run entry in database"""

    PrefectFlowRun.objects.update_or_create(
        flow_run_id=flow_run["id"],
        defaults={
            "deployment_id": deployment_id,
            "name": flow_run["name"],
            "start_time": (
                flow_run["start_time"]
                if flow_run["start_time"] not in ["", None]
                else flow_run["expected_start_time"]
            ),
            "expected_start_time": flow_run["expected_start_time"],
            "total_run_time": flow_run["total_run_time"],
            "status": flow_run["status"],
            "state_name": flow_run["state_name"],
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
        logger.info("failed to lock blocks for deployment %s, ignoring", deployment_id)

    return locks


def email_logs_to_org_users(flow_run):
    """email flow run logs to users"""
    org = get_org_from_flow_run(flow_run)
    if org:
        email_flowrun_logs_to_orgusers(org, flow_run["id"])


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
