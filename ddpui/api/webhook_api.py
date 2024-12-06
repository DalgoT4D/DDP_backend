import os
import json
from ninja import Router

from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service
from ddpui.models.tasks import TaskLock
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.webhook_helpers import (
    get_message_type,
    get_flowrun_id_and_state,
    get_org_from_flow_run,
    notify_org_managers,
    notify_platform_admins,
    email_flowrun_logs_to_superadmins,
    email_orgusers_ses_whitelisted,
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
    MAP_FLOW_RUN_STATE_NAME_TO_TYPE,
)


webhook_router = Router()
logger = CustomLogger("ddpui")
MAX_RETRIES_FOR_CRASHED_FLOW_RUNS = 1


@webhook_router.post("/v1/notification/")
def post_notification_v1(request):  # pylint: disable=unused-argument
    """webhook endpoint for notifications"""
    if request.headers.get("X-Notification-Key") != os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY"):
        raise HttpError(400, "unauthorized")
    notification = json.loads(request.body)
    # logger.info(notification)
    message = notification["body"]
    # logger.info(message)

    message_object = None
    try:
        message_object = json.loads(message)
    except ValueError:
        # not json, oh well
        pass
    if message_object is None and isinstance(message, dict):
        message_object = message

    flow_run_id = None
    state = "unknown"
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

    logger.info("flow+run : %s", flow_run)

    send_failure_notifications = state in [
        FLOW_RUN_FAILED_STATE_NAME,
        FLOW_RUN_CRASHED_STATE_NAME,
    ]

    # dont really care about the subflows inside our main flows which might have not deployment_id
    if deployment_id:
        if state in [
            FLOW_RUN_CANCELLED_STATE_NAME,
            FLOW_RUN_COMPLETED_STATE_NAME,
            FLOW_RUN_FAILED_STATE_NAME,
            FLOW_RUN_CRASHED_STATE_NAME,
        ]:  # terminal states
            TaskLock.objects.filter(flow_run_id=flow_run["id"]).delete()
            logger.info("updating the flow run in db")
            create_or_update_flowrun(flow_run, deployment_id, state)

            # retry flow run if infra went down
            if state == FLOW_RUN_CRASHED_STATE_NAME:
                prefect_flow_run = PrefectFlowRun.objects.filter(flow_run_id=flow_run_id).first()
                if (
                    os.getenv("PREFECT_RETRY_CRASHED_FLOW_RUNS") in ["True", "true", True]
                    and prefect_flow_run
                    and prefect_flow_run.retries < MAX_RETRIES_FOR_CRASHED_FLOW_RUNS
                ):
                    # dont send notification right now, retry first
                    try:
                        prefect_service.retry_flow_run(flow_run_id, 5)
                        prefect_flow_run.retries += 1
                        prefect_flow_run.save()
                        send_failure_notifications = False
                    except Exception as err:
                        logger.error(
                            f"Something went wrong retrying the flow run {flow_run_id} that just crashed - {str(err)}"
                        )

        elif state == FLOW_RUN_PENDING_STATE_NAME:  # non-terminal states
            locks = lock_tasks_for_pending_deployment(deployment_id)
            for tasklock in locks:
                logger.info("uppdating flow run id on locks")
                tasklock.flow_run_id = flow_run.get("id")
                tasklock.save()

            create_or_update_flowrun(flow_run, deployment_id, state)

        elif state == FLOW_RUN_RUNNING_STATE_NAME:  # non-terminal states
            create_or_update_flowrun(flow_run, deployment_id, state)

    # notifications
    if send_failure_notifications:
        org = get_org_from_flow_run(flow_run)
        if org:
            odf = OrgDataFlowv1.objects.filter(deployment_id=deployment_id).first()
            name_of_deployment = odf.name if odf else "[no deployment name]"
            type_of_deployment = odf.dataflow_type if odf else "[no deployment type]"
            email_flowrun_logs_to_superadmins(org, flow_run["id"])
            notify_platform_admins(org, flow_run["id"], state)
            notify_org_managers(
                org,
                f"A job for \"{name_of_deployment}\" of type \"{type_of_deployment}\" has failed, please visit {os.getenv('FRONTEND_URL')} for more details",
            )
            email_orgusers_ses_whitelisted(
                org,
                f'There is a problem with the pipeline "{name_of_deployment}"; we are working on a fix',
            )

    if state in [FLOW_RUN_COMPLETED_STATE_NAME]:
        org = get_org_from_flow_run(flow_run)
        if org:
            email_orgusers_ses_whitelisted(org, "Your pipeline completed successfully")

    return {"status": "ok"}


def create_or_update_flowrun(flow_run, deployment_id, state_name=""):
    """Create or update the flow run entry in database"""
    state_name = state_name if state_name else flow_run["state_name"]
    deployment_id = deployment_id if deployment_id else flow_run.get("deployment_id")

    PrefectFlowRun.objects.update_or_create(
        flow_run_id=flow_run["id"],
        defaults={
            **({"deployment_id": deployment_id} if deployment_id else {}),
            "name": flow_run["name"],
            "start_time": (
                flow_run["start_time"]
                if flow_run["start_time"] not in ["", None]
                else flow_run["expected_start_time"]
            ),
            "expected_start_time": flow_run["expected_start_time"],
            "total_run_time": flow_run["total_run_time"],
            "status": MAP_FLOW_RUN_STATE_NAME_TO_TYPE[state_name],
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
