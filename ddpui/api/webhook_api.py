import os
import json
from ninja import Router
from ninja.errors import HttpError
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.webhook_helpers import (
    get_message_type,
    get_flowrun_id_and_state,
    FLOW_RUN,
)
from ddpui.celeryworkers.tasks import handle_prefect_webhook
from ddpui.models.llm import LlmSession, LlmSessionStatus
from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser

webhook_router = Router()
logger = CustomLogger("ddpui")


@webhook_router.post("/v1/notification/", auth=None)
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
    handle_prefect_webhook.delay(flow_run_id, state)
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
