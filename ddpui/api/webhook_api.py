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
from ddpui.celeryworkers.tasks import (
    handle_prefect_webhook,
    generate_failure_summary,
    sync_single_airbyte_job_stats,
)
from ddpui.models.llm import LlmSession, LlmSessionStatus, LlmAssistantType
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
            state = message_object.get("state", {}).get("name", "unknown")

    else:
        # 'Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}'
        flow_run_id, state = get_flowrun_id_and_state(message)

    if not flow_run_id:
        return {"status": "ok"}

    logger.info("found flow-run id %s, state %s", flow_run_id, state)

    # Check if this is a failure state and trigger LLM summary generation
    from ddpui.ddpprefect import FLOW_RUN_FAILED_STATE_NAME, FLOW_RUN_CRASHED_STATE_NAME

    if state in [FLOW_RUN_FAILED_STATE_NAME, FLOW_RUN_CRASHED_STATE_NAME]:
        try:
            # Get flow run details to find the organization
            from ddpui.ddpprefect import prefect_service

            flow_run = prefect_service.get_flow_run_poll(flow_run_id)

            if flow_run:
                from ddpui.utils.webhook_helpers import get_org_from_flow_run

                org = get_org_from_flow_run(flow_run)

                if org and hasattr(org, "orgpreferences") and org.orgpreferences.llm_optin:
                    logger.info(
                        f"Triggering LLM failure summary for flow run {flow_run_id} in org {org.slug}"
                    )
                    deployment_id = flow_run.get("deployment_id")
                    generate_failure_summary.delay(
                        flow_run_id=flow_run_id,
                        org_id=org.id,
                        deployment_id=deployment_id,
                    )
                else:
                    logger.info(
                        f"LLM not enabled for org or org not found for flow run {flow_run_id}"
                    )
            else:
                logger.warning(f"Could not retrieve flow run details for {flow_run_id}")

        except Exception as err:
            logger.error(
                f"Failed to trigger LLM failure summary for flow run {flow_run_id}: {str(err)}"
            )

    # Continue with existing webhook processing
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


@webhook_router.get("/failure-summary/{flow_run_id}")
@has_permission(["can_view_logs"])
def get_failure_summary(request, flow_run_id: str):
    """Get LLM failure summary for a specific flow run"""
    try:
        orguser: OrgUser = request.orguser

        # Find the LLM session for this flow run
        llm_session = (
            LlmSession.objects.filter(
                org=orguser.org, flow_run_id=flow_run_id, session_status=LlmSessionStatus.COMPLETED
            )
            .order_by("-created_at")
            .first()
        )

        if not llm_session:
            return {
                "status": "not_found",
                "message": "No LLM failure summary found for this flow run",
                "flow_run_id": flow_run_id,
            }

        return {
            "status": "success",
            "flow_run_id": flow_run_id,
            "summary": {
                "session_id": llm_session.session_id,
                "created_at": llm_session.created_at,
                "user_prompts": llm_session.user_prompts,
                "assistant_prompt": llm_session.assistant_prompt,
                "response": llm_session.response,
                "session_status": llm_session.session_status,
            },
        }

    except Exception as err:
        logger.error(f"Error retrieving failure summary for flow run {flow_run_id}: {str(err)}")
        raise HttpError(500, f"Failed to retrieve failure summary: {str(err)}")


@webhook_router.get("/failure-summaries/")
@has_permission(["can_view_logs"])
def get_failure_summaries(request, limit: int = 10, offset: int = 0):
    """Get LLM failure summaries for the organization"""
    try:
        orguser: OrgUser = request.orguser

        # Get all LLM sessions for failure summaries in this org
        llm_sessions = (
            LlmSession.objects.filter(
                org=orguser.org,
                session_status=LlmSessionStatus.COMPLETED,
                session_type=LlmAssistantType.LOG_SUMMARIZATION,
                flow_run_id__isnull=False,
            )
            .order_by("-created_at")
            .select_related("orguser__user")[offset : offset + limit]
        )

        total_count = LlmSession.objects.filter(
            org=orguser.org,
            session_status=LlmSessionStatus.COMPLETED,
            session_type=LlmAssistantType.LOG_SUMMARIZATION,
            flow_run_id__isnull=False,
        ).count()

        summaries = []
        for session in llm_sessions:
            summaries.append(
                {
                    "session_id": session.session_id,
                    "flow_run_id": session.flow_run_id,
                    "created_at": session.created_at,
                    "user_prompts": session.user_prompts,
                    "assistant_prompt": session.assistant_prompt,
                    "response": session.response,
                    "session_status": session.session_status,
                    "created_by": (session.orguser.user.email if session.orguser else "System"),
                }
            )

        return {
            "status": "success",
            "summaries": summaries,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        }

    except Exception as err:
        logger.error(f"Error retrieving failure summaries: {str(err)}")
        raise HttpError(500, f"Failed to retrieve failure summaries: {str(err)}")


@webhook_router.post("/v1/airbyte_job/{job_id}", auth=None)
def post_airbyte_job_details(request, job_id: str):
    """webhook endpoint for Airbyte job details"""
    if request.headers.get("X-Notification-Key") != os.getenv("AIRBYTE_NOTIFICATIONS_WEBHOOK_KEY"):
        raise HttpError(400, "unauthorized")

    sync_single_airbyte_job_stats.delay(int(job_id))

    return {"status": "ok"}
