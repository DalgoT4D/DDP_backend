import os
import re
from ninja.errors import HttpError
from django.db.models import F
from django.utils.dateparse import parse_datetime
from django.utils.timezone import now as timezone_now
from ddpui.utils.custom_logger import CustomLogger

from ddpui.models.org import Org, OrgDataFlowv1, ConnectionMeta
from ddpui.models.tasks import OrgTask
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.tasks import TaskLock
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
from ddpui.utils.constants import (
    SYSTEM_USER_EMAIL,
    TASK_AIRBYTECLEAR,
    TASK_AIRBYTERESET,
    TASK_AIRBYTESYNC,
)
from ddpui.utils.helpers import find_all_values_for_key
from ddpui.core.notifications.delivery import (
    email_flowrun_logs_to_superadmins,
    notify_org_managers,
    notify_platform_admins,
)

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
            # if orguser is None then it is the system user
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

        # retry flow run if infra went down or in case of failures
        if state in [FLOW_RUN_CRASHED_STATE_NAME, FLOW_RUN_FAILED_STATE_NAME]:
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


def send_failure_emails(org: Org, odf: OrgDataFlowv1 | None, flow_run: dict, state: str):
    """send notification emails to org users"""
    flow_run_id = flow_run["id"]

    if os.getenv("SEND_FLOWRUN_LOGS_TO_SUPERADMINS", "").lower() in ["true", "1", "yes"]:
        email_flowrun_logs_to_superadmins(org, flow_run_id)
    if os.getenv("NOTIFY_PLATFORM_ADMINS_OF_ERRORS", "").lower() in ["true", "1", "yes"]:
        notify_platform_admins(org, flow_run_id, state)

    # Get failed task information
    task_runs = prefect_service.get_flow_run_graphs(flow_run_id)

    # Find the failed task
    failed_task = None
    for task in task_runs:
        if task.get("state_type") == "FAILED" or task.get("state_name") == "DBT_TEST_FAILED":
            failed_task = task
            break

    if not failed_task:
        logger.warning(f"No failed task found in prefect for flow_run {flow_run_id}")

    name_of_deployment = odf.name if odf else "[no deployment name]"

    # start building the email, at any point if we decide we won't send it we just return
    email_subject = f"{org.name}: Job failure for {name_of_deployment}"
    email_body = [f"To the admins of {org.name},\n"]

    if odf and odf.dataflow_type == "orchestrate":
        email_body.append(f"The pipeline {odf.name} has failed")

    elif odf and odf.dataflow_type == "manual":
        connection_ids = find_all_values_for_key(flow_run.get("parameters", {}), "connection_id")
        if len(connection_ids) > 0:
            connection_names = ConnectionMeta.objects.filter(
                connection_id__in=connection_ids
            ).values_list("connection_name", flat=True)

            email_body.append("A job has failed, the connection(s) involved were:")
            for connection_name in connection_names:
                email_body.append(f"- {connection_name}")
        else:
            orgtask_uuids = find_all_values_for_key(flow_run.get("parameters", {}), "orgtask_uuid")
            task_slugs = OrgTask.objects.filter(uuid__in=orgtask_uuids).values_list(
                "task__slug", flat=True
            )
            if task_slugs.count() == 1 and task_slugs[0] == "generate-edr":
                # don't send any email
                return

            email_body.append("A job has failed, the tasks involved were")
            for task_slug in task_slugs:
                email_body.append(f"- {task_slug}")
    else:
        email_body.append("A job has failed")
        # should we even bother to send this email???
        return

    email_body.append(f"\nPlease visit {os.getenv('FRONTEND_URL')} for more details")
    notify_org_managers(
        org,
        "\n".join(email_body),
        email_subject,
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

    try:
        deployment_id = flow_run.get("deployment_id")
        if deployment_id:
            send_failure_notifications = update_flow_run_for_deployment(
                deployment_id, state, flow_run
            )

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
                    send_failure_emails(org, odf, flow_run, state)

    except Exception as err:
        logger.error(
            "Error while handling prefect webhook for flow run %s: %s",
            flow_run_id,
            str(err),
        )
    finally:
        # if the flow_run is an airbyte job, sync its history
        for task in flow_run.get("parameters", {}).get("config", {}).get("tasks", []):
            if task.get("slug", "") in [TASK_AIRBYTESYNC, TASK_AIRBYTERESET, TASK_AIRBYTECLEAR]:
                connection_id = task.get("connection_id", None)
                if connection_id:
                    # Import here to avoid circular import
                    from ddpui.ddpairbyte import airbytehelpers

                    # sync all jobs in all 6 hours
                    airbytehelpers.fetch_and_update_airbyte_jobs_for_all_connections(
                        last_n_days=0, last_n_hours=6, connection_id=connection_id
                    )

                    logger.info(
                        "syncing airbyte job stats for connection %s in flow run %s",
                        connection_id,
                        flow_run_id,
                    )

        # Trigger automatic summarization for failures
        if state in [FLOW_RUN_FAILED_STATE_NAME, FLOW_RUN_CRASHED_STATE_NAME]:
            org = get_org_from_flow_run(flow_run)
            # Import here to avoid circular import
            from ddpui.celeryworkers.tasks import trigger_log_summarization_for_failed_flow

            trigger_log_summarization_for_failed_flow.delay(flow_run_id, flow_run)
