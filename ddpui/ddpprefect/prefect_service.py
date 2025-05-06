import os
import math
from datetime import datetime
import requests
from itertools import groupby
from operator import itemgetter
from statistics import mean

from ninja.errors import HttpError
from dotenv import load_dotenv
from django.db import transaction
from django.db.models import Window
from django.db.models.functions import RowNumber, TruncDate

from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectSecretBlockCreate,
    PrefectSecretBlockEdit,
    PrefectShellTaskSetup,
    PrefectDbtTaskSetup,
    PrefectDataFlowUpdateSchema3,
    DeploymentRunTimes,
    FilterLateFlowRunsRequest,
    DeploymentCurrentQueueTime,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.tasks import DataflowOrgTask, TaskLock
from ddpui.models.org_user import OrgUser, Org
from ddpui.models.org import OrgPrefectBlockv1, OrgDataFlowv1
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.ddpprefect import (
    DDP_WORK_QUEUE,
    FLOW_RUN_COMPLETED_STATE_TYPE,
    FLOW_RUN_CRASHED_STATE_TYPE,
    FLOW_RUN_FAILED_STATE_TYPE,
    DBTCLOUDCREDS,
)
from ddpui.utils.constants import (
    FLOW_RUN_LOGS_OFFSET_LIMIT,
)

load_dotenv()

PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")
http_timeout = int(os.getenv("PREFECT_HTTP_TIMEOUT", "30"))

logger = CustomLogger("ddpui")


# ================================================================================================
def prefect_get(endpoint: str, **kwargs) -> dict:
    """make a GET request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.get(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_post(endpoint: str, json: dict, **kwargs) -> dict:
    """make a POST request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.post(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            headers=headers,
            timeout=timeout,
            json=json,
            **kwargs,
        )
    except Exception as error:
        logger.error(error)
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_put(endpoint: str, json: dict, **kwargs) -> dict:
    """make a PUT request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.put(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            headers=headers,
            timeout=timeout,
            json=json,
            **kwargs,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_patch(endpoint: str, json: dict, **kwargs) -> dict:
    """make a PATCH request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.patch(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            headers=headers,
            timeout=timeout,
            json=json,
            **kwargs,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_delete_a_block(block_id: str, **kwargs) -> None:
    """makes a DELETE request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.delete(
            f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}",
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error


def prefect_delete(endpoint: str, **kwargs) -> None:
    """makes a DELETE request to the proxy"""
    # we send headers and timeout separately from kwargs, just to be explicit about it
    headers = kwargs.pop("headers", {})
    headers["x-ddp-org"] = logger.get_slug()
    timeout = kwargs.pop("timeout", http_timeout)

    try:
        res = requests.delete(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, res.text) from error


# ================================================================================================
def get_prefect_server_version():
    """fetches the prefect server version"""
    try:
        prefect_host = os.getenv("PREFECT_SERVER_HOST")
        prefect_port = os.getenv("PREFECT_SERVER_PORT")
        prefect_url = f"http://{prefect_host}:{prefect_port}/api/admin/version"
        prefect_response = requests.get(prefect_url, timeout=5)
        if prefect_response.status_code == 200:
            version = prefect_response.text.strip().strip('"')
            return version
        else:
            return "Not available"
    except Exception:
        return "Not available"


# ================================================================================================
def get_airbyte_server_block_id(blockname) -> str | None:
    """get the block_id for the server block having this name"""
    response = prefect_get(f"blocks/airbyte/server/{blockname}")
    return response["block_id"]


def get_airbyte_server_block(blockname) -> dict | None:
    """get the block for the server block having this name"""
    response = prefect_get(f"blocks/airbyte/server/block/{blockname}")
    return response


def create_airbyte_server_block(blockname):
    """Create airbyte server block in prefect"""
    response = prefect_post(
        "blocks/airbyte/server/",
        {
            "blockName": blockname,
            "serverHost": os.getenv("AIRBYTE_SERVER_HOST"),
            "serverPort": os.getenv("AIRBYTE_SERVER_PORT"),
            "apiVersion": os.getenv("AIRBYTE_SERVER_APIVER"),
        },
    )
    return (response["block_id"], response["cleaned_block_name"])


def update_airbyte_server_block(blockname):
    """Create airbyte server block in prefect"""
    response = prefect_put(
        "blocks/airbyte/server/",
        {
            "blockName": blockname,
            "serverHost": os.getenv("AIRBYTE_SERVER_HOST"),
            "serverPort": os.getenv("AIRBYTE_SERVER_PORT"),
        },
    )
    return (response["block_id"], response["cleaned_block_name"])


def delete_airbyte_server_block(block_id):
    """Delete airbyte server block"""
    prefect_delete_a_block(block_id)


# ================================================================================================
def update_airbyte_connection_block(blockname):
    """We don't update connection blocks"""
    raise Exception("not implemented")


def delete_airbyte_connection_block(block_id) -> None:
    """Delete airbyte connection block in prefect"""
    prefect_delete_a_block(block_id)


# ================================================================================================
def delete_shell_block(block_id):
    """Delete a prefect shell block"""
    prefect_delete_a_block(block_id)


# ================================================================================================
def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    prefect_delete_a_block(block_id)


def update_dbt_core_block_schema(block_name: str, target_configs_schema: str):
    """Update the schema inside a dbt core block in prefect"""
    response = prefect_put(
        "blocks/dbtcore_edit_schema/",
        {
            "blockName": block_name,
            "target_configs_schema": target_configs_schema,
        },
    )
    return response


# ================================================================================================
def create_dbt_cli_profile_block(
    block_name: str,
    profilename: str,
    target: str,
    wtype: str,
    bqlocation: str,
    credentials: dict,
) -> dict:
    """Create a dbt cli profile block in that has the warehouse information"""
    response = prefect_post(
        "blocks/dbtcli/profile/",
        {
            "cli_profile_block_name": block_name,
            "profile": {
                "name": profilename,
                "target": target,
                "target_configs_schema": target,
            },
            "wtype": wtype,
            "credentials": credentials,
            "bqlocation": bqlocation,
        },
    )
    return response


def update_dbt_cli_profile_block(
    block_name: str,
    wtype: str = None,
    profilename: str = None,
    target: str = None,
    credentials: dict = None,
    bqlocation: str = None,
):
    """Update the dbt cli profile for an org"""
    response = prefect_put(
        "blocks/dbtcli/profile/",
        {
            "cli_profile_block_name": block_name,
            "wtype": wtype,
            "profile": {
                "name": profilename,
                "target": target,
                "target_configs_schema": target,
            },
            "credentials": credentials,
            "bqlocation": bqlocation,
        },
    )
    return response


def delete_dbt_cli_profile_block(block_id) -> None:
    """Delete dbt cli profile block in prefect"""
    prefect_delete_a_block(block_id)


def get_dbt_cli_profile_block(block_name: str) -> dict:
    """fetches the dbt cli profile block from prefect"""
    response = prefect_get(f"blocks/dbtcli/profile/{block_name}")
    return response


# ================================================================================================


def create_secret_block(secret_block: PrefectSecretBlockCreate):
    """This will create a secret block in the prefect to store any password like string"""
    response = prefect_post(
        "blocks/secret/",
        {"blockName": secret_block.block_name, "secret": secret_block.secret},
    )
    return response


def upsert_secret_block(secret_block: PrefectSecretBlockEdit):
    """This will create a secret block in the prefect to store any password like string"""
    response = prefect_put(
        "blocks/secret/",
        {"blockName": secret_block.block_name, "secret": secret_block.secret},
    )
    return response


def delete_secret_block(block_id) -> None:
    """Delete secret block in prefect"""
    prefect_delete_a_block(block_id)


def get_secret_block_by_name(blockname: str) -> dict:
    """Fetch secret block id and block name"""
    response = prefect_get(f"blocks/secret/{blockname}")
    return response


# ================================================================================================
def run_dbt_task_sync(task: PrefectDbtTaskSetup) -> dict:  # pragma: no cover
    """initiates a dbt job sync"""
    res = prefect_post(
        "v1/flows/dbtcore/run/",
        json=task.to_json(),
    )
    return res


def run_shell_task_sync(task: PrefectShellTaskSetup) -> dict:  # pragma: no cover
    """initiates a shell task sync"""
    res = prefect_post(
        "flows/shell/run/",
        json=task.to_json(),
    )
    return res


# Flows and deployments
def create_dataflow_v1(
    payload: PrefectDataFlowCreateSchema3, queue_name=DDP_WORK_QUEUE
) -> dict:  # pragma: no cover
    """create a prefect deployment out of a flow and a cron schedule; to go away with the blocks"""
    res = prefect_post(
        "v1/deployments/",
        {
            "flow_name": payload.flow_name,
            "deployment_name": payload.deployment_name,
            "org_slug": payload.orgslug,
            "deployment_params": payload.deployment_params,
            "cron": payload.cron,
            "work_pool_name": os.getenv("PREFECT_WORKER_POOL_NAME"),
            "work_queue_name": queue_name,
        },
    )
    return res


def update_dataflow_v1(
    deployment_id: str, payload: PrefectDataFlowUpdateSchema3
) -> dict:  # pragma: no cover
    """update a prefect deployment with a new cron schedule"""
    res = prefect_put(
        f"v1/deployments/{deployment_id}",
        {"cron": payload.cron, "deployment_params": payload.deployment_params},
    )
    return res


def get_flow_runs_by_deployment_id(deployment_id: str, limit=None):  # pragma: no cover
    """
    Fetch flow runs of a deployment that are FAILED/COMPLETED
    sorted by descending start time of each run
    """
    result = []
    # sorted by start-time ASC
    for prefect_flow_run in PrefectFlowRun.objects.filter(deployment_id=deployment_id).order_by(
        "start_time"
    ):
        result.append(prefect_flow_run.to_json())

    params = {"deployment_id": deployment_id, "limit": limit}
    if len(result) > 0:
        params["start_time_gt"] = result[-1]["startTime"]
    res = prefect_get("flow_runs", params=params, timeout=60)

    # iterate so that start-time is ASC
    for flow_run in res["flow_runs"][::-1]:
        if not PrefectFlowRun.objects.filter(flow_run_id=flow_run["id"]).exists():
            if flow_run["startTime"] in ["", None]:
                flow_run["startTime"] = flow_run["expectedStartTime"]
            prefect_flow_run = PrefectFlowRun.objects.create(
                deployment_id=deployment_id,
                flow_run_id=flow_run["id"],
                name=flow_run["name"],
                start_time=flow_run["startTime"],
                expected_start_time=flow_run["expectedStartTime"],
                total_run_time=flow_run["totalRunTime"],
                status=flow_run["status"],
                state_name=flow_run["state_name"],
            )
            prefect_flow_run.refresh_from_db()
            result.append(prefect_flow_run.to_json())

    # sorted by start-time DESC
    result.reverse()
    return result[:50]


def get_flow_runs_by_deployment_id_v1(
    deployment_id: str = "", deployment_ids: list[str] = [], limit=10, offset=0
):
    """
    Fetch flow runs of a deployment that are FAILED/COMPLETED/CRASHED
    sorted by start time of each run

    If list of deployment ids is passed it will group by and
    fetch runs by the limit and offset in each group

    Everything is sorted by start time of each run
    Giving the most recent runs first
    """
    result = []

    if deployment_ids and len(deployment_ids) > 0:
        flow_runs_with_row_number = PrefectFlowRun.objects.filter(
            status__in=[
                FLOW_RUN_COMPLETED_STATE_TYPE,
                FLOW_RUN_FAILED_STATE_TYPE,
                FLOW_RUN_CRASHED_STATE_TYPE,
            ],
            deployment_id__in=deployment_ids,
        ).annotate(
            row_number=Window(
                expression=RowNumber(),
                partition_by="deployment_id",
                order_by="-start_time",
            )
        )
        limited_flow_runs = flow_runs_with_row_number.filter(
            row_number__gt=offset,
            row_number__lte=offset + limit,
        )

        result = [flow_run.to_json() for flow_run in limited_flow_runs]

        return result

    for prefect_flow_run in PrefectFlowRun.objects.filter(
        deployment_id=deployment_id,
        status__in=[
            FLOW_RUN_COMPLETED_STATE_TYPE,
            FLOW_RUN_FAILED_STATE_TYPE,
            FLOW_RUN_CRASHED_STATE_TYPE,
        ],
    ).order_by("-start_time")[offset : offset + limit]:
        result.append(prefect_flow_run.to_json())

    return result


def get_last_flow_run_by_deployment_id(deployment_id: str):  # pragma: no cover
    """Fetch most recent flow run of a deployment that is FAILED/COMPLETED"""
    res = get_flow_runs_by_deployment_id(deployment_id, limit=1)
    if len(res) > 0:
        return res[0]
    return None


def set_deployment_schedule(deployment_id: str, status: str):
    """activates / deactivates a deployment"""
    prefect_post(f"deployments/{deployment_id}/set_schedule/{status}", {})


def get_filtered_deployments(org_slug, deployment_ids):  # pragma: no cover
    # pylint: disable=dangerous-default-value
    """Fetch all deployments by org slug"""
    res = prefect_post(
        "deployments/filter",
        {"org_slug": org_slug, "deployment_ids": deployment_ids},
    )
    return res["deployments"]


def delete_deployment_by_id(deployment_id: str) -> dict:  # pragma: no cover
    """Proxy api call to delete a deployment from prefect db"""
    try:
        res = requests.delete(
            f"{PREFECT_PROXY_API_URL}/proxy/deployments/{deployment_id}",
            timeout=http_timeout,
        )
        res.raise_for_status()
    except Exception as error:
        raise HttpError(res.status_code, res.text) from error
    return {"success": 1}


def get_deployment(deployment_id) -> dict:
    """Proxy api to fetch deployment and its details"""
    res = prefect_get(f"deployments/{deployment_id}")
    return res


def get_flow_run_logs(
    flow_run_id: str, task_run_id: str, limit: int, offset: int
) -> dict:  # pragma: no cover
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(
        f"flow_runs/logs/{flow_run_id}",
        params={"offset": offset, "limit": limit, "task_run_id": task_run_id},
    )
    return {"logs": res}


def get_flow_run_logs_v2(flow_run_id: str) -> dict:  # pragma: no cover
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(
        f"flow_runs/v1/logs/{flow_run_id}",
    )
    return res


def get_flow_run_graphs(flow_run_id: str) -> dict:
    """retreive the tasks from a flow-run from prefect"""
    res = prefect_get(
        f"flow_runs/graph/{flow_run_id}",
    )
    return res


def delete_flow_run(flow_run_id: str) -> dict:
    """retreive the logs from a flow-run from prefect"""
    res = prefect_delete(f"flow_runs/{flow_run_id}")
    return res


def get_flow_run(flow_run_id: str) -> dict:
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(f"flow_runs/{flow_run_id}")
    return res


def get_flow_run_poll(flow_run_id: str) -> dict:
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(f"flow_runs/{flow_run_id}/poll")
    return res


def create_deployment_flow_run(
    deployment_id: str,
    flow_run_params: dict = None,
) -> dict:  # pragma: no cover
    """Proxy call to create a flow run for deployment."""
    res = prefect_post(
        f"deployments/{deployment_id}/flow_run",
        flow_run_params if flow_run_params else {},
    )
    return res


def schedule_deployment_flow_run(
    deployment_id: str, flow_run_params: dict = None, scheduled_time: datetime = None
) -> dict:  # pragma: no cover
    """
    Proxy call to create a flow run for deployment.
    """
    res = prefect_post(
        f"deployments/{deployment_id}/flow_run/schedule",
        {
            "runParams": flow_run_params,
            "scheduledTime": str(scheduled_time) if scheduled_time else None,
        },
    )
    return res


def lock_tasks_for_deployment(
    deployment_id: str,
    orguser: OrgUser,
    dataflow_orgtasks: list[DataflowOrgTask] = [],
):
    """locks all orgtasks for a deployment"""
    orgtask_ids = []

    # if we have the orgtasks available dont do the extra query
    if len(dataflow_orgtasks) == 0:
        dataflow_orgtasks = DataflowOrgTask.objects.filter(
            dataflow__deployment_id=deployment_id
        ).all()

    orgtask_ids = [df_orgtask.orgtask.id for df_orgtask in dataflow_orgtasks]
    lock = TaskLock.objects.filter(orgtask_id__in=orgtask_ids).first()
    if lock:
        logger.info(f"{lock.locked_by.user.email} is running this pipeline right now")
        raise HttpError(400, f"{lock.locked_by.user.email} is running this pipeline right now")

    locks = []
    try:
        with transaction.atomic():
            for df_orgtask in dataflow_orgtasks:
                task_lock = TaskLock.objects.create(
                    orgtask=df_orgtask.orgtask,
                    locked_by=orguser,
                    locking_dataflow=df_orgtask.dataflow,
                )
                locks.append(task_lock)
    except Exception as error:
        raise HttpError(400, "Someone else is trying to run this pipeline. Try again") from error
    return locks


def retry_flow_run(flow_run_id: str, minutes: int = 5):
    """retry a flow run in prefect; after x minutes"""
    res = prefect_post(f"flow_runs/{flow_run_id}/retry", {"minutes": minutes})
    return res


def recurse_flow_run_logs(
    flow_run_id: str,
    task_run_id: str = None,
    limit: int = FLOW_RUN_LOGS_OFFSET_LIMIT,
    offset: int = 0,
):
    """recursively fetch logs for a flow run"""
    logs = []
    while True:
        new_logs_set = get_flow_run_logs(flow_run_id, task_run_id, limit, offset)
        curr_logs = new_logs_set["logs"]["logs"]
        logs.extend(curr_logs)
        if len(curr_logs) == limit:
            offset += limit
        elif len(curr_logs) < limit:
            break
        else:
            logger.info(f"Something weird happening in fetching logs for {flow_run_id}")
            break

    return logs


def get_long_running_flow_runs(nhours: int):
    """gets long running flow runs from prefect"""
    flow_runs = prefect_get(f"flow_runs/long-running/{nhours}")
    return flow_runs["flow_runs"]


def get_prefect_version():
    """Fetch secret block id and block name"""
    response = prefect_get("prefect/version")
    return response


def upsert_dbt_cloud_creds_block(block_name: str, account_id: int, api_key: str) -> dict:
    """Create a dbt cloud creds block in prefect; using patch style create or udpate"""
    response = prefect_patch(
        "blocks/dbtcloudcreds/",
        {"block_name": block_name, "account_id": account_id, "api_key": api_key},
    )
    return response


def create_or_update_dbt_cloud_creds_block(
    org: Org,
    account_id: int,
    api_key: str,
) -> OrgPrefectBlockv1:
    """Create a dbt cli profile block in that has the warehouse information"""
    cloud_creds_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=DBTCLOUDCREDS).first()
    block_name = None

    if not cloud_creds_block:
        block_name = f"{org.slug}-dbtcloud-creds"
        cloud_creds_block = OrgPrefectBlockv1(
            org=org,
            block_type=DBTCLOUDCREDS,
            block_name=block_name,
        )
    else:
        block_name = cloud_creds_block.block_name

    result = upsert_dbt_cloud_creds_block(block_name, account_id, api_key)

    cloud_creds_block.block_id = result["block_id"]
    cloud_creds_block.block_name = result["block_name"]
    cloud_creds_block.save()

    return cloud_creds_block


def cancel_queued_manual_job(flow_run_id: str, payload):
    """Cancels a queued manual sync"""
    res = prefect_post(
        f"flow_runs/{flow_run_id}/set_state",
        payload,
    )
    return res


def get_late_flow_runs(payload: FilterLateFlowRunsRequest) -> list[dict]:
    res = prefect_post("flow_runs/late", payload.dict())

    return res["flow_runs"]


def post_prefect_workers_count(work_queue_name: str, work_pool_name: str) -> int:
    res = prefect_post(
        "workers/filter/",
        {"work_queue_names": [work_queue_name], "work_pool_names": [work_pool_name]},
    )

    return res["count"]


############################## Related to estimation of flow run times ##############################


def compute_dataflow_run_times_from_history(
    dataflow: OrgDataFlowv1, limit=20, statuses_to_include: list[str] = None
) -> DeploymentRunTimes:
    """
    Estimate how much time does a flow run of this deployment might take to run
    Use the past history (last "limit" flow runs) to compute average, max & min times of the run
    Save the estimates to db
    """
    if statuses_to_include is None:
        statuses_to_include = ["COMPLETED"]

    flow_runs = PrefectFlowRun.objects.filter(
        deployment_id=dataflow.deployment_id,
        status__in=statuses_to_include,  # failed runs mostly have a time close to 0 or very high, which could skew the averages
    ).order_by("-start_time")[:limit]

    flow_runs = flow_runs.annotate(start_date=TruncDate("start_time")).values(
        "deployment_id", "start_date", "total_run_time"
    )

    flow_runs_list = list(flow_runs)

    if not flow_runs_list:
        return DeploymentRunTimes(
            max_run_time=-1,
            min_run_time=-1,
            avg_run_time=-1,
            wt_avg_run_time=-1,
        )

    grouped_data = {
        (deploy_id, st_date): list(item)
        for (deploy_id, st_date), item in groupby(
            flow_runs_list, key=itemgetter("deployment_id", "start_date")
        )
    }

    # a deployment may have multiple runs during any given day
    # use the average run time per day to do further processing
    run_times_per_day = {
        (deploy_id, st_date): {
            "run_time_per_day_avg": mean(item["total_run_time"] for item in v),
        }
        for (deploy_id, st_date), v in grouped_data.items()
    }

    max_run_time = float("-inf")
    min_run_time = float("inf")
    avg_sum = 0
    avg_denominator = 0
    wt_avg_sum = 0
    wt_avg_denominator = 0
    cnt = len(run_times_per_day.keys())
    for (deploy_id, st_date), run_time in run_times_per_day.items():
        max_run_time = max(run_time["run_time_per_day_avg"], max_run_time)
        min_run_time = min(run_time["run_time_per_day_avg"], min_run_time)

        avg_sum += run_time["run_time_per_day_avg"]
        avg_denominator += 1

        wt_avg_sum += (
            cnt * run_time["run_time_per_day_avg"]
        )  # give the most recent run; the max weight
        wt_avg_denominator += cnt

        cnt -= 1

    all_run_times = DeploymentRunTimes()
    if len(run_times_per_day.keys()) > 0:
        all_run_times = DeploymentRunTimes(
            max_run_time=math.ceil(max_run_time),
            min_run_time=math.ceil(min_run_time),
            avg_run_time=math.ceil(avg_sum / avg_denominator),
            wt_avg_run_time=math.ceil(wt_avg_sum / wt_avg_denominator),
        )

        dataflow.meta = all_run_times.dict()
        dataflow.save()

    return all_run_times


def estimate_time_for_next_queued_run_of_dataflow(
    dataflow: OrgDataFlowv1,
) -> DeploymentCurrentQueueTime:
    """
    This function worst case and best case scenarios. For each scenario, we will get
    1. Queue no (queue_no) - how many flow runs are scheduled before the dataflow's next flow run
    2. Queue time (min max queue times) - time that user needs to wait before the next scheduled flow run will trigger
    """

    # Check if the flag is set and is true
    if not os.getenv("ESTIMATE_TIME_FOR_QUEUE_RUNS", "false").lower() == "true":
        logger.info("ESTIMATE_TIME_FOR_QUEUE_RUNS flag is not set or is false. Returning early.")
        return DeploymentCurrentQueueTime(
            queue_no=-1,
            min_wait_time=-1,
            max_wait_time=-1,
        )

    # get the current late run for the deployment
    dataflow_late_runs = get_late_flow_runs(
        payload=FilterLateFlowRunsRequest(deployment_id=dataflow.deployment_id, limit=1)
    )

    if not dataflow_late_runs:
        logger.info(f"No late run found for the dataflow {dataflow.deployment_name}")
        return DeploymentCurrentQueueTime(
            queue_no=-1,
            min_wait_time=-1,
            max_wait_time=-1,
        )

    current_queued_flow_run = dataflow_late_runs[0]

    logger.info(
        f"Found the late run for the dataflow with flow run id: {current_queued_flow_run['id']}"
    )

    # read the queue name and work pool of this flow run
    queue_name = current_queued_flow_run["workQueueName"]
    work_pool_name = current_queued_flow_run["workPoolName"]  # we only have one work pool

    if not queue_name or not work_pool_name:
        logger.info(
            f"Couldn't map the late run to a work pool or a queue. work pool : {work_pool_name} queue: {queue_name}"
        )
        return DeploymentCurrentQueueTime(
            queue_no=-1,
            min_wait_time=-1,
            max_wait_time=-1,
        )

    logger.info(f"Current late run is on the pool: {work_pool_name} and queue: {queue_name}")

    # fetch flow runs ("Late") that are in the same queue and work pool before the current run
    queued_late_flow_runs = get_late_flow_runs(
        payload=FilterLateFlowRunsRequest(
            work_pool_name=work_pool_name,
            work_queue_name=queue_name,
            limit=100,
            before_start_time=current_queued_flow_run["expectedStartTime"],
            exclude_flow_run_ids=[current_queued_flow_run["id"]],
        )
    )

    logger.info(
        f"No of queued runs before the current late dataflow run : {len(queued_late_flow_runs)}"
    )

    # for the above flow runs look at their deployment_id and use any of the run_times to compute queue_time
    run_time_type = "wt_avg_run_time"
    queue_no = 1
    queue_time_in_seconds = (
        5  # 5 secs of approx latency of the flow run going from pos no 1 to the prefect worker
    )
    for flow_run in queued_late_flow_runs:
        deployment_meta = (
            OrgDataFlowv1.objects.filter(deployment_id=flow_run["deployment_id"]).first().meta
        )

        queue_no += 1  # even if we dont have the time we can atleast give them the right queue no
        if (
            deployment_meta
            and run_time_type in deployment_meta
            and deployment_meta[run_time_type] > 0
        ):  # we should have a positive time
            queue_time_in_seconds += deployment_meta[run_time_type]

    # find no of workers listening to the queue and adjust the queue_no & time accordingly
    min_workers = 1
    max_workers = post_prefect_workers_count(
        work_queue_name=queue_name, work_pool_name=work_pool_name
    )

    logger.info(f"Max workers for the queue {queue_name} : {max_workers}")

    return DeploymentCurrentQueueTime(
        queue_no=queue_no,
        min_wait_time=math.ceil(queue_time_in_seconds / max_workers),
        max_wait_time=math.ceil(queue_time_in_seconds / min_workers),
    )


####################################################################################################
