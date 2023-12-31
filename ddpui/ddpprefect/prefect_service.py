import os
import requests

from ninja.errors import HttpError
from dotenv import load_dotenv
from django.db import transaction
from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
    PrefectAirbyteSync,
    PrefectDataFlowCreateSchema2,
    PrefectDataFlowCreateSchema3,
    PrefectDbtCore,
    PrefectDataFlowUpdateSchema2,
    PrefectSecretBlockCreate,
    PrefectShellTaskSetup,
    PrefectDbtTaskSetup,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.tasks import DataflowOrgTask, TaskLock
from ddpui.models.orgjobs import BlockLock, DataflowBlock
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun

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


# ================================================================================================
def get_airbyte_server_block_id(blockname) -> str | None:
    """get the block_id for the server block having this name"""
    response = prefect_get(f"blocks/airbyte/server/{blockname}")
    return response["block_id"]


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
    """We don't update server blocks"""
    raise Exception("not implemented")


def delete_airbyte_server_block(block_id):
    """Delete airbyte server block"""
    prefect_delete_a_block(block_id)


# ================================================================================================
def get_airbyte_connection_block_id(blockname) -> str | None:
    """get the block_id for the connection block having this name"""
    response = prefect_get(
        f"blocks/airbyte/connection/byblockname/{blockname}",
    )
    return response["block_id"]


def get_airbye_connection_blocks(block_names) -> dict:
    """Filter out blocks by query params"""
    response = prefect_post(
        "blocks/airbyte/connection/filter",
        {"block_names": block_names},
    )
    return response


def get_airbyte_connection_block_by_id(block_id: str) -> dict:
    """look up a prefect airbyte-connection block by id"""
    response = prefect_get(
        f"blocks/airbyte/connection/byblockid/{block_id}",
    )
    return response


def create_airbyte_connection_block(
    conninfo: PrefectAirbyteConnectionSetup,
) -> str:
    """Create airbyte connection block"""
    response = prefect_post(
        "blocks/airbyte/connection/",
        {
            "serverBlockName": conninfo.serverBlockName,
            "connectionId": conninfo.connectionId,
            "connectionBlockName": conninfo.connectionBlockName,
        },
    )
    return response["block_id"]


def update_airbyte_connection_block(blockname):
    """We don't update connection blocks"""
    raise Exception("not implemented")


def delete_airbyte_connection_block(block_id) -> None:
    """Delete airbyte connection block in prefect"""
    prefect_delete_a_block(block_id)


def post_prefect_blocks_bulk_delete(block_ids: list) -> dict:
    """
    Delete airbyte connection blocks in prefect
    corresponding the connection ids array passed
    """
    response = prefect_post(
        "blocks/bulk/delete/",
        {"block_ids": block_ids},
    )
    return response


# ================================================================================================
def get_shell_block_id(blockname) -> str | None:
    """get the block_id for the shell block having this name"""
    response = prefect_get(f"blocks/shell/{blockname}")
    return response["block_id"]


def create_shell_block(shell: PrefectShellSetup) -> str:
    """Create a prefect shell block"""
    response = prefect_post(
        "blocks/shell/",
        {
            "blockName": shell.blockname,
            "commands": shell.commands,
            "env": shell.env,
            "workingDir": shell.workingDir,
        },
    )
    return response


def delete_shell_block(block_id):
    """Delete a prefect shell block"""
    prefect_delete_a_block(block_id)


# ================================================================================================
def get_dbtcore_block_id(blockname) -> str | None:
    """get the block_id for the dbtcore block having this name"""
    response = prefect_get(f"blocks/dbtcore/{blockname}")
    return response["block_id"]


def create_dbt_core_block(
    dbtcore: PrefectDbtCoreSetup,
    profilename: str,
    cli_profile_block_name: str,
    target: str,
    wtype: str,
    credentials: dict,
    bqlocation: str,
) -> dict:
    """Create a dbt core block in prefect"""
    response = prefect_post(
        "blocks/dbtcore/",
        {
            "blockName": dbtcore.block_name,
            "profile": {
                "name": profilename,
                "target": target,
                "target_configs_schema": target,
            },
            "cli_profile_block_name": cli_profile_block_name,
            "wtype": wtype,
            "credentials": credentials,
            "bqlocation": bqlocation,
            "commands": dbtcore.commands,
            "env": dbtcore.env,
            "working_dir": dbtcore.working_dir,
            "profiles_dir": dbtcore.profiles_dir,
            "project_dir": dbtcore.project_dir,
        },
    )
    return response


def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    prefect_delete_a_block(block_id)


def update_dbt_core_block_credentials(wtype: str, block_name: str, credentials: dict):
    """Update the credentials of a dbt core block in prefect"""
    response = prefect_put(
        f"blocks/dbtcore_edit/{wtype}/",
        {
            "blockName": block_name,
            "credentials": credentials,
        },
    )
    return response


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
    wtype: str,
    profilename: str = None,
    target: str = None,
    credentials: dict = None,
    bqlocation: str = None,
    new_block_name: str = None,
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
            "new_block_name": new_block_name,
        },
    )
    return response


def delete_dbt_cli_profile_block(block_id) -> None:
    """Delete dbt cli profile block in prefect"""
    prefect_delete_a_block(block_id)


# ================================================================================================


def create_secret_block(secret_block: PrefectSecretBlockCreate):
    """This will create a secret block in the prefect to store any password like string"""
    response = prefect_post(
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
def run_airbyte_connection_sync(
    run_flow: PrefectAirbyteSync,
) -> dict:  # pragma: no cover
    """initiates an airbyte connection sync"""
    res = prefect_post(
        "flows/airbyte/connection/sync/",
        json=run_flow.to_json(),
    )
    return res


def run_dbt_core_sync(run_flow: PrefectDbtCore) -> dict:  # pragma: no cover
    """initiates a dbt job sync"""
    res = prefect_post(
        "flows/dbtcore/run/",
        json=run_flow.to_json(),
    )
    return res


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
def create_dataflow(payload: PrefectDataFlowCreateSchema2) -> dict:  # pragma: no cover
    """create a prefect deployment out of a flow and a cron schedule"""
    res = prefect_post(
        "deployments/",
        {
            "flow_name": payload.flow_name,
            "deployment_name": payload.deployment_name,
            "org_slug": payload.orgslug,
            "connection_blocks": [
                {"seq": conn.seq, "blockName": conn.blockName}
                for conn in payload.connection_blocks
            ],
            "dbt_blocks": payload.dbt_blocks,
            "cron": payload.cron,
        },
    )
    return res


def create_dataflow_v1(
    payload: PrefectDataFlowCreateSchema3,
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
        },
    )
    return res


def update_dataflow(
    deployment_id: str, payload: PrefectDataFlowUpdateSchema2
) -> dict:  # pragma: no cover
    """update a prefect deployment with a new cron schedule"""
    res = prefect_put(
        f"deployments/{deployment_id}",
        {
            "cron": payload.cron,
            "connection_blocks": [
                {"seq": conn.seq, "blockName": conn.blockName}
                for conn in payload.connection_blocks
            ],
            "dbt_blocks": payload.dbt_blocks,
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
    for prefect_flow_run in PrefectFlowRun.objects.filter(
        deployment_id=deployment_id
    ).order_by("start_time"):
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


def get_flow_run_logs(flow_run_id: str, offset: int) -> dict:  # pragma: no cover
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(
        f"flow_runs/logs/{flow_run_id}",
        params={"offset": offset},
    )
    return {"logs": res}


def get_flow_run(flow_run_id: str) -> dict:
    """retreive the logs from a flow-run from prefect"""
    res = prefect_get(f"flow_runs/{flow_run_id}")
    return res


def create_deployment_flow_run(deployment_id: str) -> dict:  # pragma: no cover
    """
    Proxy call to create a flow run for deployment.
    """
    res = prefect_post(f"deployments/{deployment_id}/flow_run", {})
    return res


def lock_blocks_for_deployment(deployment_id: str, orguser: OrgUser):
    """locks all orgprefectblocks for a deployment"""
    dataflow_blocks = DataflowBlock.objects.filter(
        dataflow__deployment_id=deployment_id
    )

    block_names = [
        x["opb__block_name"] for x in dataflow_blocks.values("opb__block_name")
    ]
    lock = BlockLock.objects.filter(opb__block_name__in=block_names).first()
    if lock:
        logger.info(f"{lock.locked_by.user.email} is running this pipeline right now")
        raise HttpError(
            400, f"{lock.locked_by.user.email} is running this pipeline right now"
        )

    locks = []
    try:
        with transaction.atomic():
            for df_block in dataflow_blocks:
                blocklock = BlockLock.objects.create(
                    opb=df_block.opb, locked_by=orguser
                )
                locks.append(blocklock)
    except Exception as error:
        raise HttpError(
            400, "Someone else is trying to run this pipeline... try again"
        ) from error
    return locks


def lock_tasks_for_deployment(deployment_id: str, orguser: OrgUser):
    """locks all orgtasks for a deployment"""
    dataflow_orgtasks = DataflowOrgTask.objects.filter(
        dataflow__deployment_id=deployment_id
    ).all()

    orgtask_ids = [df_orgtask.orgtask.id for df_orgtask in dataflow_orgtasks]
    lock = TaskLock.objects.filter(orgtask_id__in=orgtask_ids).first()
    if lock:
        logger.info(f"{lock.locked_by.user.email} is running this pipeline right now")
        raise HttpError(
            400, f"{lock.locked_by.user.email} is running this pipeline right now"
        )

    locks = []
    try:
        with transaction.atomic():
            for df_orgtask in dataflow_orgtasks:
                task_lock = TaskLock.objects.create(
                    orgtask=df_orgtask.orgtask, locked_by=orguser
                )
                locks.append(task_lock)
    except Exception as error:
        raise HttpError(
            400, "Someone else is trying to run this pipeline... try again"
        ) from error
    return locks
