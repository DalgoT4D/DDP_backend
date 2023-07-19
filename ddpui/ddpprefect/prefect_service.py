import os
import requests

from ninja.errors import HttpError
from dotenv import load_dotenv
from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
    PrefectAirbyteSync,
    PrefectDataFlowCreateSchema2,
    PrefectDbtCore,
    PrefectDataFlowUpdateSchema,
)
from ddpui.utils.custom_logger import CustomLogger

load_dotenv()

PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")
http_timeout = int(os.getenv("PREFECT_HTTP_TIMEOUT", "30"))

logger = CustomLogger("prefect")


# ================================================================================================
def prefect_get(endpoint: str, **kwargs) -> dict:
    """make a GET request to the proxy"""
    new_logger = CustomLogger("prefect")
    orgname = new_logger.get_slug()
    headers = kwargs.get("headers", {})
    headers["x-ddp-org"] = orgname
    kwargs["headers"] = headers
    custom_logger = CustomLogger("airbyte")
    try:
        res = requests.get(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            timeout=http_timeout,
            headers=headers,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        custom_logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_post(endpoint: str, json: dict, **kwargs) -> dict:
    """make a POST request to the proxy"""
    new_logger = CustomLogger("prefect")
    orgname = new_logger.get_slug()
    headers = kwargs.get("headers", {})
    headers["x-ddp-org"] = orgname
    kwargs["headers"] = headers
    custom_logger = CustomLogger("airbyte")
    try:
        res = requests.post(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}",
            timeout=http_timeout,
            json=json,
            headers=headers,
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        custom_logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_put(endpoint: str, json: dict) -> dict:
    """make a PUT request to the proxy"""
    custom_logger = CustomLogger("prefect")
    try:
        res = requests.put(
            f"{PREFECT_PROXY_API_URL}/proxy/{endpoint}", timeout=http_timeout, json=json
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        custom_logger.exception(error)
        raise HttpError(res.status_code, res.text) from error
    return res.json()


def prefect_delete_a_block(block_id: str) -> None:
    """makes a DELETE request to the proxy"""
    custom_logger = CustomLogger("airbyte")
    try:
        res = requests.delete(
            f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}", timeout=http_timeout
        )
    except Exception as error:
        raise HttpError(500, "connection error") from error
    try:
        res.raise_for_status()
    except Exception as error:
        custom_logger.exception(error)
        raise HttpError(res.status_code, res.text) from error


# ================================================================================================
def get_airbyte_server_block_id(blockname) -> str | None:
    """get the block_id for the server block having this name"""
    response = prefect_get(f"blocks/airbyte/server/{blockname}")
    return response["block_id"]


def create_airbyte_server_block(blockname) -> str:
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
    return response["block_id"]


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
    return response["block_id"]


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


def update_dataflow(
    deployment_id: str, payload: PrefectDataFlowUpdateSchema
) -> dict:  # pragma: no cover
    """update a prefect deployment with a new cron schedule"""
    res = prefect_put(
        f"deployments/{deployment_id}",
        {
            "cron": payload.cron,
        },
    )
    return res


def get_flow_runs_by_deployment_id(deployment_id: str, limit=None):  # pragma: no cover
    """
    Fetch flow runs of a deployment that are FAILED/COMPLETED
    sorted by descending start time of each run
    """
    res = prefect_get(
        "flow_runs",
        params={"deployment_id": deployment_id, "limit": limit},
    )
    return res["flow_runs"]


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
    This is like a quick check to see if deployment is running
    """
    res = prefect_post(f"deployments/{deployment_id}/flow_run", {})
    return res
