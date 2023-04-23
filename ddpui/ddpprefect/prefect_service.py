import os
import requests

from dotenv import load_dotenv
from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
    DbtProfile,
    PrefectDataFlowCreateSchema2,
)

load_dotenv()

PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")

# ================================================================================================
def get_airbyte_server_block_id(blockname) -> str | None:
    """get the block_id for the server block having this name"""
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/server/{blockname}", timeout=30)
    response.raise_for_status()
    return response.json()['block_id']


def create_airbyte_server_block(blockname) -> str:
    """Create airbyte server block in prefect"""

    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/server/", timeout=30, json={
        "blockName": blockname,
        "serverHost": os.getenv("AIRBYTE_SERVER_HOST"),
        "serverPort": os.getenv("AIRBYTE_SERVER_PORT"),
        "apiVersion": os.getenv("AIRBYTE_SERVER_APIVER"),
    })
    response.raise_for_status()
    return response.json()['block_id']


def update_airbyte_server_block(blockname):
    """We don't update server blocks"""
    raise Exception("not implemented")


def delete_airbyte_server_block(block_id):
    """Delete airbyte server block"""
    requests.delete(f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}", timeout=30)


# ================================================================================================
def get_airbyte_connection_block_id(blockname) -> str | None:
    """get the block_id for the connection block having this name"""
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/connection/byblockname/{blockname}", timeout=30)
    response.raise_for_status()
    return response.json()['block_id']

def get_airbyte_connection_block_by_id(block_id: str):
    """look up a prefect airbyte-connection block by id"""
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/connection/byblockid/{block_id}", timeout=30)
    response.raise_for_status()
    return response.json()

def create_airbyte_connection_block(
    conninfo: PrefectAirbyteConnectionSetup,
) -> str:
    """Create airbyte connection block"""

    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/connection/", timeout=30, json={
        "serverBlockName": conninfo.serverBlockName,
        "connectionId": conninfo.connectionId,
        "connectionBlockName": conninfo.connectionBlockName,
    })
    response.raise_for_status()
    return response.json()['block_id']


def update_airbyte_connection_block(blockname):
    """We don't update connection blocks"""
    raise Exception("not implemented")


def delete_airbyte_connection_block(block_id):
    """Delete airbyte connection block in prefect"""
    requests.delete(f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}", timeout=30)


# ================================================================================================
def get_shell_block_id(blockname) -> str | None:
    """get the block_id for the shell block having this name"""
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/shell/{blockname}", timeout=30)
    response.raise_for_status()
    return response.json()['block_id']


def create_shell_block(shell: PrefectShellSetup):
    """Create a prefect shell block"""

    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/blocks/shell/", timeout=30, json={
        "blockName": shell.blockname,
        "commands": shell.commands, 
        "env": shell.env, 
        "workingDir": shell.workingDir,
    })
    response.raise_for_status()
    return response.json()['block_id']


def delete_shell_block(block_id):
    """Delete a prefect shell block"""
    requests.delete(f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}", timeout=30)


# ================================================================================================
def get_dbtcore_block_id(blockname) -> str | None:
    """get the block_id for the dbtcore block having this name"""
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/dbtcore/{blockname}", timeout=30)
    response.raise_for_status()
    return response.json()['block_id']


def create_dbt_core_block(
    dbtcore: PrefectDbtCoreSetup, profile: DbtProfile, wtype: str, credentials: dict
):
    """Create a dbt core block in prefect"""

    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/blocks/dbtcore/", timeout=30, json={
        "blockName": dbtcore.block_name,
        "profile": {
            "name": profile.name,
            "target": profile.target,
            "target_configs_schema": profile.target_configs_schema,
        },
        "wtype": wtype,
        "credentials": credentials,

        "commands": dbtcore.commands,
        "env": dbtcore.env,
        "working_dir": dbtcore.working_dir,
        "profiles_dir": dbtcore.profiles_dir,
        "project_dir": dbtcore.project_dir

    })
    response.raise_for_status()
    return response.json()['block_id']


def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    requests.delete(f"{PREFECT_PROXY_API_URL}/delete-a-block/{block_id}", timeout=30)


# ================================================================================================
def run_airbyte_connection_sync(block_name: str):
    """initiates an airbyte connection sync"""
    res = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/flows/airbyte/connection/sync/", timeout=30, json={
        "blockName": block_name
    })
    res.raise_for_status()
    return res.json()


def run_dbt_core_sync(block_name: str):
    """initiates a dbt job sync"""
    res = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/flows/dbtcore/run/", timeout=30, json={
        "blockName": block_name
    })
    res.raise_for_status()
    return res.json()


def create_dataflow(payload: PrefectDataFlowCreateSchema2):
    """create a prefect deployment out of a flow and a cron schedule"""
    res = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/deployments/", timeout=30, json={
        "flow_name": payload.flow_name,
        "deployment_name": payload.deployment_name,
        "org_slug": payload.orgslug,
        "connection_blocks": payload.connection_blocks,
        "dbt_blocks": payload.dbt_blocks,
        "cron": payload.cron
    })
    res.raise_for_status()
    return res.json()


# Flows and deployments
def get_flow_runs_by_deployment_id(deployment_id, limit=None):
    """Fetch flow runs of a deployment that are FAILED/COMPLETED sorted by descending start time of each run"""
    res = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/flow_runs", timeout=30, params={
        "deployment_id": deployment_id,
        "limit": limit
    })
    res.raise_for_status()
    return res.json()['flow_runs']


def get_last_flow_run_by_deployment_id(deployment_id):
    """Fetch most recent flow run of a deployment that is FAILED/COMPLETED"""
    res = get_flow_runs_by_deployment_id(deployment_id, limit=1)
    if len(res) > 0:
        return res[0]
    return None


def get_deployments_by_org_slug(org_slug):
    """Fetch all deployments by org slug"""
    res = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/deployments", timeout=30, params={
        "org_slug": org_slug,
    })
    res.raise_for_status()
    return res.json()['flow_runs']
