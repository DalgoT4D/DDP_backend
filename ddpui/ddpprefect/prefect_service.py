import os
import requests

from dotenv import load_dotenv

from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
)
from ddpui.ddpprefect.schema import DbtProfile


load_dotenv()

PREFECT_PROXY_API_URL = os.getenv("PREFECT_PROXY_API_URL")
# prefect block names
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"

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
    response = requests.get(f"{PREFECT_PROXY_API_URL}/proxy/blocks/airbyte/connection/{blockname}", timeout=30)
    response.raise_for_status()
    return response.json()['block_id']


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
def run_airbyte_connection_prefect_flow(blockname):
    """run an airbyte connection sync"""
    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/flows/airbyte/connection/sync/", timeout=30, json={
        "blockName": blockname
    })
    return response.json()

def run_dbtcore_prefect_flow(blockname):
    """run a dbt block sync"""
    response = requests.post(f"{PREFECT_PROXY_API_URL}/proxy/flows/dbtcore/run/", timeout=30, json={
        "blockName": blockname
    })
    return response.json()
