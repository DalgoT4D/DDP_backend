import os
from typing import Union
import requests

from dotenv import load_dotenv
from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
    DbtProfile,
    DbtCredentialsPostgres,
)
from ddpui.ddpprefect import (
    AIRBYTECONNECTION,
    AIRBYTESERVER,
    SHELLOPERATION,
    DBTCORE,
    FLOW_RUN_COMPLETED,
    FLOW_RUN_FAILED,
)


load_dotenv()


def prefect_get(endpoint):
    """GET request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}")
    res.raise_for_status()
    return res.json()


def prefect_post(endpoint, json):
    """POST request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", json=json)
    res.raise_for_status()
    return res.json()


def prefect_patch(endpoint, json):
    """PATCH request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.patch(f"{root}/{endpoint}", json=json)
    res.raise_for_status()
    return res.json()


def prefect_delete(endpoint):
    """DELETE request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}")
    res.raise_for_status()
    return


def get_block_type(querystr):
    """Fetch prefect block type"""
    res = prefect_post(
        "block_types/filter",
        {
            "block_types": {
                "name": {"like_": querystr},
            }
        },
    )
    if len(res) != 1:
        raise Exception(
            f'Expected exactly one prefect block type for query "{querystr}", received {len(res)} instead'
        )
    blocktype = res[0]
    return blocktype


def get_block_schema_type(querystr, blocktypeid=None):
    """Fetch prefect block type schema"""
    if blocktypeid is None:
        blocktypeid = get_block_type(querystr)["id"]
    res = prefect_post(
        "block_schemas/filter",
        {
            "block_schemas": {
                "operator": "and_",
                "block_type_id": {"any_": [blocktypeid]},
            }
        },
    )
    if len(res) != 1:
        raise Exception(
            f'Expected exactly one prefect block schema for query "{blocktypeid}", received {len(res)} instead'
        )
    blockschematype = res[0]
    return blockschematype


def get_block(blocktype, blockname):
    """Fetch prefect block"""
    blocktype_id = get_block_type(blocktype)["id"]
    res = prefect_post(
        "block_documents/filter",
        {
            "block_documents": {
                "operator": "and_",
                "block_type_id": {"any_": [blocktype_id]},
                "name": {"any_": [blockname]},
            }
        },
    )
    if len(res) > 1:
        raise Exception(f"Expected at most one {blocktype} block named {blockname}")
    if len(res) == 0:
        return None
    block = res[0]
    return block


def get_block_by_id(block_id):
    """Fetch prefect block by id"""
    block = prefect_get(
        f"block_documents/{block_id}",
    )
    return block


def get_blocks(blocktype, blockname=""):
    """Fetch prefect blocks"""
    blocktype_id = get_block_type(blocktype)["id"]
    query = {
        "block_documents": {
            "operator": "and_",
            "block_type_id": {"any_": [blocktype_id]},
        }
    }
    if len(blockname) > 0:
        query["block_documents"]["name"] = {"any_": [blockname]}
    res = prefect_post("block_documents/filter", query)
    return res


def create_airbyte_server_block(blockname):
    """Create airbyte server block in prefect"""
    airbyte_server_blocktype_id = get_block_type(AIRBYTESERVER)["id"]
    airbyte_server_blockschematype_id = get_block_schema_type(
        AIRBYTESERVER, airbyte_server_blocktype_id
    )["id"]

    res = prefect_post(
        "block_documents/",
        {
            "name": blockname,
            "block_type_id": airbyte_server_blocktype_id,
            "block_schema_id": airbyte_server_blockschematype_id,
            "data": {
                # 'username': ,
                # 'password': ,
                "server_host": os.getenv("AIRBYTE_SERVER_HOST"),
                "server_port": os.getenv("AIRBYTE_SERVER_PORT"),
                "api_version": os.getenv("AIRBYTE_SERVER_APIVER"),
            },
            "is_anonymous": False,
        },
    )
    return res


# todo
def update_airbyte_server_block(blockname):
    """Update the airbyte server block with a particular block name"""
    return {}


def delete_airbyte_server_block(blockid):
    """Delete airbyte server block"""
    return prefect_delete(f"block_documents/{blockid}")


def create_airbyte_connection_block(conninfo: PrefectAirbyteConnectionSetup):
    """Create airbyte connection block"""
    airbyte_connection_blocktype_id = get_block_type(AIRBYTECONNECTION)["id"]
    airbyte_connection_blockschematype_id = get_block_schema_type(
        AIRBYTECONNECTION, airbyte_connection_blocktype_id
    )["id"]

    serverblock = get_block(AIRBYTESERVER, conninfo.serverBlockName)
    if serverblock is None:
        raise Exception(
            f"could not find {AIRBYTESERVER} block called {conninfo.serverBlockName}"
        )

    res = prefect_post(
        "block_documents/",
        {
            "name": conninfo.connectionBlockName,
            "block_type_id": airbyte_connection_blocktype_id,
            "block_schema_id": airbyte_connection_blockschematype_id,
            "data": {
                "airbyte_server": {"$ref": {"block_document_id": serverblock["id"]}},
                "connection_id": conninfo.connectionId,
            },
            "is_anonymous": False,
        },
    )

    return res


# todo
def update_airbyte_connection_block(blockname):
    """Update the airbyte server block with a particular block name"""
    return {}


def delete_airbyte_connection_block(blockid):
    """Delete airbyte connection block in prefect"""
    return prefect_delete(f"block_documents/{blockid}")


def create_shell_block(shell: PrefectShellSetup):
    """Create a prefect shell block"""
    shell_blocktype_id = get_block_type(SHELLOPERATION)["id"]
    shell_blockschematype_id = get_block_schema_type(
        SHELLOPERATION, shell_blocktype_id
    )["id"]

    res = prefect_post(
        "block_documents/",
        {
            "name": shell.blockname,
            "block_type_id": shell_blocktype_id,
            "block_schema_id": shell_blockschematype_id,
            "data": {
                "working_dir": shell.working_dir,
                "env": shell.env,
                "commands": shell.commands,
            },
            "is_anonymous": False,
        },
    )
    return res


def delete_shell_block(blockid):
    """Delete a prefect shell block"""
    return prefect_delete(f"block_documents/{blockid}")


def create_dbt_core_block(
    dbtcore: PrefectDbtCoreSetup,
    profile: DbtProfile,
    credentials: Union[DbtCredentialsPostgres, None],
):
    """Create a dbt core block in prefect"""
    dbtcore_blocktype_id = get_block_type(DBTCORE)["id"]
    dbtcore_blockschematype_id = get_block_schema_type(DBTCORE, dbtcore_blocktype_id)[
        "id"
    ]

    dbt_cli_profile = {
        "name": profile.name,
        "target": profile.target,
        "target_configs": {
            "type": profile.target_configs_type,
            "schema": profile.target_configs_schema,
            "credentials": None,
        },
    }
    if profile.target_configs_type == "postgres":
        dbt_cli_profile["target_configs"]["credentials"] = {
            "driver": "postgresql+psycopg2",
            "host": credentials.host,
            "port": credentials.port,
            "username": credentials.username,
            "password": credentials.password,
            "database": credentials.database,
        }
    else:
        raise Exception(
            f"unrecognized target_configs_type {profile.target_configs_type}"
        )

    res = prefect_post(
        "block_documents/",
        {
            "name": dbtcore.block_name,
            "block_type_id": dbtcore_blocktype_id,
            "block_schema_id": dbtcore_blockschematype_id,
            "data": {
                "profiles_dir": dbtcore.profiles_dir,
                "project_dir": dbtcore.project_dir,
                "working_dir": dbtcore.working_dir,
                "env": dbtcore.env,
                "dbt_cli_profile": dbt_cli_profile,
                "commands": dbtcore.commands,
            },
            "is_anonymous": False,
        },
    )
    return res


def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    return prefect_delete(f"block_documents/{block_id}")


# Flows and deployments
def get_flow_runs_by_deployment_id(deployment_id, limit=None):
    """Fetch flow runs of a deployment that are FAILED/COMPLETED sorted desc by start time of each run"""
    query = {
        "sort": "START_TIME_DESC",
        "deployments": {"id": {"any_": [deployment_id]}},
        "flow_runs": {
            "operator": "and_",
            "state": {"type": {"any_": [FLOW_RUN_COMPLETED, FLOW_RUN_FAILED]}},
        },
    }

    if limit:
        query["limit"] = limit

    filtered_res = []

    for flow_run in prefect_post("flow_runs/filter", query):
        filtered_res.append(
            {
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "status": flow_run["state"]["type"],
            }
        )

    return filtered_res


def get_last_flow_run_by_deployment_id(deployment_id):
    """Fetch flow runs of a deployment that are FAILED/COMPLETED sorted desc by start time of each run"""

    res = get_flow_runs_by_deployment_id(deployment_id, limit=1)

    if len(res) > 0:
        return res[0]

    return None


def get_deployments_by_org_slug(org_slug):
    """Fetch all deployments by org slug"""
    res = prefect_post(
        "deployments/filter",
        {"deployments": {"tags": {"all_": [org_slug]}}},
    )

    filtered_res = []

    for deployment in res:
        filtered_res.append(
            {
                "name": deployment["name"],
                "id": deployment["id"],
                "tags": deployment["tags"],
                "cron": deployment["schedule"]["cron"],
            }
        )

    return filtered_res
