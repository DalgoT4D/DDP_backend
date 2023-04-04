import os
from typing import Union
import requests

from dotenv import load_dotenv
from prefect import flow
from prefect_airbyte import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
from prefect_dbt.cli.commands import DbtCoreOperation
from ddpui.ddpprefect.schema import (
    PrefectDbtCoreSetup,
    PrefectShellSetup,
    PrefectAirbyteConnectionSetup,
)
from ddpui.ddpprefect.schema import DbtProfile, DbtCredentialsPostgres


load_dotenv()


# prefect block names
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"


def prefect_get(endpoint):
    """Docstring"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}")
    res.raise_for_status()
    return res.json()


def prefect_post(endpoint, json):
    """Docstring"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", json=json)
    res.raise_for_status()
    return res.json()


def prefect_patch(endpoint, json):
    """Docstring"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.patch(f"{root}/{endpoint}", json=json)
    res.raise_for_status()
    return res.json()


def prefect_delete(endpoint):
    """Docstring"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}")
    res.raise_for_status()
    return res.json()


def get_block_type(querystr):
    """Docstring"""
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
    """Docstring"""
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
    """Docstring"""
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


def create_airbyte_server_block(blockname):
    """Docstring"""
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


def delete_airbyte_server_block(blockid):
    """Docstring"""
    return prefect_delete(f"block_documents/{blockid}")


def create_airbyte_connection_block(conninfo: PrefectAirbyteConnectionSetup):
    """Docstring"""
    airbyte_connection_blocktype_id = get_block_type(AIRBYTECONNECTION)["id"]
    airbyte_connection_blockschematype_id = get_block_schema_type(
        AIRBYTECONNECTION, airbyte_connection_blocktype_id
    )["id"]

    serverblock = get_block(AIRBYTESERVER, conninfo.serverblockname)
    if serverblock is None:
        raise Exception(
            f"could not find {AIRBYTESERVER} block called {conninfo.serverblockname}"
        )

    res = prefect_post(
        "block_documents/",
        {
            "name": conninfo.connectionblockname,
            "block_type_id": airbyte_connection_blocktype_id,
            "block_schema_id": airbyte_connection_blockschematype_id,
            "data": {
                "airbyte_server": serverblock["id"],
                "connection_id": conninfo.connection_id,
            },
            "is_anonymous": False,
        },
    )
    return res


def delete_airbyte_connection_block(blockid):
    """Docstring"""
    return prefect_delete(f"block_documents/{blockid}")


def create_shell_block(shell: PrefectShellSetup):
    """Docstring"""
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
    """Docstring"""
    return prefect_delete(f"block_documents/{blockid}")


def create_dbt_core_block(
    dbtcore: PrefectDbtCoreSetup,
    profile: DbtProfile,
    credentials: Union[DbtCredentialsPostgres, None],
):
    """Docstring"""
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
            "name": dbtcore.blockname,
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


def delete_dbt_core_block(blockid):
    """Docstring"""
    return prefect_delete(f"block_documents/{blockid}")


@flow
def run_airbyte_connection_prefect_flow(blockname):
    """Docstring"""
    airbyte_connection = AirbyteConnection.load(blockname)
    return run_connection_sync(airbyte_connection)


@flow
def run_dbtcore_prefect_flow(blockname):
    """Docstring"""
    dbt_op = DbtCoreOperation.load(blockname)
    dbt_op.run()
