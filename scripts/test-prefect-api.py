import os
import django

import uuid

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
# os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "false"
django.setup()

import argparse
import json
from ddpui.ddpprefect import prefect_service as prefectapi
from ddpui.ddpprefect import schema as prefectschemas

from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
args = parser.parse_args()

# == airbyte ==
# x = prefectapi.get_block_type(prefectapi.AIRBYTESERVER)
# print("airbyte server block type = " + x["id"])

# x = prefectapi.get_block_schema_type(prefectapi.AIRBYTESERVER)
# print("airbyte server block schema type = " + x["id"])


# ================================================================================================
AIRBYTE_BLOCKNAME = "absrvr-unittest"
try:
    prefectapi.create_airbyte_server_block(AIRBYTE_BLOCKNAME)
except ValueError:
    print("server block already exists, continuing")

server_block_id = prefectapi.get_airbyte_server_block_id(AIRBYTE_BLOCKNAME)

# ================================================================================================
# create a connection block referencing this server block
AIRBYTE_CONNECTION_BLOCKNAME = "abconn-unittest"
try:
    prefectapi.create_airbyte_connection_block(
        prefectschemas.PrefectAirbyteConnectionSetup(
            serverBlockName=AIRBYTE_BLOCKNAME,
            connectionBlockName=AIRBYTE_CONNECTION_BLOCKNAME,
            connectionId=str(uuid.uuid4()),
        )
    )
except ValueError:
    print("connection block already exists, continuing")

connection_block_id = prefectapi.get_airbyte_connection_block_id(
    AIRBYTE_CONNECTION_BLOCKNAME
)

prefectapi.delete_airbyte_connection_block(connection_block_id)

# ================================================================================================
# clean up the server block
prefectapi.delete_airbyte_server_block(server_block_id)

# ================================================================================================
# == shell ==
SHELLOP_BLOCKNAME = "shellop-unittest"
try:
    prefectapi.create_shell_block(
        prefectapi.PrefectShellSetup(
            blockname=SHELLOP_BLOCKNAME,
            workingDir=os.getcwd(),
            env={"VAR": "VAL"},
            commands=["shellcmd1", "shellcmd2"],
        )
    )
except ValueError:
    print("shell block already exists, continuing")

shell_block_id = prefectapi.get_shell_block_id(SHELLOP_BLOCKNAME)

prefectapi.delete_shell_block(shell_block_id)

# ================================================================================================
# == dbt ==
DBT_BINARY = os.getenv("TESTING_DBT_BINARY")
DBT_PROJECT_DIR = os.getenv("TESTING_DBT_PROJECT_DIR")
DBT_TARGET = os.getenv("TESTING_DBT_TARGET")
DBT_PROFILE = os.getenv("TESTING_DBT_PROFILE")
WAREHOUSETYPE = os.getenv("TESTING_WAREHOUSETYPE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("TESTING_DBT_TARGETCONFIGS_SCHEMA")
DBT_CREDENTIALS_USERNAME = os.getenv("TESTING_DBT_CREDENTIALS_USERNAME")
DBT_CREDENTIALS_PASSWORD = os.getenv("TESTING_DBT_CREDENTIALS_PASSWORD")
DBT_CREDENTIALS_DATABASE = os.getenv("TESTING_DBT_CREDENTIALS_DATABASE")
DBT_CREDENTIALS_HOST = os.getenv("TESTING_DBT_CREDENTIALS_HOST")
DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE = os.getenv(
    "TESTING_DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE"
)

if os.path.exists(f"{DBT_PROJECT_DIR}/profiles/profiles.yml"):
    os.remove(f"{DBT_PROJECT_DIR}/profiles/profiles.yml")

DBT_BLOCKNAME = "dbt-core-op-unittest-1"

dbtcore = prefectschemas.PrefectDbtCoreSetup(
    block_name=DBT_BLOCKNAME,
    profiles_dir=f"{DBT_PROJECT_DIR}/profiles/",
    project_dir=DBT_PROJECT_DIR,
    working_dir=DBT_PROJECT_DIR,
    env={},
    commands=[f"{DBT_BINARY} run --target {DBT_TARGET}"],
)

dbtprofile = prefectschemas.DbtProfile(
    name=DBT_PROFILE,
    target=DBT_TARGET,
    target_configs_schema=DBT_TARGETCONFIGS_SCHEMA,
)
if WAREHOUSETYPE == "postgres":
    dbtcredentials = {
        "username": DBT_CREDENTIALS_USERNAME,
        "password": DBT_CREDENTIALS_PASSWORD,
        "database": DBT_CREDENTIALS_DATABASE,
        "host": DBT_CREDENTIALS_HOST,
        "port": 5432,
    }
elif WAREHOUSETYPE == "bigquery":
    with open(DBT_BIGQUERY_SERVICE_ACCOUNT_CREDSFILE, "r", -1, "utf8") as credsfile:
        dbtcredentials = json.loads(credsfile.read())

try:
    prefectapi.create_dbt_core_block(
        dbtcore, dbtprofile, WAREHOUSETYPE, dbtcredentials
    )
except ValueError:
    print(f"dbt core op block {dbtcore.block_name} already exists, continuing")

block_id = prefectapi.get_dbtcore_block_id(DBT_BLOCKNAME)
if block_id:
    print(f"deleting block having id {block_id}")
    prefectapi.delete_dbt_core_block(block_id)

if os.path.exists(f"{DBT_PROJECT_DIR}/profiles/profiles.yml"):
    os.remove(f"{DBT_PROJECT_DIR}/profiles/profiles.yml")
