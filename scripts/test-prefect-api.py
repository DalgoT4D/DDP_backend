import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
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
x = prefectapi.get_block_type(prefectapi.AIRBYTESERVER)
print("airbyte server block type = " + x["id"])

x = prefectapi.get_block_schema_type(prefectapi.AIRBYTESERVER)
print("airbyte server block schema type = " + x["id"])

AIRBYTE_BLOCKNAME = "absrvr-unittest"
x = prefectapi.get_block(prefectapi.AIRBYTESERVER, AIRBYTE_BLOCKNAME)
if x:
    print(f"found airbyte server block {AIRBYTE_BLOCKNAME}, deleting")
    prefectapi.delete_airbyte_server_block(x["id"])
else:
    print(f"no airbyte server block named {AIRBYTE_BLOCKNAME}, creating")

prefectapi.create_airbyte_server_block(AIRBYTE_BLOCKNAME)

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, AIRBYTE_BLOCKNAME)
if x:
    print(f"creation success: found airbyte server block {AIRBYTE_BLOCKNAME}")
else:
    raise Exception(f"no airbyte server block named {AIRBYTE_BLOCKNAME}")

# create a connection block referencing this server block
AIRBYTE_CONNECTION_BLOCKNAME = "abconn-unittest"
connection_block_data = prefectschemas.PrefectAirbyteConnectionSetup(
    serverblockname=AIRBYTE_BLOCKNAME,
    connectionblockname=AIRBYTE_CONNECTION_BLOCKNAME,
    connection_id="fake-conn-id",
)
prefectapi.create_airbyte_connection_block(connection_block_data)
x = prefectapi.get_block(prefectapi.AIRBYTECONNECTION, AIRBYTE_CONNECTION_BLOCKNAME)
if x:
    print(
        f"creation success: found airbyte connection block {AIRBYTE_CONNECTION_BLOCKNAME}"
    )
else:
    raise Exception(f"no airbyte connection block named {AIRBYTE_CONNECTION_BLOCKNAME}")

prefectapi.delete_airbyte_connection_block(x["id"])

x = prefectapi.get_block(prefectapi.AIRBYTECONNECTION, AIRBYTE_CONNECTION_BLOCKNAME)
if x:
    raise Exception(
        f"found airbyte connection block {AIRBYTE_CONNECTION_BLOCKNAME} after deletion"
    )
else:
    print(
        f"deletion success: no airbyte connection block named {AIRBYTE_CONNECTION_BLOCKNAME}"
    )

# clean up the server block
x = prefectapi.get_block(prefectapi.AIRBYTESERVER, AIRBYTE_BLOCKNAME)
prefectapi.delete_airbyte_server_block(x["id"])

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, AIRBYTE_BLOCKNAME)
if x:
    raise Exception(f"found airbyte server block {AIRBYTE_BLOCKNAME} after deletion")
else:
    print(f"deletion success: no airbyte server block named {AIRBYTE_BLOCKNAME}")


# == shell ==
x = prefectapi.get_block_type(prefectapi.SHELLOPERATION)
print("shell operation block type = " + x["id"])

x = prefectapi.get_block_schema_type(prefectapi.SHELLOPERATION)
print("shell operation block schema type = " + x["id"])

SHELLOP_BLOCKNAME = "shellop-unittest"
x = prefectapi.get_block(prefectapi.SHELLOPERATION, SHELLOP_BLOCKNAME)
if x:
    print(f"found shell operation block {SHELLOP_BLOCKNAME}, deleting")
    prefectapi.delete_shell_block(x["id"])
else:
    print(f"no shell operation block named {SHELLOP_BLOCKNAME}, creating")

shellop_blockdata = prefectschemas.PrefectShellSetup(
    blockname=SHELLOP_BLOCKNAME,
    working_dir="WORKING_DIR",
    env={"VAR": "VAL"},
    commands=["shellcmd1", "shellcmd2"],
)
prefectapi.create_shell_block(shellop_blockdata)

x = prefectapi.get_block(prefectapi.SHELLOPERATION, SHELLOP_BLOCKNAME)
if x:
    print(f"creation success: found shell operation block {SHELLOP_BLOCKNAME}")
    print("  working_dir= " + x["data"]["working_dir"])
    print("  commands= " + json.dumps(x["data"]["commands"]))
else:
    raise Exception(f"no shell operation block named {SHELLOP_BLOCKNAME}")

prefectapi.delete_shell_block(x["id"])

x = prefectapi.get_block(prefectapi.SHELLOPERATION, SHELLOP_BLOCKNAME)
if x:
    raise Exception(f"found shell operation block {SHELLOP_BLOCKNAME} after deletion")
else:
    print(f"deletion success: no shell operation block named {SHELLOP_BLOCKNAME}")


# == dbt ==
x = prefectapi.get_block_type(prefectapi.DBTCORE)
print("dbt core block type = " + x["id"])

x = prefectapi.get_block_schema_type(prefectapi.DBTCORE)
print("dbt core block schema type = " + x["id"])

DBT_BLOCKNAME = "dbt-unittest"
x = prefectapi.get_block(prefectapi.DBTCORE, DBT_BLOCKNAME)
if x:
    print(f"found dbt core block {DBT_BLOCKNAME}, deleting")
    prefectapi.delete_dbt_core_block(x["id"])
else:
    print(f"no dbt core block named {DBT_BLOCKNAME}, creating")

DBT_BINARY = os.getenv("TESTING_DBT_BINARY")
DBT_PROJECT_DIR = os.getenv("TESTING_DBT_PROJECT_DIR")
DBT_TARGET = os.getenv("TESTING_DBT_TARGET")
DBT_PROFILE = os.getenv("TESTING_DBT_PROFILE")
DBT_TARGETCONFIGS_TYPE = os.getenv("TESTING_DBT_TARGETCONFIGS_TYPE")
DBT_TARGETCONFIGS_SCHEMA = os.getenv("TESTING_DBT_TARGETCONFIGS_SCHEMA")
DBT_CREDENTIALS_USERNAME = os.getenv("TESTING_DBT_CREDENTIALS_USERNAME")
DBT_CREDENTIALS_PASSWORD = os.getenv("TESTING_DBT_CREDENTIALS_PASSWORD")
DBT_CREDENTIALS_DATABASE = os.getenv("TESTING_DBT_CREDENTIALS_DATABASE")
DBT_CREDENTIALS_HOST = os.getenv("TESTING_DBT_CREDENTIALS_HOST")

if os.path.exists(f"{DBT_PROJECT_DIR}/profiles/profiles.yml"):
    os.remove(f"{DBT_PROJECT_DIR}/profiles/profiles.yml")

blockdata = prefectschemas.PrefectDbtCoreSetup(
    blockname=DBT_BLOCKNAME,
    profiles_dir=f"{DBT_PROJECT_DIR}/profiles/",
    project_dir=DBT_PROJECT_DIR,
    working_dir=DBT_PROJECT_DIR,
    env={},
    commands=[f"{DBT_BINARY} run --target {DBT_TARGET}"],
)

dbtprofile = prefectschemas.DbtProfile(
    name=DBT_PROFILE,
    target=DBT_TARGET,
    target_configs_type=DBT_TARGETCONFIGS_TYPE,
    target_configs_schema=DBT_TARGETCONFIGS_SCHEMA,
)
dbtcredentials = prefectschemas.DbtCredentialsPostgres(
    username=DBT_CREDENTIALS_USERNAME,
    password=DBT_CREDENTIALS_PASSWORD,
    database=DBT_CREDENTIALS_DATABASE,
    host=DBT_CREDENTIALS_HOST,
    port=5432,
)

prefectapi.create_dbt_core_block(blockdata, dbtprofile, dbtcredentials)

x = prefectapi.get_block(prefectapi.DBTCORE, DBT_BLOCKNAME)
if x:
    print(f"creation success: found dbt core block {DBT_BLOCKNAME}")
    print("  working_dir= " + x["data"]["working_dir"])
    print("  profiles_dir= " + x["data"]["profiles_dir"])
    print("  project_dir= " + x["data"]["project_dir"])
    print("  commands= " + json.dumps(x["data"]["commands"]))

    print("running block")
    prefectapi.manual_dbt_core_flow(DBT_BLOCKNAME)

else:
    raise Exception(f"no dbt core block named {DBT_BLOCKNAME}")

prefectapi.delete_dbt_core_block(x["id"])

x = prefectapi.get_block(prefectapi.DBTCORE, DBT_BLOCKNAME)
if x:
    raise Exception(f"found dbt core block {DBT_BLOCKNAME} after deletion")
else:
    print(f"deletion success: no dbt core block named {DBT_BLOCKNAME}")

if os.path.exists(f"{DBT_PROJECT_DIR}/profiles/profiles.yml"):
    os.remove(f"{DBT_PROJECT_DIR}/profiles/profiles.yml")
