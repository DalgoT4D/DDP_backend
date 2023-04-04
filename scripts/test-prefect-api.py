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
x = prefectapi.get_blocktype(prefectapi.AIRBYTESERVER)
print("airbyte server block type = " + x["id"])

x = prefectapi.get_blockschematype(prefectapi.AIRBYTESERVER)
print("airbyte server block schema type = " + x["id"])

airbyte_blockname = "absrvr-unittest"
x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
    print(f"found airbyte server block {airbyte_blockname}, deleting")
    prefectapi.delete_airbyteserver_block(x["id"])
else:
    print(f"no airbyte server block named {airbyte_blockname}, creating")

prefectapi.create_airbyteserver_block(airbyte_blockname)

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
    print(f"creation success: found airbyte server block {airbyte_blockname}")
else:
    raise Exception(f"no airbyte server block named {airbyte_blockname}")

# create a connection block referencing this server block
airbyte_connection_blockname = "abconn-unittest"
connection_block_data = prefectschemas.PrefectAirbyteConnectionSetup(
    serverblockname=airbyte_blockname,
    connectionblockname=airbyte_connection_blockname,
    connection_id="fake-conn-id",
)
prefectapi.create_airbyteconnection_block(connection_block_data)
x = prefectapi.get_block(prefectapi.AIRBYTECONNECTION, airbyte_connection_blockname)
if x:
    print(
        f"creation success: found airbyte connection block {airbyte_connection_blockname}"
    )
else:
    raise Exception(f"no airbyte connection block named {airbyte_connection_blockname}")

prefectapi.delete_airbyteconnection_block(x["id"])

x = prefectapi.get_block(prefectapi.AIRBYTECONNECTION, airbyte_connection_blockname)
if x:
    raise Exception(
        f"found airbyte connection block {airbyte_connection_blockname} after deletion"
    )
else:
    print(
        f"deletion success: no airbyte connection block named {airbyte_connection_blockname}"
    )

# clean up the server block
x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
prefectapi.delete_airbyteserver_block(x["id"])

x = prefectapi.get_block(prefectapi.AIRBYTESERVER, airbyte_blockname)
if x:
    raise Exception(f"found airbyte server block {airbyte_blockname} after deletion")
else:
    print(f"deletion success: no airbyte server block named {airbyte_blockname}")


# == shell ==
x = prefectapi.get_blocktype(prefectapi.SHELLOPERATION)
print("shell operation block type = " + x["id"])

x = prefectapi.get_blockschematype(prefectapi.SHELLOPERATION)
print("shell operation block schema type = " + x["id"])

shellop_blockname = "shellop-unittest"
x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
    print(f"found shell operation block {shellop_blockname}, deleting")
    prefectapi.delete_shell_block(x["id"])
else:
    print(f"no shell operation block named {shellop_blockname}, creating")

shellop_blockdata = prefectschemas.PrefectShellSetup(
    blockname=shellop_blockname,
    working_dir="WORKING_DIR",
    env={"VAR": "VAL"},
    commands=["shellcmd1", "shellcmd2"],
)
prefectapi.create_shell_block(shellop_blockdata)

x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
    print(f"creation success: found shell operation block {shellop_blockname}")
    print("  working_dir= " + x["data"]["working_dir"])
    print("  commands= " + json.dumps(x["data"]["commands"]))
else:
    raise Exception(f"no shell operation block named {shellop_blockname}")

prefectapi.delete_shell_block(x["id"])

x = prefectapi.get_block(prefectapi.SHELLOPERATION, shellop_blockname)
if x:
    raise Exception(f"found shell operation block {shellop_blockname} after deletion")
else:
    print(f"deletion success: no shell operation block named {shellop_blockname}")


# == dbt ==
x = prefectapi.get_blocktype(prefectapi.DBTCORE)
print("dbt core block type = " + x["id"])

x = prefectapi.get_blockschematype(prefectapi.DBTCORE)
print("dbt core block schema type = " + x["id"])

dbt_blockname = "dbt-unittest"
x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
    print(f"found dbt core block {dbt_blockname}, deleting")
    prefectapi.delete_dbtcore_block(x["id"])
else:
    print(f"no dbt core block named {dbt_blockname}, creating")

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
    blockname=dbt_blockname,
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

prefectapi.create_dbtcore_block(blockdata, dbtprofile, dbtcredentials)

x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
    print(f"creation success: found dbt core block {dbt_blockname}")
    print("  working_dir= " + x["data"]["working_dir"])
    print("  profiles_dir= " + x["data"]["profiles_dir"])
    print("  project_dir= " + x["data"]["project_dir"])
    print("  commands= " + json.dumps(x["data"]["commands"]))

    print("running block")
    prefectapi.run_dbtcore_prefect_flow(dbt_blockname)

else:
    raise Exception(f"no dbt core block named {dbt_blockname}")

prefectapi.delete_dbtcore_block(x["id"])

x = prefectapi.get_block(prefectapi.DBTCORE, dbt_blockname)
if x:
    raise Exception(f"found dbt core block {dbt_blockname} after deletion")
else:
    print(f"deletion success: no dbt core block named {dbt_blockname}")

if os.path.exists(f"{DBT_PROJECT_DIR}/profiles/profiles.yml"):
    os.remove(f"{DBT_PROJECT_DIR}/profiles/profiles.yml")
