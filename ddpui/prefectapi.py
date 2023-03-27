import os
import requests
from typing import Union

from dotenv import load_dotenv
load_dotenv()

from .prefectschemas import PrefectDbtCoreSetup, PrefectShellSetup, DbtProfile, DbtCredentialsPostgres
from .prefectschemas import PrefectAirbyteConnectionSetup

# =====================================================================================================================
# prefect block names
AIRBYTESERVER = 'Airbyte Server'
AIRBYTECONNECTION = 'Airbyte Connection'
SHELLOPERATION = 'Shell Operation'
DBTCORE = 'dbt Core Operation'

# =====================================================================================================================
def prefectget(endpoint):
  root = os.getenv('PREFECT_API_URL')
  r = requests.get(f"{root}/{endpoint}")
  r.raise_for_status()
  return r.json()

def prefectpost(endpoint, json):
  root = os.getenv('PREFECT_API_URL')
  r = requests.post(f"{root}/{endpoint}", json=json)
  r.raise_for_status()
  return r.json()

def prefectpatch(endpoint, json):
  root = os.getenv('PREFECT_API_URL')
  r = requests.patch(f"{root}/{endpoint}", json=json)
  r.raise_for_status()
  return r.json()

def prefectdelete(endpoint):
  root = os.getenv('PREFECT_API_URL')
  r = requests.delete(f"{root}/{endpoint}")
  r.raise_for_status()
  return

# =====================================================================================================================
def get_blocktype(querystr):
  r = prefectpost('block_types/filter', 
    {"block_types": {"name": {"like_": querystr},}}
  )
  if len(r) != 1:
    raise Exception(f"Expected exactly one prefect block type for query \"{querystr}\", received {len(r)} instead")
  blocktype = r[0]
  return blocktype

def get_blockschematype(querystr, blocktypeid=None):
  if blocktypeid is None:
    blocktypeid = get_blocktype(querystr)['id']
  r = prefectpost('block_schemas/filter', 
    {"block_schemas": {"operator": "and_", "block_type_id": {"any_": [blocktypeid]}}}
  )
  if len(r) != 1:
    raise Exception(f"Expected exactly one prefect block schema for query \"{blocktypeid}\", received {len(r)} instead")
  blockschematype = r[0]
  return blockschematype

def get_block(blocktype, blockname):
  blocktype_id = get_blocktype(blocktype)['id']
  r = prefectpost('block_documents/filter', 
    {"block_documents": {
      "operator": "and_", 
      "block_type_id": {"any_": [blocktype_id]},
      "name": {"any_": [blockname]},
    }}
  )
  if len(r) > 1:
    raise Exception(f"Expected at most one {blocktype} block named {blockname}")
  if len(r) == 0:
    return None
  block = r[0]
  return block

# =====================================================================================================================
def create_airbyteserver_block(blockname):
  airbyte_server_blocktype_id = get_blocktype(AIRBYTESERVER)['id']
  airbyte_server_blockschematype_id = get_blockschematype(AIRBYTESERVER, airbyte_server_blocktype_id)['id']

  r = prefectpost(f'block_documents/', {
    "name": blockname,
    "block_type_id": airbyte_server_blocktype_id,
    "block_schema_id": airbyte_server_blockschematype_id,
    "data": {
      # 'username': ,
      # 'password': ,
      'server_host': os.getenv('AIRBYTE_SERVER_HOST'),
      'server_port': os.getenv('AIRBYTE_SERVER_PORT'),
      'api_version': os.getenv('AIRBYTE_SERVER_APIVER'),
    },
    "is_anonymous": False
  })
  return r

def delete_airbyteserver_block(blockid):
  return prefectdelete(f"block_documents/{blockid}")

# =====================================================================================================================
def create_airbyteconnection_block(conninfo: PrefectAirbyteConnectionSetup):

  airbyte_connection_blocktype_id = get_blocktype(AIRBYTECONNECTION)['id']
  airbyte_connection_blockschematype_id = get_blockschematype(AIRBYTECONNECTION, airbyte_connection_blocktype_id)['id']

  serverblock = get_block(AIRBYTESERVER, conninfo.serverblockname)
  if serverblock is None:
    raise Exception(f"could not find {AIRBYTESERVER} block called {conninfo.serverblockname}")

  r = prefectpost(f'block_documents/', {
    "name": conninfo.connectionblockname,
    "block_type_id": airbyte_connection_blocktype_id,
    "block_schema_id": airbyte_connection_blockschematype_id,
    "data": {
      'airbyte_server': serverblock['id'],
      'connection_id': conninfo.connection_id,
    },
    "is_anonymous": False
  })
  return r


def delete_airbyteconnection_block(blockid):
  return prefectdelete(f"block_documents/{blockid}")

# =====================================================================================================================
def create_shell_block(shell: PrefectShellSetup):
  shell_blocktype_id = get_blocktype(SHELLOPERATION)['id']
  shell_blockschematype_id = get_blockschematype(SHELLOPERATION, shell_blocktype_id)['id']

  r = prefectpost(f'block_documents/', {
    "name": shell.blockname,
    "block_type_id": shell_blocktype_id,
    "block_schema_id": shell_blockschematype_id,
    "data": {
      "working_dir": shell.working_dir,
      "env": shell.env,
      "commands": shell.commands,
    },
    "is_anonymous": False
  })
  return r

def delete_shell_block(blockid):
  return prefectdelete(f"block_documents/{blockid}")

# =====================================================================================================================
def create_dbtcore_block(dbtcore: PrefectDbtCoreSetup, profile: DbtProfile, credentials: Union[DbtCredentialsPostgres, None]):
  dbtcore_blocktype_id = get_blocktype(DBTCORE)['id']
  dbtcore_blockschematype_id = get_blockschematype(DBTCORE, dbtcore_blocktype_id)['id']

  dbt_cli_profile = {
    "name": profile.name, 
    "target": profile.target,
    "target_configs": {
      "type": profile.target_configs_type, 
      "schema": profile.target_configs_schema,
      "credentials": None
    }
  }
  if profile.target_configs_type == 'postgres':
    dbt_cli_profile["target_configs"]["credentials"] = {
      "driver": "postgresql+psycopg2",
      "host": credentials.host,
      "port": credentials.port,
      "username": credentials.username, 
      "password": credentials.password,
      "database": credentials.database, 
    }
  else:
    raise Exception(f"unrecognized target_configs_type {profile.target_configs_type}")

  r = prefectpost(f'block_documents/', {
    "name": dbtcore.blockname,
    "block_type_id": dbtcore_blocktype_id,
    "block_schema_id": dbtcore_blockschematype_id,
    "data": {
      "profiles_dir": dbtcore.profiles_dir,
      "project_dir": dbtcore.project_dir,
      "working_dir": dbtcore.working_dir,
      "env": dbtcore.env,
      "dbt_cli_profile": dbt_cli_profile,
      "commands": dbtcore.commands
    },
    "is_anonymous": False
  })
  return r

def delete_dbtcore_block(blockid):
  return prefectdelete(f"block_documents/{blockid}")

# =====================================================================================================================
from prefect import flow
from prefect_airbyte import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
from prefect_dbt.cli.commands import DbtCoreOperation
@flow
def run_airbyte_connection_prefect_flow(blockname):
  airbyte_connection = AirbyteConnection.load(blockname)
  return run_connection_sync(airbyte_connection)

@flow
def run_dbtcore_prefect_flow(blockname):
  dbt_op = DbtCoreOperation.load(blockname)
  dbt_op.run()
