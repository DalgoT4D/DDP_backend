import os
import requests
from typing import Union

from dotenv import load_dotenv
load_dotenv()

from prefect import flow
from prefect_airbyte import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
from prefect_dbt.cli.commands import DbtCoreOperation

from ddpui.ddpprefect.Schemas import PrefectDbtCoreSetup, PrefectShellSetup, PrefectAirbyteConnectionSetup
from ddpui.ddpprefect.Schemas import DbtProfile, DbtCredentialsPostgres

# =====================================================================================================================
# prefect block names
AIRBYTESERVER = 'Airbyte Server'
AIRBYTECONNECTION = 'Airbyte Connection'
SHELLOPERATION = 'Shell Operation'
DBTCORE = 'dbt Core Operation'

# =====================================================================================================================
def prefectGet(endpoint):
  root = os.getenv('PREFECT_API_URL')
  r = requests.get(f"{root}/{endpoint}")
  r.raise_for_status()
  return r.json()

def prefectPost(endpoint, json):
  root = os.getenv('PREFECT_API_URL')
  r = requests.post(f"{root}/{endpoint}", json=json)
  r.raise_for_status()
  return r.json()

def prefectPatch(endpoint, json):
  root = os.getenv('PREFECT_API_URL')
  r = requests.patch(f"{root}/{endpoint}", json=json)
  r.raise_for_status()
  return r.json()

def prefectDelete(endpoint):
  root = os.getenv('PREFECT_API_URL')
  r = requests.delete(f"{root}/{endpoint}")
  r.raise_for_status()
  return

# =====================================================================================================================
def getBlockType(querystr):
  r = prefectPost('block_types/filter', 
    {"block_types": {"name": {"like_": querystr},}}
  )
  if len(r) != 1:
    raise Exception(f"Expected exactly one prefect block type for query \"{querystr}\", received {len(r)} instead")
  blocktype = r[0]
  return blocktype

def getBlockSchemaType(querystr, blocktypeid=None):
  if blocktypeid is None:
    blocktypeid = getBlockType(querystr)['id']
  r = prefectPost('block_schemas/filter', 
    {"block_schemas": {"operator": "and_", "block_type_id": {"any_": [blocktypeid]}}}
  )
  if len(r) != 1:
    raise Exception(f"Expected exactly one prefect block schema for query \"{blocktypeid}\", received {len(r)} instead")
  blockschematype = r[0]
  return blockschematype

def getBlock(blocktype, blockname):
  blocktype_id = getBlockType(blocktype)['id']
  r = prefectPost('block_documents/filter', 
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
def createAirbyteserverBlock(blockname):
  airbyte_server_blocktype_id = getBlockType(AIRBYTESERVER)['id']
  airbyte_server_blockschematype_id = getBlockSchemaType(AIRBYTESERVER, airbyte_server_blocktype_id)['id']

  r = prefectPost(f'block_documents/', {
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

def deleteAirbyteserverBlock(blockid):
  return prefectDelete(f"block_documents/{blockid}")

# =====================================================================================================================
def createAirbyteConnectionBlock(conninfo: PrefectAirbyteConnectionSetup):

  airbyte_connection_blocktype_id = getBlockType(AIRBYTECONNECTION)['id']
  airbyte_connection_blockschematype_id = getBlockSchemaType(AIRBYTECONNECTION, airbyte_connection_blocktype_id)['id']

  serverblock = getBlock(AIRBYTESERVER, conninfo.serverblockname)
  if serverblock is None:
    raise Exception(f"could not find {AIRBYTESERVER} block called {conninfo.serverblockname}")

  r = prefectPost(f'block_documents/', {
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
  return prefectDelete(f"block_documents/{blockid}")

# =====================================================================================================================
def createShellBlock(shell: PrefectShellSetup):
  shell_blocktype_id = getBlockType(SHELLOPERATION)['id']
  shell_blockschematype_id = getBlockSchemaType(SHELLOPERATION, shell_blocktype_id)['id']

  r = prefectPost(f'block_documents/', {
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
  return prefectDelete(f"block_documents/{blockid}")

# =====================================================================================================================
def createDbtCoreBlock(dbtcore: PrefectDbtCoreSetup, profile: DbtProfile, credentials: Union[DbtCredentialsPostgres, None]):
  dbtcore_blocktype_id = getBlockType(DBTCORE)['id']
  dbtcore_blockschematype_id = getBlockSchemaType(DBTCORE, dbtcore_blocktype_id)['id']

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

  r = prefectPost(f'block_documents/', {
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

def deleteDbtCoreBlock(blockid):
  return prefectDelete(f"block_documents/{blockid}")

# =====================================================================================================================
@flow
def run_airbyte_connection_prefect_flow(blockname):
  airbyte_connection = AirbyteConnection.load(blockname)
  return run_connection_sync(airbyte_connection)

@flow
def run_dbtcore_prefect_flow(blockname):
  dbt_op = DbtCoreOperation.load(blockname)
  dbt_op.run()
