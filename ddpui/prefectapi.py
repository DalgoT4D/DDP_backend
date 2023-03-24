import os
import requests

from dotenv import load_dotenv
load_dotenv()

from .prefectschemas import PrefectDbtCoreSetup, PrefectShellSetup

# =====================================================================================================================
# prefect block names
AIRBYTESERVER = 'airbyte server'
SHELLOPERATION = 'shell operation'
DBTCORE = 'dbt core'

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
def create_dbtcore_block(dbtcore: PrefectDbtCoreSetup):
  dbtcore_blocktype_id = get_blocktype(DBTCORE)['id']
  dbtcore_blockschematype_id = get_blockschematype(DBTCORE, dbtcore_blocktype_id)['id']

  r = prefectpost(f'block_documents/', {
    "name": dbtcore.blockname,
    "block_type_id": dbtcore_blocktype_id,
    "block_schema_id": dbtcore_blockschematype_id,
    "data": {
      "profiles_dir": dbtcore.profiles_dir,
      "project_dir": dbtcore.project_dir,
      "working_dir": dbtcore.working_dir,
      "env": dbtcore.env,
      "commands": dbtcore.commands
    },
    "is_anonymous": False
  })
  return r

def delete_dbtcore_block(blockid):
  return prefectdelete(f"block_documents/{blockid}")

