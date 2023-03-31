import os
import requests

from dotenv import load_dotenv
load_dotenv()

from ddpui.ddpairbyte import schemas

# ====================================================================================================
def abreq(endpoint, req=None):
  abhost = os.getenv('AIRBYTE_SERVER_HOST')
  abport = os.getenv('AIRBYTE_SERVER_PORT')
  abver  = os.getenv('AIRBYTE_SERVER_APIVER')
  token  = os.getenv('AIRBYTE_API_TOKEN')

  r = requests.post(
    f"http://{abhost}:{abport}/api/{abver}/{endpoint}",
    headers={'Authorization': f"Basic {token}"},
    json=req
  )
  return r.json()

# ====================================================================================================
def get_workspaces():
  return abreq('workspaces/list')

# ====================================================================================================
def get_workspace(workspace_id):
  return abreq('workspaces/get', {"workspaceId": workspace_id})

# ====================================================================================================
def setworkspacename(workspace_id, name):
  abreq('workspaces/update_name', {'workspaceId': workspace_id, 'name': name})

# ====================================================================================================
def createworkspace(name):
  r = abreq('workspaces/create', {"name": name})
  if 'workspaceId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def getsourcedefinitions(workspace_id, **kwargs):
  r = abreq(
    'source_definitions/list_for_workspace', 
    {'workspaceId': workspace_id}
  )
  if 'sourceDefinitions' not in r:
    raise Exception(r)
  return r['sourceDefinitions']

# ====================================================================================================
def getsourcedefinitionspecification(workspace_id, sourcedef_id):
  r = abreq('source_definition_specifications/get', {
    'sourceDefinitionId': sourcedef_id, 
    'workspaceId': workspace_id
  })
  if 'connectionSpecification' not in r:
    raise Exception(r)
  return r['connectionSpecification']

# ====================================================================================================
def getsources(workspace_id):
  r = abreq('sources/list', {"workspaceId": workspace_id})
  if 'sources' not in r:
    raise Exception(r)
  return r['sources']

# ====================================================================================================
def getsource(workspace_id, source_id):
  r = abreq('sources/get', {"sourceId": source_id})
  if 'sourceId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def createsource(workspace_id, name, sourcedef_id, config):
  r = abreq('sources/create', {
    "workspaceId": workspace_id,
    "name": name,
    "sourceDefinitionId": sourcedef_id,
    "connectionConfiguration": config,
  })
  if 'sourceId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def checksourceconnection(workspace_id, source_id):
  r = abreq('sources/check_connection', {'sourceId': source_id})
  # {
  #   'status': 'succeeded',
  #   'jobInfo': {
  #     'id': 'ecd78210-5eaa-4a70-89ad-af1d9bc7c7f2',
  #     'configType': 'check_connection_source',
  #     'configId': 'Optional[decd338e-5647-4c0b-adf4-da0e75f5a750]',
  #     'createdAt': 1678891375849,
  #     'endedAt': 1678891403356,
  #     'succeeded': True,
  #     'connectorConfigurationUpdated': False,
  #     'logs': {'logLines': [str]}
  #   }
  # }
  return r

# ====================================================================================================
def getsourceschemacatalog(workspace_id, source_id):
  r = abreq('sources/discover_schema', {'sourceId': source_id})
  if 'catalog' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def getdestinationdefinitions(workspace_id, **kwargs):
  r = abreq(
    'destination_definitions/list_for_workspace', 
    {'workspaceId': workspace_id}
  )
  if 'destinationDefinitions' not in r:
    raise Exception(r)
  return r['destinationDefinitions']

# ====================================================================================================
def getdestinationdefinitionspecification(workspace_id, destinationdef_id):
  r = abreq('destination_definition_specifications/get', {
    'destinationDefinitionId': destinationdef_id, 
    'workspaceId': workspace_id
  })
  if 'connectionSpecification' not in r:
    raise Exception(r)
  return r['connectionSpecification']

# ====================================================================================================
def getdestinations(workspace_id):
  r = abreq('destinations/list', {"workspaceId": workspace_id})
  if 'destinations' not in r:
    raise Exception(r)
  return r['destinations']

# ====================================================================================================
def getdestination(workspace_id, destination_id):
  r = abreq('destinations/get', {"destinationId": destination_id})
  if 'destinationId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def createdestination(workspace_id, name, destinationdef_id, config):
  r = abreq('destinations/create', {
    "workspaceId": workspace_id,
    "name": name,
    "destinationDefinitionId": destinationdef_id,
    "connectionConfiguration": config,
  })
  if 'destinationId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def checkdestinationconnection(workspace_id, destination_id):
  r = abreq('destinations/check_connection', {'destinationId': destination_id})
  return r

# ====================================================================================================
def getconnections(workspace_id):
  r = abreq('connections/list', {"workspaceId": workspace_id})
  if 'connections' not in r:
    raise Exception(r)
  return r['connections']

# ====================================================================================================
def getconnection(workspace_id, connection_id):
  r = abreq('connections/get', {"connectionId": connection_id})
  if 'connectionId' not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def createconnection(workspace_id, connection_info: schemas.AirbyteConnectionCreate):

  if len(connection_info.streamnames) == 0:
    raise Exception("must specify stream names")

  sourceschemacatalog = getsourceschemacatalog(workspace_id, connection_info.source_id)

  payload = {
    "sourceId": connection_info.source_id, 
    "destinationId": connection_info.destination_id,
    "sourceCatalogId": sourceschemacatalog['catalogId'],
    "syncCatalog": {
      "streams": [
        # <== we're going to put the stream configs in here in the next step below
      ]
    },
    "status": "active",
    "prefix": "",
    "namespaceDefinition": "destination",
    "namespaceFormat": "${SOURCE_NAMESPACE}",
    "nonBreakingChangesPreference": "ignore",
    "scheduleType": "manual",
    "geography": "auto",
    "name": connection_info.name,
    "operations": [
      {
        "name": "Normalization",
        "workspaceId": workspace_id,
        "operatorConfiguration": {
          "operatorType": "normalization",
          "normalization": {
            "option": "basic"
          }
        }
      }
    ],
  }

  # one stream per table
  for ss in sourceschemacatalog['catalog']['streams']:
    if ss['stream']['name'] in connection_info.streamnames:
      # set ss['config']['syncMode'] from ss['stream']['supportedSyncModes'] here
      payload['syncCatalog']['streams'].append(ss)

  r = abreq("connections/create", payload)
  if "connectionId" not in r:
    raise Exception(r)
  return r

# ====================================================================================================
def syncconnection(workspace_id, connection_id):
  r = abreq('connections/sync', {'connectionId': connection_id})
  return r
