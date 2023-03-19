import os
import requests

from dotenv import load_dotenv
load_dotenv()

# ====================================================================================================
from django.db import models
from ninja import ModelSchema, Schema

# ====================================================================================================
class AirbyteCreate(Schema):
  name: str

class AirbyteWorkspace(Schema):
  name: str
  workspaceId: str
  initialSetupComplete: bool

class AirbyteSourceCreate(Schema):
  name: str
  sourcedef_id: str
  config: dict

class AirbyteDestinationCreate(Schema):
  name: str
  destinationdef_id: str
  config: dict

class AirbyteConnectionCreate(Schema):
  name: str
  source_id: str
  destination_id: str
  streamnames: list

# ====================================================================================================
def abreq(endpoint, req=None):
  root = os.getenv('AIRBYTE_API_URL')
  r = requests.post(
    f"{root}/{endpoint}",
    headers={'Authorization': "Basic YWlyYnl0ZTpwYXNzd29yZA=="},
    json=req
  )
  return r.json()

# ====================================================================================================
def get_workspaces():
  return abreq('workspaces/list')

# ====================================================================================================
class ClientAirbyte:

  def __init__(self, workspace_id=None):
    self.workspace_id = workspace_id
    self.workspace = None

    self.sourcedefinitions = None
    self.sourceconnectionspecifications = {}
    self.sources = {}

    self.destinationdefinitions = None
    self.destinationconnectionspecifications = {}
    self.destinations = {}

    self.connections = None

  def fetchworkspace(self):
    assert(self.workspace_id)
    self.workspace = abreq('workspaces/get', {'workspaceId': self.workspace_id})

  def setworkspacename(self, name):
    assert(self.workspace_id)
    abreq('workspaces/update_name', {'workspaceId': self.workspace_id, 'name': name})

  def createworkspace(self, name):
    assert(self.workspace_id is None)
    r = abreq('workspaces/create', {"name": name})
    if 'workspaceId' not in r:
      raise Exception(r)
    self.workspace_id = r['workspaceId']
    self.workspace = r

  def getsourcedefinitions(self, **kwargs):
    assert(self.workspace_id)
    if self.sourcedefinitions and not kwargs.get('force'):
      return self.sourcedefinitions
    r = abreq(
      'source_definitions/list_for_workspace', 
      {'workspaceId': self.workspace_id}
    )
    if 'sourceDefinitions' not in r:
      raise Exception(r)
    self.sourcedefinitions = r['sourceDefinitions']
    return self.sourcedefinitions

  def getsourcedefinitionspecification(self, sourcedef_id):
    if sourcedef_id in self.sourceconnectionspecifications:
      return self.sourceconnectionspecifications[sourcedef_id]
    r = abreq('source_definition_specifications/get', {
      'sourceDefinitionId': sourcedef_id, 
      'workspaceId': self.workspace_id
    })
    if 'connectionSpecification' not in r:
      raise Exception(r)
    self.sourceconnectionspecifications[sourcedef_id] = r['connectionSpecification']
    return self.sourceconnectionspecifications[sourcedef_id]
  
  def getsources(self):
    r = abreq('sources/list', {"workspaceId": self.workspace_id})
    if 'sources' not in r:
      raise Exception(r)
    return r['sources']

  def getsource(self, source_id):
    r = abreq('sources/get', {"sourceId": source_id})
    if 'sourceId' not in r:
      raise Exception(r)
    return r

  def createsource(self, name, sourcedef_id, config):
    r = abreq('sources/create', {
      "workspaceId": self.workspace_id,
      "name": name,
      "sourceDefinitionId": sourcedef_id,
      "connectionConfiguration": config,
    })
    if 'sourceId' not in r:
      raise Exception(r)
    self.sources[r['sourceId']] = r
    return r['sourceId']

  def checksourceconnection(self, source_id):
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

  def getsourceschemacatalog(self, source_id):
    r = abreq('sources/discover_schema', {'sourceId': source_id})
    if 'catalog' not in r:
      raise Exception(r)
    return r

  def getdestinationdefinitions(self, **kwargs):
    assert(self.workspace_id)
    if self.destinationdefinitions and not kwargs.get('force'):
      return self.destinationdefinitions
    r = abreq(
      'destination_definitions/list_for_workspace', 
      {'workspaceId': self.workspace_id}
    )
    if 'destinationDefinitions' not in r:
      raise Exception(r)
    self.destinationdefinitions = r['destinationDefinitions']
    return self.destinationdefinitions

  def getdestinationdefinitionspecification(self, destinationdef_id):
    if destinationdef_id in self.destinationconnectionspecifications:
      return self.destinationconnectionspecifications[destinationdef_id]
    r = abreq('destination_definition_specifications/get', {
      'destinationDefinitionId': destinationdef_id, 
      'workspaceId': self.workspace_id
    })
    if 'connectionSpecification' not in r:
      raise Exception(r)
    self.destinationconnectionspecifications[destinationdef_id] = r['connectionSpecification']
    return self.destinationconnectionspecifications[destinationdef_id]
  
  def getdestinations(self):
    r = abreq('destinations/list', {"workspaceId": self.workspace_id})
    if 'destinations' not in r:
      raise Exception(r)
    return r['destinations']

  def getdestination(self, destination_id):
    r = abreq('destinations/get', {"destinationId": destination_id})
    if 'destinationId' not in r:
      raise Exception(r)
    return r

  def createdestination(self, name, destinationdef_id, config):
    r = abreq('destinations/create', {
      "workspaceId": self.workspace_id,
      "name": name,
      "destinationDefinitionId": destinationdef_id,
      "connectionConfiguration": config,
    })
    if 'destinationId' not in r:
      raise Exception(r)
    self.destinations[r['destinationId']] = r
    return r['destinationId']

  def checkdestinationconnection(self, destination_id):
    r = abreq('destinations/check_connection', {'destinationId': destination_id})
    return r
  
  def getconnections(self):
    r = abreq('connections/list', {"workspaceId": self.workspace_id})
    if 'connections' not in r:
      raise Exception(r)
    self.connections = r['connections']
    return r['connections']
  
  def getconnection(self, connection_id):
    r = abreq('connections/get', {"connectionId": connection_id})
    if 'connectionId' not in r:
      raise Exception(r)
    return r
  
  def createconnection(self, connection_info: AirbyteConnectionCreate):

    if len(connection_info.streamnames) == 0:
      raise Exception("must specify stream names")

    sourceschemacatalog = self.getsourceschemacatalog(connection_info.source_id)

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
          "workspaceId": self.workspace_id,
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

