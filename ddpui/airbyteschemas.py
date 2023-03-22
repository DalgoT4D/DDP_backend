from ninja import ModelSchema, Schema

# request schemas
class AirbyteWorkspaceCreate(Schema):
  name: str

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

# response schemas
class AirbyteWorkspace(Schema):
  name: str
  workspaceId: str
  initialSetupComplete: bool

