from typing import Optional
from ninja import Schema
from ddpui.ddpprefect.schema import DeploymentCurrentQueueTime
from typing import Dict, Any
from pydantic import Field
from typing import List, Optional, Dict, Any


# In ddpui/ddpairbyte/schema.py
# (Place this along with other response schemas)

# In ddpui/ddpairbyte/schema.py
# (Place this along with other response schemas)
# In ddpui/ddpairbyte/schema.py
# (Ensure 'from typing import Optional' and 'from ninja import Schema' are at the top)


class AirbyteTaskResponse(Schema):
    task_id: str
    message: Optional[str] = None


class AirbyteJobAttemptLog(Schema):
    logLines: List[str]


class AirbyteJobStatusResponse(Schema):
    status: str


class AirbyteSuccessResponse(Schema):
    success: int


class AirbyteSourceDefinition(Schema):
    sourceDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    icon: Optional[str] = None


class AirbyteConnectionSpecification(Schema):
    dollar_schema: str = Field(alias="$schema")
    title: str
    type: str
    required: List[str]
    properties: Dict[str, Any]


class AirbyteConnectionCreatePayload(BaseModel):
    sourceId: str
    destinationId: str
    name: str
    status: str


class AirbyteConnectionResponse(BaseModel):
    connectionId: str
    name: str
    status: str
    lastSyncTime: Optional[str] = None


class AirbyteConnectionsListResponse(BaseModel):
    connections: List[AirbyteConnectionResponse]
    totalCount: int


# ...
# request schemas
class AirbyteWorkspaceCreate(Schema):
    """Docstring"""

    name: str


class AirbyteSourceCreate(Schema):
    """Docstring"""

    name: str
    sourceDefId: str
    config: dict


class AirbyteSourceUpdate(Schema):
    """Docstring"""

    name: str
    config: dict
    sourceDefId: str


class AirbyteSourceUpdateCheckConnection(Schema):
    """Docstring"""

    name: str
    config: dict


class AirbyteDestinationCreate(Schema):
    """Docstring"""

    name: str
    destinationDefId: str
    config: dict


class AirbyteDestinationUpdate(Schema):
    """Schema for updating an Airbyte destination"""

    name: str
    destinationDefId: str
    config: dict


class AirbyteDestinationUpdateCheckConnection(Schema):
    """Schema for updating an Airbyte destination"""

    name: str
    config: dict


class AirbyteConnectionCreate(Schema):
    """Docstring"""

    name: str
    sourceId: str
    streams: list
    catalogId: str
    syncCatalog: dict
    destinationId: str = None
    destinationSchema: str = None


# In ddpui/ddpairbyte/schema.py
# ... (existing schemas)


class AirbyteSourceCreateResponse(Schema):  # <-- Using a clear, specific name
    """Schema for the response when creating an Airbyte source."""

    sourceId: str


class AirbyteConnectionCreateResponse(Schema):
    """Docstring"""

    name: str
    connectionId: str
    source: dict
    destination: dict
    catalogId: str
    syncCatalog: dict
    status: str
    deploymentId: str = None
    lastRun: Optional[dict | None]
    destinationSchema: str = ""
    lock: Optional[dict | None]
    isRunning: bool = False
    resetConnDeploymentId: str = None
    sourceId: str


class AirbyteGetConnectionsResponse(Schema):
    """Docstring"""

    name: str
    connectionId: str
    source: dict
    destination: dict
    status: str
    deploymentId: str = None
    lastRun: Optional[dict | None]
    destinationSchema: str = ""
    lock: Optional[dict | None]
    isRunning: bool = False
    resetConnDeploymentId: str = None
    clearConnDeploymentId: str = None
    queuedFlowRunWaitTime: DeploymentCurrentQueueTime = None


class AirbyteConnectionUpdate(Schema):
    """Docstring"""

    name: str
    streams: list
    syncCatalog: dict
    catalogId: str
    destinationId: str = None
    destinationSchema: str = None


# response schemas
class AirbyteWorkspace(Schema):
    """Docstring"""

    name: str
    workspaceId: str
    initialSetupComplete: bool


class AirbyteConnectionSchemaUpdate(Schema):
    """Docstring"""

    syncCatalog: dict
    connectionId: str
    sourceCatalogId: str


class AirbyteConnectionSchemaUpdateSchedule(Schema):
    """Docstring"""

    catalogDiff: dict
    cron: str = None
