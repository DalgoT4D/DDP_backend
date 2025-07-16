from typing import Optional
from ninja import Schema
from ddpui.ddpprefect.schema import DeploymentCurrentQueueTime


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
