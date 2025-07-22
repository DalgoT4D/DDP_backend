from typing import Any, Dict, List, Optional
from ninja import Schema
from pydantic import BaseModel, Field
from ddpui.ddpprefect.schema import DeploymentCurrentQueueTime


class AirbyteJob(Schema):
    """Schema for the 'job' object within Airbyte job info."""

    id: int
    status: str
    configType: str
    configId: str
    createdAt: int
    updatedAt: int


class AirbyteAttempt(Schema):
    """Schema for a single attempt within an Airbyte job."""

    id: int  # Unique identifier for the attempt
    status: str  # e.g., "running", "succeeded", "failed"
    createdAt: int  # Unix timestamp of attempt creation
    updatedAt: int  # Unix timestamp of last update
    endedAt: Optional[int] = None  # Unix timestamp when attempt ended (null if still running)
    bytesSync: Optional[int] = None  # Number of bytes synced
    recordsSync: Optional[int] = None  # Number of records synced
    # Logs are often structured as a dictionary with a 'logLines' key
    logs: Optional[Dict[str, List[str]]] = None  # Example: {'logLines': ['line1', 'line2']}
    # Add other fields if present (e.g., 'failureSummary' which is another nested object)


class AirbyteJobInfo(Schema):
    """Schema for a full Airbyte Job Information object."""

    job: AirbyteJob  # The main job object
    attempts: List[AirbyteAttempt]  # A list of all attempts for this job
    # Add any other top-level fields that might be present in the job info response:
    # connectionId: Optional[str] = None # Often derived from job.configId but sometimes top-level


class AirbyteStreamJsonSchema(Schema):
    """Basic schema for the JSON schema part of an Airbyte stream."""

    type: str
    properties: Dict[str, Any]


class AirbyteStream(Schema):
    """Schema for a single Airbyte stream definition within a catalog."""

    name: str
    jsonSchema: AirbyteStreamJsonSchema
    supportedSyncModes: List[str]
    sourceDefinedCursor: bool
    defaultCursorField: Optional[List[str]] = None  # Often a list of field names
    sourceDefinedPrimaryKey: Optional[List[List[str]]] = None  # A list of lists for composite keys
    namespace: Optional[str] = None


class AirbyteStreamConfig(Schema):
    """Schema for the configuration of a selected stream within a connection."""

    syncMode: str  # e.g., "full_refresh", "incremental"
    cursorField: Optional[List[str]] = None
    primaryKey: Optional[List[List[str]]] = None
    aliasName: Optional[str] = None
    selected: bool


class AirbyteSelectedStream(Schema):
    """Represents a stream that is selected and configured within a connection's sync catalog."""

    stream: AirbyteStream
    config: AirbyteStreamConfig


class AirbyteSyncCatalog(Schema):
    """Schema for the sync catalog part of an Airbyte connection, listing selected streams."""

    streams: List[AirbyteSelectedStream]


class AirbyteSchedule(Schema):
    """Schema for a connection's sync schedule."""

    # This structure can vary based on `scheduleType`.
    # Common fields for 'basic' schedules:
    units: Optional[int] = None  # e.g., 24 for daily
    timeUnit: Optional[str] = None  # e.g., "hours", "days", "weeks"
    cronExpression: Optional[str] = None


class AirbyteConnectionSimpleResponse(Schema):
    """Schema for a full Airbyte connection object."""

    connectionId: str
    sourceId: str
    destinationId: str
    name: str
    status: str
    syncCatalog: AirbyteSyncCatalog
    schedule: Optional[AirbyteSchedule] = None
    scheduleType: Optional[str] = None
    sourceDefinitionId: str
    destinationDefinitionId: str


class AirbyteJobSimpleStatus(Schema):
    """Schema for a simple job status response, without logs."""

    status: str


class AirbyteDestination(Schema):
    """Schema for a full Airbyte destination object."""

    destinationId: str
    destinationDefinitionId: str
    workspaceId: str
    name: str
    destinationName: str
    connectionConfiguration: Dict[str, Any]
    icon: Optional[str] = None


class AirbyteDestinationIdResponse(Schema):
    """Response schema for a newly created or updated destination ID."""

    destinationId: str


class AirbyteDestinationDefinition(Schema):
    """Schema for a single Airbyte Destination Definition."""

    destinationDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    icon: Optional[str] = None


class AirbyteCatalog(Schema):
    """Schema for a full Airbyte source schema catalog."""

    streams: List[Dict[str, Any]]


class AirbyteSource(Schema):
    """Schema for a full Airbyte source object."""

    sourceId: str
    sourceDefinitionId: str  # Renamed from sourceDefId to match common Airbyte API naming
    workspaceId: str
    name: str
    sourceName: str
    configuration: Dict[str, Any]  # 'config' in Airbyte API is 'configuration' in response
    icon: Optional[str] = None


class AirbyteConnectionSchemaUpdate(Schema):
    """Schema for individual schema changes within a connection."""

    connectorId: str
    streamName: str
    newFields: List[str]
    removedFields: List[str]
    changedFields: List[str]


class AirbyteCheckConnectionResponse(Schema):
    """Response schema for connection checks (source or destination)"""

    status: str
    logs: List[str]


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


class AirbyteResponse(BaseModel):
    connectionId: str
    name: str
    status: str
    lastSyncTime: Optional[str] = None


class AirbyteConnectionsListResponse(BaseModel):
    connections: List[AirbyteResponse]
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


class AirbyteConnectionOneSchemaUpdate(Schema):
    """Docstring"""

    syncCatalog: dict
    connectionId: str
    sourceCatalogId: str


class AirbyteConnectionSchemaUpdateSchedule(Schema):
    """Docstring"""

    catalogDiff: dict
    cron: str = None
