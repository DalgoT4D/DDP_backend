from typing import Any, List, Optional
from datetime import datetime
from ninja import Schema
from ddpui.ddpprefect.schema import DeploymentCurrentQueueTime


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
    destinationId: Optional[str] = None
    destinationSchema: Optional[str] = None


class AirbyteConnectionCreateResponse(Schema):
    """Docstring"""

    name: str
    connectionId: str
    source: dict
    destination: dict
    catalogId: str
    syncCatalog: dict
    status: str
    deploymentId: Optional[str] = None
    lastRun: Optional[dict] = None
    destinationSchema: str = ""
    lock: Optional[dict] = None
    isRunning: bool = False
    resetConnDeploymentId: Optional[str] = None


class AirbyteGetConnectionsResponse(Schema):
    """Docstring"""

    name: str
    connectionId: str
    source: dict
    destination: dict
    status: str
    deploymentId: Optional[str] = None
    lastRun: Optional[dict] = None
    destinationSchema: str = ""
    lock: Optional[dict] = None
    isRunning: bool = False
    resetConnDeploymentId: Optional[str] = None
    clearConnDeploymentId: Optional[str] = None
    queuedFlowRunWaitTime: Optional[DeploymentCurrentQueueTime] = None


class AirbyteConnectionUpdate(Schema):
    """Docstring"""

    name: str
    streams: list
    syncCatalog: dict
    catalogId: str
    destinationId: Optional[str] = None
    destinationSchema: Optional[str] = None


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
    cron: Optional[str] = None


# ============================================================
# New response schemas
# ============================================================


class AirbyteSourceDefinition(Schema):
    """Schema for an Airbyte source definition"""

    sourceDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: Optional[str] = None
    releaseStage: Optional[str] = None


class AirbyteSourceRead(Schema):
    """Schema for a single Airbyte source"""

    sourceId: str
    name: str
    sourceName: str
    sourceDefinitionId: str
    workspaceId: str
    connectionConfiguration: dict


class AirbyteDestinationDefinition(Schema):
    """Schema for an Airbyte destination definition"""

    destinationDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: Optional[str] = None
    releaseStage: Optional[str] = None


class AirbyteDestinationRead(Schema):
    """Schema for a single Airbyte destination"""

    destinationId: str
    name: str
    destinationName: str
    destinationDefinitionId: str
    workspaceId: str
    connectionConfiguration: dict


class AirbyteSourceIdResponse(Schema):
    """Schema for source create/update response"""

    sourceId: str


class AirbyteDestinationIdResponse(Schema):
    """Schema for destination create/update response"""

    destinationId: str


class AirbyteCheckConnectionResponse(Schema):
    """Schema for connection check response"""

    status: str
    logs: List[str]


class AirbyteJobStatusResponse(Schema):
    """Schema for job status response with logs"""

    status: str
    logs: List[str]


class AirbyteJobStatusWithoutLogsResponse(Schema):
    """Schema for job status response without logs"""

    status: str


class AirbyteSuccessResponse(Schema):
    """Schema for simple success responses"""

    success: int


class AirbyteTaskResponse(Schema):
    """Schema for async task responses"""

    task_id: str
    message: Optional[str] = None


class AirbyteSyncHistoryItemResponse(Schema):
    """Schema for a single sync history item"""

    job_id: int
    status: str
    job_type: str
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    records_emitted: int = 0
    bytes_emitted: Any = 0
    records_committed: int = 0
    bytes_committed: Any = 0
    stream_stats: Optional[Any] = None
    reset_config: Optional[Any] = None
    duration_seconds: Optional[float] = None
    last_attempt_no: Optional[int] = None


class AirbyteSyncHistoryResponse(Schema):
    """Schema for paginated sync history"""

    history: List[AirbyteSyncHistoryItemResponse]
    totalSyncs: int


class AirbyteSchemaChangeResponse(Schema):
    """Schema for a schema change entry"""

    connection_id: str
    change_type: str
    schedule_job: Optional[Any] = None


class AirbyteCancelJobResponse(Schema):
    """Schema for cancel job response"""

    cancelled: bool
