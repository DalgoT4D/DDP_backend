from typing import Optional
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


class SourceOAuthConsentCreate(Schema):
    """Request to start the Google OAuth consent flow for a source"""

    sourceDefId: str


class SourceOAuthComplete(Schema):
    """Complete the OAuth flow and create (or update) the source in one step.

    The user fills in `name` + `config` (e.g. spreadsheet_id) before authenticating; on
    consent we complete the token exchange and save the source server-side, so the OAuth
    credentials (client_secret, refresh_token) never travel through the browser. `config`
    must NOT include a `credentials` block — the backend fills it in. Pass `sourceId` to
    re-authenticate an existing source (update); omit it to create a new one.
    """

    sourceDefId: str
    name: str
    config: dict
    state: str
    queryParams: dict
    sourceId: Optional[str] = None


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
