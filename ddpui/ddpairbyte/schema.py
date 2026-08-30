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


class SourceGoogleOAuthConsentCreate(Schema):
    """Request to start the Google OAuth consent flow for a source. `sourceName` is the
    source-definition name (e.g. "Google Sheets") the frontend already has from the same
    workspace catalog it got `sourceDefId` from — it is the OAuth registry key."""

    sourceDefId: str
    sourceName: str


class SourceGoogleOAuthPickerConfigFetch(Schema):
    """Ask for the Google Picker config behind a `refresh_token_ref` the caller owns.
    `sourceName` is checked against the ref, as create/update do."""

    sourceName: str
    refresh_token_ref: str


class SourceGoogleOAuthCreate(Schema):
    """Create a NEW source from a redeemed Google OAuth `refresh_token_ref`.

    The user fills in `name` + `config` (e.g. spreadsheet_id) and authenticates via Google;
    the backend has already exchanged the code and stashed the refresh_token server-side
    under the opaque `refresh_token_ref`. Here the backend redeems `refresh_token_ref`, builds
    the `credentials` block (from env + refresh_token), and saves the source — so the
    refresh_token never travels through the browser. `config` must NOT include a `credentials`
    block — the backend fills it in. To re-authenticate an EXISTING source, use the update
    endpoint (PUT /sources/oauth/{source_id}) instead."""

    sourceDefId: str
    sourceName: str
    name: str
    config: dict
    refresh_token_ref: str


class SourceGoogleOAuthUpdate(Schema):
    """Re-authenticate an EXISTING source from a redeemed Google OAuth `refresh_token_ref`.

    Same shape as create, minus the source id — that comes from the URL path. The backend
    redeems `refresh_token_ref`, rebuilds the `credentials` block, and updates the source in
    the caller's own workspace. `config` must NOT include a `credentials` block."""

    sourceDefId: str
    sourceName: str
    name: str
    config: dict
    refresh_token_ref: str


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
    post_sync_transform: Optional[dict] = None


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
    post_sync_transform: Optional[dict] = None


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
    post_sync_transform: Optional[dict] = None


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
