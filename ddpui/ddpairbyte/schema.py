from typing import Optional
from ninja import Schema


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
    destinationId: Optional[str] = None
    destinationSchema: Optional[str] = None
    streams: list


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
    isRunning: Optional[bool] = False
    resetConnDeploymentId: Optional[str] = None


class AirbyteGetConnectionsResponse(Schema):
    """Docstring"""

    name: str
    connectionId: str
    source: dict
    destination: dict
    status: str
    deploymentId: Optional[str] = None
    lastRun: Optional[dict | None]
    destinationSchema: str = ""
    lock: Optional[dict | None]
    isRunning: Optional[bool] = False
    resetConnDeploymentId: Optional[str] = None
    clearConnDeploymentId: Optional[str] = None


class AirbyteConnectionUpdate(Schema):
    """Docstring"""

    name: str
    streams: list
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
