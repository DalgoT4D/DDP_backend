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
    destinationId: str = None
    destinationSchema: str = None
    streams: list
    normalize: bool = False

class AirbyteConnectionUpdate(Schema):
    """Docstring"""

    name: str
    streams: list
    normalize: bool = False
    destinationId: str = None
    destinationSchema: str = None


# response schemas
class AirbyteWorkspace(Schema):
    """Docstring"""

    name: str
    workspaceId: str
    initialSetupComplete: bool
