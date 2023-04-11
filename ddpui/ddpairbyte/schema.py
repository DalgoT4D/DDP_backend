from typing import Optional
from ninja import Schema


# request schemas
class AirbyteWorkspaceCreate(Schema):
    """Docstring"""

    name: str


class AirbyteSourceCreate(Schema):
    """Docstring"""

    name: str
    sourcedef_id: str
    config: dict


class AirbyteSourceUpdate(Schema):
    """Docstring"""

    name: str
    config: dict

class AirbyteDestinationCreate(Schema):
    """Docstring"""

    name: str
    destinationdef_id: str
    config: dict

class AirbyteDestinationUpdate(Schema):
    """Schema for updating an Airbyte destination"""

    destination_id: str
    name: Optional[str] = None
    config: Optional[dict] = None
    
class AirbyteConnectionCreate(Schema):
    """Docstring"""

    name: str
    source_id: str
    destination_id: str
    streamnames: list


class AirbyteConnectionUpdate(Schema):
    """Docstring"""

    connection_id: str
    name: Optional[str] = None
    source_id: Optional[str] = None
    destination_id: Optional[str] = None
    streamnames: Optional[list] = None


# response schemas
class AirbyteWorkspace(Schema):
    """Docstring"""

    name: str
    workspaceId: str
    initialSetupComplete: bool
