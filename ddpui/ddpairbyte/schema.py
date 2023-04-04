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


class AirbyteDestinationCreate(Schema):
    """Docstring"""

    name: str
    destinationdef_id: str
    config: dict


class AirbyteConnectionCreate(Schema):
    """Docstring"""

    name: str
    source_id: str
    destination_id: str
    streamnames: list


# response schemas
class AirbyteWorkspace(Schema):
    """Docstring"""

    name: str
    workspaceId: str
    initialSetupComplete: bool
