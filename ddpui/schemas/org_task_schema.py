from typing import Optional
from ninja import Schema


class TaskParameters(Schema):
    """Schema to parameterize Task Runs"""

    flags: list | None
    options: dict | None


class SelectedStream(Schema):
    """Schema to define a selected stream"""

    name: str
    namespace: str = None


class ClearSelectedStreams(Schema):
    """Schema to define the request payload for clearing selected streams in Airbyte"""

    connectionId: str
    streams: list[SelectedStream]


class CreateOrgTaskPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    task_slug: str
    flags: list | None
    options: dict | None


class DbtProjectSchema(Schema):
    default_schema: str
