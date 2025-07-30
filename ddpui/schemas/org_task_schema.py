from ninja import Schema
from typing import List


class TaskParameters(Schema):
    """Schema to parameterize Task Runs"""

    flags: list | None
    options: dict | None


class StreamSchema(Schema):
    streamName: str


class ClearSelectedStreams(Schema):
    """Schema to define the request payload for clearing selected streams in Airbyte"""

    connection_id: str
    streams: List[StreamSchema]


class CreateOrgTaskPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    task_slug: str
    flags: list | None
    options: dict | None


class DbtProjectSchema(Schema):
    default_schema: str
