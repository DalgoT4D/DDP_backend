from ninja import Field, Schema
import uuid


class CreateDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    name: str
    display_name: str
    dest_schema: str
    input_uuids: list[uuid.UUID]
    config: dict
    op_type: str


class SyncSourcesSchema(Schema):
    """
    schema to sync sources from the schema
    """

    schema_name: str
    source_name: str
