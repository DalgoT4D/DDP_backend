from ninja import Field, Schema


class InputModelPayload(Schema):
    """
    Schema to be expected when we are creating models in a chain
    """

    uuid: str
    columns: list[str] = []
    seq: int = 1


class CreateDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    config: dict
    op_type: str
    target_model_uuid: str = ""
    input_uuid: str = ""
    source_columns: list[str] = []
    other_inputs: list[InputModelPayload] = []


class CompleteDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    name: str
    display_name: str
    dest_schema: str


class SyncSourcesSchema(Schema):
    """
    schema to sync sources from the schema
    """

    schema_name: str = None
    source_name: str = None
