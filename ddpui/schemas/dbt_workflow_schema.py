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
    canvas_lock_id: str = None


class EditDbtOperationPayload(Schema):
    """
    schema to define the payload required to edit a dbt operation
    """

    config: dict
    op_type: str
    input_uuid: str = ""
    source_columns: list[str] = []
    other_inputs: list[InputModelPayload] = []
    canvas_lock_id: str = None


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


class LockCanvasRequestSchema(Schema):
    """schema to acquire a lock on the ui4t canvas"""

    lock_id: str = None


class LockCanvasResponseSchema(Schema):
    """schema representing lock on the ui4t canvas"""

    lock_id: str = None
    locked_by: str
    locked_at: str
