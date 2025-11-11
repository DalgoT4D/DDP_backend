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
    canvas_lock_id: str = None


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


# ==============================================================================
# UI4T V2 API - New unified architecture using CanvasNode and CanvasEdge
# ==============================================================================


class ModelSrcNodeInputPayload(Schema):
    """Schema to define inputs for a multi input operation. The node_uuid refers to a CanvasNode"""

    node_uuid: str
    columns: list[str] = []
    seq: int = 1


class CreateOperationNodePayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    config: dict
    base_node_uuid: (
        str  # The CanvasNode (source/model/operation) on which this operation is applied
    )
    op_type: str
    source_columns: list[str]
    other_inputs: list[
        ModelSrcNodeInputPayload
    ] = []  # List of other CanvasNode inputs for multi-input operations
    canvas_lock_id: str = None


class EditOperationNodePayload(Schema):
    """
    schema to define the payload required to edit a dbt operation
    """

    config: dict
    op_type: str
    source_columns: list[str] = []
    other_inputs: list[
        ModelSrcNodeInputPayload
    ] = []  # List of other CanvasNode inputs for multi-input operations
    canvas_lock_id: str = None
