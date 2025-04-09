from ninja import Field, Schema


class EdgeConfig(Schema):
    """Define a config of a directed edge"""

    source_columns: list[str]
    seq: int = 0


class CreateDbtModelNodePayload(Schema):
    """
    schema to define the payload required to create a model node on the canvas
    """

    canvas_lock_id: str = None


class ChainOperationPayload(Schema):
    """
    schema to chain operation on an existing node
    """

    op_type: str
    config: dict
    canvas_lock_id: str = None
