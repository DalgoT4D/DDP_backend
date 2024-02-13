from ninja import Schema, Field


class CreateDbtModelPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    name: str
    display_name: str
    config: dict
    op_type: str
