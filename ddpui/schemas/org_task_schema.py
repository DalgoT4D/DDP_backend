from ninja import Schema, Field


class CreateOrgTaskPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    task_slug: str
    flags: list | None
    options: dict | None
