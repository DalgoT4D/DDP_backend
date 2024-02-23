from ninja import Field, Schema


class TaskParameters(Schema):
    """Schema to parameterize Task Runs"""

    flags: list | None
    options: dict | None


class CreateOrgTaskPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    task_slug: str
    flags: list | None
    options: dict | None


class DbtProjectSchema(Schema):

    default_schema: str
