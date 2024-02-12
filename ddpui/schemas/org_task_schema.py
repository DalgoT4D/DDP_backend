from ninja import Field, Schema


class CreateOrgTaskPayload(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    task_slug: str
    flags: list | None
    options: dict | None


class DbtProjectSchema(Schema):

    project_name: str
    default_schema: str
