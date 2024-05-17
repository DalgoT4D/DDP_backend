from ninja import Field, Schema


class ColumnMetrics(Schema):
    """
    schema to define the payload required to create a custom org task
    """

    db_schema: str
    db_table: str
    column_name: str
    filter: dict = None
