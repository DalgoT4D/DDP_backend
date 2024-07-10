from ninja import Field, Schema


class RequestorColumnSchema(Schema):
    """
    schema to query on insights for a column
    """

    db_schema: str
    db_table: str
    column_name: str
    filter: dict = None
    refresh: bool = False


class AskWarehouseRequest(Schema):
    """
    Payload to ask warehouse a question to be responded via llm
    """

    sql: str
    session_name: str
    user_prompt: str
