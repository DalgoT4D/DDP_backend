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
    user_prompt: str


class SaveLlmSessionRequest(Schema):
    """
    Payload to save the llm analysis session for future reference
    """

    session_name: str
    overwrite: bool = False
    old_session_id: str = None  # if overwrite is True, then this is required


class LlmSessionFeedbackRequest(Schema):
    """
    Payload to give feedback for llm session
    """

    feedback: str


class WarehouseRagTrainConfig(Schema):
    """
    Configuration to be used to train rag on warehouse
    """

    exclude_schemas: list[str] = []
    exclude_tables: list[str] = []
    exclude_columns: list[str] = []


class PgVectorCreds(Schema):
    """Pg Vector Creds where the embeddings for the RAG should be stored"""

    username: str
    password: str
    host: str
    port: int
    database: str
