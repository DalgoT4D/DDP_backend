from ninja import Schema


class FetchChatMessagesRequest(Schema):
    """
    Schema to fetch conversation messages for a orguser
    """

    thread_uuid: str
    limit: int = 10
    offset: int = 0


class StartThreadRequest(Schema):
    """
    Schema to start a thread and open a session with llm service
    """

    sql: str
    meta: dict = {}


class AskChatWithDataBotRequest(Schema):
    """
    Schema to ask question to chat with data bot
    """

    message: str
    thread_uuid: str


class CloseThreadRequest(Schema):
    """
    Schema to ask question to chat with data bot
    """

    thread_uuid: str


class GenerateSqlAndStartThreadRequest(Schema):
    """
    Schema to take in the user prompt for sql filter to start a thread
    """

    user_prompt: str
