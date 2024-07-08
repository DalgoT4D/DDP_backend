from pydantic import BaseModel
from ninja import Schema
from enum import Enum


class WebsocketResponseStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"


class WebsocketResponse(Schema):
    """
    Generic schema for all responses sent back via websockets
    """

    message: str
    status: WebsocketResponseStatus
    data: dict = {}


# ========================================================================
