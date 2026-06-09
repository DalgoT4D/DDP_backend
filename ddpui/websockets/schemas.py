from pydantic import BaseModel
from ninja import Schema
from enum import Enum


class WebsocketCloseCodes:
    """Custom WebSocket close codes (4000-4999 range is for application use)"""

    NO_TOKEN = 4001
    INVALID_TOKEN = 4003


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
