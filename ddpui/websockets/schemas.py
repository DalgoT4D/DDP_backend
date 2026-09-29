from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict
from ninja import Schema

from ddpui.core.ai.typed_dicts import InputRequiredEvent


class WebsocketCloseCodes:
    """Custom WebSocket close codes (4000-4999 range is for application use)"""

    NO_TOKEN = 4001
    INVALID_TOKEN = 4003
    FORBIDDEN = 4004  # authenticated but not allowed (permission/flag/consent/ownership)


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


class PendingInput(Schema):
    """A chat session's unanswered approval/question card, as stored in Redis
    by the Chat with Data consumer — re-sent on reconnect, and the source of
    truth a resume is checked against."""

    # extra="allow" reaches the nested TypedDicts too (Pydantic applies the
    # parent model's config to them): the event is re-sent to the browser, which
    # must not silently lose a key that was added to the event but not to
    # InputRequiredEvent.
    model_config = ConfigDict(from_attributes=True, extra="allow")

    kind: Literal["approval", "question"]
    # the model the paused turn started on; the resume continues on it
    model: Optional[str] = None
    # keeps every run of one question on one Langfuse trace
    trace_id: Optional[str] = None
    event: InputRequiredEvent


# ========================================================================
