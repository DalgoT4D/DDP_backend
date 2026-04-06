"""Websocket event contracts for dashboard chat."""

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict


class DashboardChatProgressStage(str, Enum):
    """Stable progress stages exposed to the chat UI."""

    UNDERSTANDING_QUESTION = "understanding_question"
    LOADING_CONTEXT = "loading_context"
    SEARCHING_CONTEXT = "searching_context"
    VALIDATING_QUERY = "validating_query"
    QUERYING_DATA = "querying_data"
    PREPARING_ANSWER = "preparing_answer"
    CANCELLING = "cancelling"


class DashboardChatProgressEvent(BaseModel):
    """One in-flight progress update for a running dashboard-chat turn."""

    model_config = ConfigDict(frozen=True)

    event_type: Literal["progress"] = "progress"
    session_id: str
    turn_id: str
    dashboard_id: int
    occurred_at: datetime
    label: str
    stage: DashboardChatProgressStage | None = None
    message_id: str | None = None


class DashboardChatCancelledEvent(BaseModel):
    """Event emitted when one dashboard-chat turn is cancelled."""

    model_config = ConfigDict(frozen=True)

    event_type: Literal["cancelled"] = "cancelled"
    session_id: str
    turn_id: str
    dashboard_id: int
    occurred_at: datetime
    label: str


class DashboardChatAssistantMessageEvent(BaseModel):
    """Assistant message event emitted over websocket after one turn completes."""

    model_config = ConfigDict(frozen=True)

    event_type: Literal["assistant_message"] = "assistant_message"
    session_id: str
    turn_id: str
    message_id: str
    dashboard_id: int
    occurred_at: datetime
    id: str
    role: Literal["assistant"]
    content: str
    created_at: datetime
    payload: dict
    response_latency_ms: int | None = None
    timing_breakdown: dict | None = None
