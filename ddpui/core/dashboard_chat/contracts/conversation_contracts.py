"""Conversation-related dashboard chat contracts."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DashboardChatConversationMessage(BaseModel):
    """Single prior conversation message."""

    model_config = ConfigDict(frozen=True)

    role: str
    content: str
    payload: dict[str, Any] = Field(default_factory=dict)


class DashboardChatConversationContext(BaseModel):
    """Reusable context extracted from prior assistant turns."""

    model_config = ConfigDict(frozen=True)

    last_sql_query: str | None = None
    last_tables_used: list[str] = Field(default_factory=list)
    last_chart_ids: list[str] = Field(default_factory=list)
    last_metrics: list[str] = Field(default_factory=list)
    last_dimensions: list[str] = Field(default_factory=list)
    last_filters: list[str] = Field(default_factory=list)
    last_response_type: str | None = None
    last_answer_text: str | None = None
    last_intent: str | None = None
