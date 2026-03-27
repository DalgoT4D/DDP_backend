"""Conversation-related dashboard chat contracts."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DashboardChatConversationMessage:
    """Single prior conversation message."""

    role: str
    content: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DashboardChatConversationContext:
    """Reusable context extracted from prior assistant turns."""

    last_sql_query: str | None = None
    last_tables_used: list[str] = field(default_factory=list)
    last_chart_ids: list[str] = field(default_factory=list)
    last_metrics: list[str] = field(default_factory=list)
    last_dimensions: list[str] = field(default_factory=list)
    last_filters: list[str] = field(default_factory=list)
    last_response_type: str | None = None
    last_answer_text: str | None = None
    last_intent: str | None = None
