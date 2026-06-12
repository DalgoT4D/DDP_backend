"""Intent-routing dashboard chat contracts."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DashboardChatIntent(str, Enum):
    """Prototype-aligned top-level intents for dashboard chat."""

    QUERY_WITH_SQL = "query_with_sql"
    QUERY_WITHOUT_SQL = "query_without_sql"
    FOLLOW_UP_SQL = "follow_up_sql"
    FOLLOW_UP_CONTEXT = "follow_up_context"
    NEEDS_CLARIFICATION = "needs_clarification"
    SMALL_TALK = "small_talk"
    IRRELEVANT = "irrelevant"


class DashboardChatFollowUpContext(BaseModel):
    """Prototype-style follow-up metadata returned by the router."""

    model_config = ConfigDict(frozen=True)

    is_follow_up: bool
    follow_up_type: str | None = None
    reusable_elements: dict[str, Any] = Field(default_factory=dict)
    modification_instruction: str | None = None


class DashboardChatIntentDecision(BaseModel):
    """Intent-routing outcome."""

    model_config = ConfigDict(frozen=True)

    intent: DashboardChatIntent
    confidence: float
    reason: str
    missing_info: list[str] = Field(default_factory=list)
    force_tool_usage: bool = False
    clarification_question: str | None = None
    follow_up_context: DashboardChatFollowUpContext = Field(
        default_factory=lambda: DashboardChatFollowUpContext(is_follow_up=False)
    )
