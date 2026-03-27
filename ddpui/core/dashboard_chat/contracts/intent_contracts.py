"""Intent-routing dashboard chat contracts."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DashboardChatIntent(str, Enum):
    """Prototype-aligned top-level intents for dashboard chat."""

    QUERY_WITH_SQL = "query_with_sql"
    QUERY_WITHOUT_SQL = "query_without_sql"
    FOLLOW_UP_SQL = "follow_up_sql"
    FOLLOW_UP_CONTEXT = "follow_up_context"
    NEEDS_CLARIFICATION = "needs_clarification"
    SMALL_TALK = "small_talk"
    IRRELEVANT = "irrelevant"


@dataclass(frozen=True)
class DashboardChatFollowUpContext:
    """Prototype-style follow-up metadata returned by the router."""

    is_follow_up: bool
    follow_up_type: str | None = None
    reusable_elements: dict[str, Any] = field(default_factory=dict)
    modification_instruction: str | None = None


@dataclass(frozen=True)
class DashboardChatIntentDecision:
    """Intent-routing outcome."""

    intent: DashboardChatIntent
    confidence: float
    reason: str
    missing_info: list[str] = field(default_factory=list)
    force_tool_usage: bool = False
    clarification_question: str | None = None
    follow_up_context: DashboardChatFollowUpContext = field(
        default_factory=lambda: DashboardChatFollowUpContext(is_follow_up=False)
    )
