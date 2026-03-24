"""Typed runtime contracts for dashboard chat orchestration."""

from dataclasses import asdict, dataclass, field
from enum import Enum
import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder


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


@dataclass(frozen=True)
class DashboardChatRetrievedDocument:
    """Retrieved document returned from the vector store."""

    document_id: str
    source_type: str
    source_identifier: str
    content: str
    dashboard_id: int | None = None
    distance: float | None = None


@dataclass(frozen=True)
class DashboardChatSchemaSnippet:
    """Schema description for a warehouse table."""

    table_name: str
    columns: list[dict[str, Any]]


@dataclass(frozen=True)
class DashboardChatSqlValidationResult:
    """Outcome of SQL guard validation."""

    is_valid: bool
    sanitized_sql: str | None
    tables: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DashboardChatCitation:
    """Citation attached to a chat response."""

    source_type: str
    source_identifier: str
    title: str
    snippet: str
    dashboard_id: int | None = None
    table_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable citation payload."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardChatResponse:
    """Final runtime response returned by the LangGraph runner."""

    answer_text: str
    intent: DashboardChatIntent
    citations: list[DashboardChatCitation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sql: str | None = None
    sql_results: list[dict[str, Any]] | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable payload."""
        payload = {
            "answer_text": self.answer_text,
            "intent": self.intent.value,
            "citations": [citation.to_dict() for citation in self.citations],
            "warnings": self.warnings,
            "sql": self.sql,
            "sql_results": self.sql_results,
            "usage": self.usage,
            "tool_calls": self.tool_calls,
            "metadata": self.metadata,
        }
        return json.loads(json.dumps(payload, cls=DjangoJSONEncoder))
