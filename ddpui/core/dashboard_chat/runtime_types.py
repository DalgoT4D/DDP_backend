"""Typed runtime contracts for dashboard chat orchestration."""

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class DashboardChatIntent(str, Enum):
    """Supported high-level intents for dashboard chat."""

    DATA_QUERY = "data_query"
    CONTEXT_QUERY = "context_query"
    NEEDS_CLARIFICATION = "needs_clarification"
    SMALL_TALK = "small_talk"
    IRRELEVANT = "irrelevant"


class DashboardChatPlanMode(str, Enum):
    """Execution modes chosen after planning."""

    SQL = "sql"
    CONTEXT = "context"
    CLARIFY = "clarify"


@dataclass(frozen=True)
class DashboardChatMessage:
    """Single prior conversation message."""

    role: str
    content: str


@dataclass(frozen=True)
class DashboardChatIntentDecision:
    """Intent-routing outcome."""

    intent: DashboardChatIntent
    reason: str
    force_sql_path: bool = False
    clarification_question: str | None = None


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
class DashboardChatTextFilterPlan:
    """Filter that requires a distinct-values lookup before SQL generation."""

    table_name: str
    column_name: str
    requested_value: str


@dataclass(frozen=True)
class DashboardChatQueryPlan:
    """Structured plan produced before SQL generation."""

    mode: DashboardChatPlanMode
    reason: str
    relevant_tables: list[str] = field(default_factory=list)
    schema_lookup_tables: list[str] = field(default_factory=list)
    text_filters: list[DashboardChatTextFilterPlan] = field(default_factory=list)
    answer_strategy: str | None = None
    clarification_question: str | None = None


@dataclass(frozen=True)
class DashboardChatSqlDraft:
    """LLM-produced SQL draft and metadata."""

    sql: str | None
    reason: str
    warnings: list[str] = field(default_factory=list)
    clarification_question: str | None = None


@dataclass(frozen=True)
class DashboardChatSchemaSnippet:
    """Schema description for a warehouse table."""

    table_name: str
    columns: list[dict[str, Any]]

    def to_prompt_text(self) -> str:
        """Format a compact schema summary for prompts."""
        column_lines = []
        for column in self.columns:
            column_lines.append(
                "- {name} ({data_type}, nullable={nullable})".format(
                    name=column.get("name"),
                    data_type=column.get("data_type"),
                    nullable=column.get("nullable"),
                )
            )
        return f"Table: {self.table_name}\n" + "\n".join(column_lines)


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
    chart_id: int | None = None
    table_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable citation payload."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardChatRelatedDashboard:
    """Dashboard suggestion when the current dashboard is not sufficient."""

    dashboard_id: int
    title: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable suggestion payload."""
        return asdict(self)


@dataclass(frozen=True)
class DashboardChatResponse:
    """Final runtime response returned by the LangGraph runner."""

    answer_text: str
    intent: DashboardChatIntent
    citations: list[DashboardChatCitation] = field(default_factory=list)
    related_dashboards: list[DashboardChatRelatedDashboard] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sql: str | None = None
    sql_results: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable API payload."""
        return {
            "answer_text": self.answer_text,
            "intent": self.intent.value,
            "citations": [citation.to_dict() for citation in self.citations],
            "related_dashboards": [
                related_dashboard.to_dict()
                for related_dashboard in self.related_dashboards
            ],
            "warnings": self.warnings,
            "sql": self.sql,
            "sql_results": self.sql_results,
            "metadata": self.metadata,
        }
