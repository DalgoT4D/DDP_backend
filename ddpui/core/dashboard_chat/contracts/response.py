"""Response-related dashboard chat contracts."""

from dataclasses import asdict, dataclass, field
import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from .intents import DashboardChatIntent


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
