"""Response-related dashboard chat contracts."""

import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder
from pydantic import BaseModel, ConfigDict, Field

from ddpui.core.dashboard_chat.contracts.intent_contracts import DashboardChatIntent


class DashboardChatCitation(BaseModel):
    """Citation attached to a chat response."""

    model_config = ConfigDict(frozen=True)

    source_type: str
    source_identifier: str
    title: str
    snippet: str
    url: str | None = None
    dashboard_id: int | None = None
    table_name: str | None = None


class DashboardChatResponse(BaseModel):
    """Final runtime response returned by the LangGraph runner."""

    model_config = ConfigDict(frozen=True)

    answer_text: str
    intent: DashboardChatIntent
    citations: list[DashboardChatCitation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    sql: str | None = None
    sql_results: list[dict[str, Any]] | None = None
    usage: dict[str, Any] = Field(default_factory=dict)
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable payload safe for JSON storage and websocket delivery.

        Uses DjangoJSONEncoder to handle warehouse-specific types (Decimal, datetime, etc.)
        that may appear in sql_results.
        """
        return json.loads(json.dumps(self.model_dump(mode="json"), cls=DjangoJSONEncoder))
