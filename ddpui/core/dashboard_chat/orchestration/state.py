"""Shared graph state and constants for dashboard chat orchestration."""

from typing import Any, TypedDict
import re

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatCitation,
    DashboardChatConversationContext,
    DashboardChatConversationMessage,
    DashboardChatIntentDecision,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSchemaSnippet,
    DashboardChatSqlValidationResult,
)
from ddpui.models.org import Org

SMALL_TALK_FAST_PATH_PATTERN = re.compile(
    r"^\s*(hi|hello|hey|yo|good\s+morning|good\s+afternoon|good\s+evening|thanks|thank\s+you|what\s+can\s+you\s+do|who\s+are\s+you)\b[\s!.?]*$",
    re.IGNORECASE,
)


class DashboardChatRuntimeState(TypedDict, total=False):
    """LangGraph state for one dashboard chat turn."""

    org: Org
    dashboard_id: int
    session_id: str | None
    vector_collection_name: str | None
    user_query: str
    conversation_history: list[DashboardChatConversationMessage]
    conversation_context: DashboardChatConversationContext
    small_talk_response: str | None
    dashboard_export: dict[str, Any]
    dbt_index: dict[str, Any]
    allowlist: DashboardChatAllowlist
    session_schema_cache: dict[str, DashboardChatSchemaSnippet]
    session_distinct_cache: set[tuple[str, str, str]]
    intent_decision: DashboardChatIntentDecision
    retrieved_documents: list[DashboardChatRetrievedDocument]
    citations: list[DashboardChatCitation]
    tool_calls: list[dict[str, Any]]
    timing_breakdown: dict[str, Any]
    sql: str | None
    sql_validation: DashboardChatSqlValidationResult | None
    sql_results: list[dict[str, Any]] | None
    warnings: list[str]
    usage: dict[str, Any]
    response: DashboardChatResponse
