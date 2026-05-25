"""Shared graph state for dashboard chat orchestration."""

from typing import Any, TypedDict


class DashboardChatGraphState(TypedDict, total=False):
    """LangGraph state for one dashboard chat turn."""

    org_id: int
    dashboard_id: int
    session_id: str | None
    user_query: str
    conversation_history: list[dict[str, Any]]
    conversation_context: dict[str, Any]
    small_talk_response: str | None
    org_context_markdown: str
    dashboard_context_markdown: str
    dashboard_export_payload: dict[str, Any]
    chart_registry_payload: list[dict[str, Any]]
    metadata_artifact_payload: dict[str, Any] | None
    metadata_artifact_status: str | None
    allowlist_payload: dict[str, Any]
    schema_snippet_payloads: dict[str, Any]
    validated_distinct_payloads: dict[str, Any]
    intent_decision: dict[str, Any]
    retrieved_documents: list[dict[str, Any]]
    citations: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]
    timing_breakdown: dict[str, Any]
    draft_answer_text: str | None
    sql: str | None
    sql_validation: dict[str, Any] | None
    sql_results: list[dict[str, Any]] | None
    warnings: list[str]
    usage: dict[str, Any]
    response: dict[str, Any]
