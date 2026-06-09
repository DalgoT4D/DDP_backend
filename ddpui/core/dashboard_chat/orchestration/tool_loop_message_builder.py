"""Prompt-message stack helpers for dashboard chat graph execution."""

from typing import Any

from django.utils import timezone

from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatChartRegistryEntry
from ddpui.models.dashboard_chat import DashboardChatPromptTemplateKey

from ddpui.core.dashboard_chat.orchestration.conversation_context import (
    build_follow_up_context_prompt,
    detect_sql_modification_type,
)
from ddpui.core.dashboard_chat.contracts.conversation_contracts import (
    DashboardChatConversationContext,
)
from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def build_runtime_date_prompt() -> str:
    """Return runtime date context for relative time interpretation."""
    return (
        "RUNTIME DATE CONTEXT:\n"
        f"- Today is {timezone.localdate().isoformat()}.\n"
        "- Use this date for phrases like 'this year', 'this quarter', and 'this month' unless the user clearly names a different fiscal year, program year, or explicit date range."
    )


def build_inline_dashboard_assets_prompt(state: DashboardChatGraphState) -> str:
    """Return the always-on context bundle sent with every SQL-planning turn."""
    chart_registry = [
        DashboardChatChartRegistryEntry.model_validate(entry)
        for entry in (state.get("chart_registry_payload") or [])
    ]
    chart_lines = [
        (
            f"- [{chart.chart_id}] {chart.title}"
            f" | section={chart.section or 'unsectioned'}"
            f" | type={chart.chart_type or 'unknown'}"
            f" | table={chart.preferred_table or 'unknown'}"
            f" | metrics={', '.join(chart.metric_columns) or 'none'}"
            f" | dimensions={', '.join(chart.dimension_columns) or 'none'}"
            f" | time={chart.time_column or 'none'}"
        )
        for chart in chart_registry
    ]

    return (
        "INLINE DASHBOARD ASSETS:\n"
        "Use these assets before exploring deeper metadata.\n\n"
        "ORGANIZATION CONTEXT:\n"
        f"{(state.get('org_context_markdown') or '').strip() or '(none)'}\n\n"
        "DASHBOARD CONTEXT:\n"
        f"{(state.get('dashboard_context_markdown') or '').strip() or '(none)'}\n\n"
        "CHART REGISTRY:\n"
        f"{chr(10).join(chart_lines) if chart_lines else '- (no charts available)'}\n\n"
        "ENRICHED METADATA RUNTIME NOTES:\n"
        f"- Metadata artifact status: {state.get('metadata_artifact_status') or 'missing'}.\n"
        "- Inspect chart-table metadata first.\n"
        "- Only search related tables or join paths if the chart tables cannot completely answer the question.\n"
        "- read_full_metadata is an extreme last resort."
    )


def build_route_guidance_prompt(state: DashboardChatGraphState) -> str | None:
    """Return route-specific guidance for the current turn."""
    intent_payload = state.get("intent_decision") or {}
    if not intent_payload:
        return None
    intent = DashboardChatIntentDecision.model_validate(intent_payload).intent
    if intent in {
        DashboardChatIntent.QUERY_WITHOUT_SQL,
        DashboardChatIntent.FOLLOW_UP_CONTEXT,
    }:
        return (
            "ROUTE GUIDANCE:\n"
            "- This turn is primarily contextual or advisory.\n"
            "- Default to a direct answer from dashboard context, chart context, and enriched metadata.\n"
            "- If the user asks what the dashboard is built on, use per-table upstream_models from metadata to explain the underlying lineage, then mention surfaced chart tables only as secondary context.\n"
            "- If the user is asking for recommendations such as how can we improve, what should we do, or what do you recommend, do not start with SQL.\n"
            "- Use SQL only if one or two targeted queries would materially strengthen the recommendation or resolve a concrete factual ambiguity.\n"
            "- Do not run a long analytical workflow unless the user explicitly asks for numeric evidence, exact figures, or a concrete breakdown."
        )
    return None


def build_new_query_messages(
    llm_client,
    state: DashboardChatGraphState,
) -> list[dict[str, Any]]:
    """Build the new-query message stack."""
    system_prompt = llm_client.get_prompt(DashboardChatPromptTemplateKey.NEW_QUERY_SYSTEM)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "system", "content": build_runtime_date_prompt()},
        {"role": "system", "content": build_inline_dashboard_assets_prompt(state)},
        {"role": "user", "content": state["user_query"]},
    ]
    route_guidance = build_route_guidance_prompt(state)
    if route_guidance:
        messages.insert(3, {"role": "system", "content": route_guidance})
    return messages


def build_follow_up_messages(
    llm_client,
    state: DashboardChatGraphState,
) -> list[dict[str, Any]]:
    """Build the follow-up message stack."""
    modification_type = detect_sql_modification_type(state["user_query"])
    system_prompt = llm_client.get_prompt(DashboardChatPromptTemplateKey.FOLLOW_UP_SYSTEM)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "system", "content": build_runtime_date_prompt()},
        {"role": "system", "content": build_inline_dashboard_assets_prompt(state)},
        {
            "role": "system",
            "content": build_follow_up_context_prompt(
                DashboardChatConversationContext.model_validate(
                    state.get("conversation_context") or {}
                ),
                state["user_query"],
            ),
        },
        {"role": "system", "content": f"MODIFICATION_TYPE: {modification_type}"},
        {"role": "user", "content": state["user_query"]},
    ]
    route_guidance = build_route_guidance_prompt(state)
    if route_guidance:
        messages.insert(3, {"role": "system", "content": route_guidance})
    return messages
