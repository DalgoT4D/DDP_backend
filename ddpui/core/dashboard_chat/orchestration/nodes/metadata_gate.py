"""Helpers for blocking dashboard chat when metadata artifacts are not ready."""

from __future__ import annotations

from typing import Any

from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.contracts.response_contracts import DashboardChatResponse
from ddpui.core.dashboard_chat.orchestration.response_composer import build_usage_summary
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState


def metadata_artifact_is_ready(state: DashboardChatGraphState) -> bool:
    """Return whether the dashboard-scoped metadata artifact is ready for runtime use."""
    return (
        state.get("metadata_artifact_status") == "ready"
        and state.get("metadata_artifact_payload") is not None
    )


def metadata_blocked_response(
    state: DashboardChatGraphState,
    llm_client,
) -> dict[str, Any]:
    """Return an explicit blocked response when metadata is missing or unavailable."""
    status = str(state.get("metadata_artifact_status") or "missing")
    answer_text = {
        "missing": (
            "Dashboard chat metadata has not been built for this dashboard yet. "
            "Rebuild metadata from AI settings before asking dashboard questions."
        ),
        "building": (
            "Dashboard chat metadata is currently being built for this dashboard. "
            "Wait for the build to finish, then try again."
        ),
        "failed": (
            "Dashboard chat metadata failed to build for this dashboard. "
            "Rebuild metadata from AI settings and check the build error there before trying again."
        ),
        "stale": (
            "Dashboard chat metadata is stale for this dashboard. "
            "Rebuild metadata from AI settings before asking dashboard questions."
        ),
    }.get(
        status,
        "Dashboard chat metadata is not ready for this dashboard. "
        "Rebuild metadata from AI settings before asking dashboard questions.",
    )
    return {
        "response": DashboardChatResponse(
            answer_text=answer_text,
            intent=_intent_from_state(state),
            warnings=[f"dashboard_chat_metadata_{status}"],
            usage=build_usage_summary(llm_client),
            metadata={
                "runtime_blocked": True,
                "metadata_artifact_status": status,
            },
        ).to_dict()
    }


def _intent_from_state(state: DashboardChatGraphState) -> DashboardChatIntent:
    raw_intent_decision = state.get("intent_decision") or {}
    try:
        return DashboardChatIntentDecision.model_validate(raw_intent_decision).intent
    except Exception:
        return DashboardChatIntent.NEEDS_CLARIFICATION
