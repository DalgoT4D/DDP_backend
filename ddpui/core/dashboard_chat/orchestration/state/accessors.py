"""Typed accessors for reconstructing runtime views from checkpoint-safe state payloads."""

from ddpui.models.org import Org

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import (
    DashboardChatConversationContext,
    DashboardChatIntentDecision,
    DashboardChatResponse,
    DashboardChatRetrievedDocument,
    DashboardChatSqlValidationResult,
)
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import (
    deserialize_allowlist,
    deserialize_conversation_context,
    deserialize_intent_decision,
    deserialize_response,
    deserialize_retrieved_documents,
    deserialize_sql_validation_result,
)

def get_runtime_org(state: DashboardChatGraphState) -> Org:
    """Return the Django org object for the current runtime state."""
    return Org.objects.select_related("dbt").get(id=int(state["org_id"]))


def get_runtime_allowlist(state: DashboardChatGraphState) -> DashboardChatAllowlist:
    """Return the reconstructed allowlist for the current runtime state."""
    return deserialize_allowlist(state.get("allowlist_payload"))


def get_conversation_context(state: DashboardChatGraphState) -> DashboardChatConversationContext:
    """Return the reconstructed conversation context for the current runtime state."""
    return deserialize_conversation_context(state.get("conversation_context"))


def get_intent_decision(state: DashboardChatGraphState) -> DashboardChatIntentDecision:
    """Return the reconstructed intent decision for the current runtime state."""
    return deserialize_intent_decision(state.get("intent_decision"))


def get_runtime_response(state: DashboardChatGraphState) -> DashboardChatResponse:
    """Return the reconstructed response contract for the current runtime state."""
    return deserialize_response(state.get("response"))


def get_retrieved_documents(
    state: DashboardChatGraphState,
) -> list[DashboardChatRetrievedDocument]:
    """Return the reconstructed retrieved-document contracts for the current runtime state."""
    return deserialize_retrieved_documents(state.get("retrieved_documents"))


def get_sql_validation_result(
    state: DashboardChatGraphState,
) -> DashboardChatSqlValidationResult | None:
    """Return the reconstructed SQL validation result for the current runtime state."""
    return deserialize_sql_validation_result(state.get("sql_validation"))
