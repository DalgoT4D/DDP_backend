"""Load-context node for dashboard chat graph."""

from typing import Any

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlistBuilder
from ddpui.services.dashboard_service import DashboardService

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.state.payload_codec import serialize_allowlist
from ddpui.core.dashboard_chat.orchestration.state.accessors import get_runtime_org


def load_context_node(state: DashboardChatGraphState) -> dict[str, Any]:
    """Bootstrap or reuse the session-stable checkpointed dashboard context."""
    if (
        state.get("dashboard_export_payload") is not None
        and state.get("allowlist_payload") is not None
        and state.get("dbt_index") is not None
    ):
        return {
            "schema_snippet_payloads": dict(state.get("schema_snippet_payloads") or {}),
            "validated_distinct_payloads": dict(state.get("validated_distinct_payloads") or {}),
        }

    org = get_runtime_org(state)
    dashboard_export = DashboardService.export_dashboard_context(
        state["dashboard_id"],
        org,
    )
    manifest_json = DashboardChatAllowlistBuilder.load_manifest_json(org.dbt)
    allowlist = DashboardChatAllowlistBuilder.build(
        dashboard_export,
        manifest_json=manifest_json,
    )
    return {
        "dashboard_export_payload": dashboard_export,
        "dbt_index": DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist),
        "allowlist_payload": serialize_allowlist(allowlist),
        "schema_snippet_payloads": dict(state.get("schema_snippet_payloads") or {}),
        "validated_distinct_payloads": dict(state.get("validated_distinct_payloads") or {}),
    }
