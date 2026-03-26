"""Load-context node for dashboard chat graph."""

from typing import Any

from ..session_snapshot import load_session_snapshot
from ..state import DashboardChatRuntimeState


def load_context_node(state: DashboardChatRuntimeState) -> dict[str, Any]:
    """Load or reuse the session-stable dashboard context snapshot."""
    snapshot = load_session_snapshot(state)
    return {
        "dashboard_export": snapshot["dashboard_export"],
        "dbt_index": snapshot["dbt_index"],
        "allowlist": snapshot["allowlist"],
        "session_schema_cache": snapshot["schema_cache"],
        "session_distinct_cache": snapshot["distinct_cache"],
    }
