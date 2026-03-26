"""Session-snapshot helpers for dashboard chat graph execution."""

from typing import Any

from django.core.cache import cache

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlistBuilder
from ddpui.core.dashboard_chat.contracts import DashboardChatSchemaSnippet
from ddpui.core.dashboard_chat.sessions.cache import (
    DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS,
    build_dashboard_chat_session_snapshot_cache_key,
    deserialize_allowlist,
    deserialize_distinct_cache,
    deserialize_schema_snippets,
    serialize_allowlist,
    serialize_distinct_cache,
    serialize_schema_snippets,
)
from ddpui.services.dashboard_service import DashboardService

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatRuntimeState


def load_session_snapshot(state: DashboardChatRuntimeState) -> dict[str, Any]:
    """Return the current session's frozen dashboard context snapshot."""
    session_id = state.get("session_id")
    if not session_id:
        return build_session_snapshot(state)

    cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is not None:
        return {
            "dashboard_export": dict(cached_snapshot["dashboard_export"]),
            "dbt_index": dict(cached_snapshot.get("dbt_index") or {"resources_by_unique_id": {}}),
            "allowlist": deserialize_allowlist(cached_snapshot.get("allowlist")),
            "schema_cache": deserialize_schema_snippets(cached_snapshot.get("schema_cache")),
            "distinct_cache": deserialize_distinct_cache(cached_snapshot.get("distinct_cache")),
        }

    snapshot = build_session_snapshot(state)
    cache.set(
        cache_key,
        {
            "dashboard_export": snapshot["dashboard_export"],
            "dbt_index": snapshot["dbt_index"],
            "allowlist": serialize_allowlist(snapshot["allowlist"]),
            "schema_cache": serialize_schema_snippets(snapshot["schema_cache"]),
            "distinct_cache": serialize_distinct_cache(snapshot["distinct_cache"]),
        },
        DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS,
    )
    return snapshot


def build_session_snapshot(state: DashboardChatRuntimeState) -> dict[str, Any]:
    """Build one session-stable snapshot of dashboard-specific runtime context."""
    dashboard_export = DashboardService.export_dashboard_context(
        state["dashboard_id"],
        state["org"],
    )
    manifest_json = DashboardChatAllowlistBuilder.load_manifest_json(state["org"].dbt)
    allowlist = DashboardChatAllowlistBuilder.build(
        dashboard_export,
        manifest_json=manifest_json,
    )
    return {
        "dashboard_export": dashboard_export,
        "dbt_index": DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist),
        "allowlist": allowlist,
        "schema_cache": {},
        "distinct_cache": set(),
    }


def persist_session_schema_cache(
    state: DashboardChatRuntimeState,
    schema_cache: dict[str, DashboardChatSchemaSnippet],
) -> None:
    """Persist lazily loaded schema snippets back into the session snapshot cache."""
    session_id = state.get("session_id")
    if not session_id:
        state["session_schema_cache"] = dict(schema_cache)
        return

    cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is None:
        return
    cached_snapshot["schema_cache"] = serialize_schema_snippets(schema_cache)
    cache.set(cache_key, cached_snapshot, DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS)
    state["session_schema_cache"] = dict(schema_cache)


def persist_session_distinct_cache(
    state: DashboardChatRuntimeState,
    distinct_cache: set[tuple[str, str, str]],
) -> None:
    """Persist validated distinct values back into the session snapshot cache."""
    session_id = state.get("session_id")
    if not session_id:
        state["session_distinct_cache"] = set(distinct_cache)
        return

    cache_key = build_dashboard_chat_session_snapshot_cache_key(session_id)
    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is None:
        return
    cached_snapshot["distinct_cache"] = serialize_distinct_cache(distinct_cache)
    cache.set(cache_key, cached_snapshot, DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS)
    state["session_distinct_cache"] = set(distinct_cache)
