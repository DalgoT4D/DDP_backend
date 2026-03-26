"""Session-scoped cache helpers for dashboard chat runtime snapshots."""

from typing import Any

from ddpui.core.dashboard_chat.context.allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts import DashboardChatSchemaSnippet

DASHBOARD_CHAT_SESSION_CACHE_TTL_SECONDS = 24 * 60 * 60


def build_dashboard_chat_session_snapshot_cache_key(session_id: str) -> str:
    """Return the cache key used for one chat session's runtime snapshot."""
    return f"dashboard_chat:session_snapshot:{session_id}"


def serialize_allowlist(allowlist: DashboardChatAllowlist) -> dict[str, Any]:
    """Convert an allowlist to a cache-safe dictionary payload."""
    return {
        "chart_tables": sorted(allowlist.chart_tables),
        "upstream_tables": sorted(allowlist.upstream_tables),
        "allowed_tables": sorted(allowlist.allowed_tables),
        "allowed_unique_ids": sorted(allowlist.allowed_unique_ids),
        "unique_id_to_table": dict(allowlist.unique_id_to_table),
        "table_to_unique_ids": {
            table_name: sorted(unique_ids)
            for table_name, unique_ids in allowlist.table_to_unique_ids.items()
        },
    }


def deserialize_allowlist(payload: dict[str, Any] | None) -> DashboardChatAllowlist:
    """Rebuild an allowlist from cached data."""
    payload = payload or {}
    return DashboardChatAllowlist(
        chart_tables=set(payload.get("chart_tables") or []),
        upstream_tables=set(payload.get("upstream_tables") or []),
        allowed_tables=set(payload.get("allowed_tables") or []),
        allowed_unique_ids=set(payload.get("allowed_unique_ids") or []),
        unique_id_to_table=dict(payload.get("unique_id_to_table") or {}),
        table_to_unique_ids={
            table_name: set(unique_ids)
            for table_name, unique_ids in (payload.get("table_to_unique_ids") or {}).items()
        },
    )


def serialize_schema_snippets(
    snippets: dict[str, DashboardChatSchemaSnippet],
) -> dict[str, Any]:
    """Convert schema snippets to a cache-safe dictionary payload."""
    return {
        table_name: {
            "table_name": snippet.table_name,
            "columns": list(snippet.columns),
        }
        for table_name, snippet in snippets.items()
    }


def deserialize_schema_snippets(
    payload: dict[str, Any] | None,
) -> dict[str, DashboardChatSchemaSnippet]:
    """Rebuild schema snippets from cached data."""
    snippets: dict[str, DashboardChatSchemaSnippet] = {}
    for table_name, snippet_payload in (payload or {}).items():
        snippets[table_name.lower()] = DashboardChatSchemaSnippet(
            table_name=str(snippet_payload.get("table_name") or table_name),
            columns=list(snippet_payload.get("columns") or []),
        )
    return snippets


def serialize_distinct_cache(
    distinct_cache: set[tuple[str, str, str]],
) -> dict[str, Any]:
    """Convert validated distinct values to a cache-safe nested payload."""
    serialized: dict[str, dict[str, list[str]]] = {}
    for table_name, column_name, value in distinct_cache:
        serialized.setdefault(table_name, {}).setdefault(column_name, []).append(value)

    return {
        table_name: {
            column_name: sorted(set(values))
            for column_name, values in column_map.items()
        }
        for table_name, column_map in serialized.items()
    }


def deserialize_distinct_cache(
    payload: dict[str, Any] | None,
) -> set[tuple[str, str, str]]:
    """Rebuild validated distinct values from cached data."""
    distinct_cache: set[tuple[str, str, str]] = set()
    for table_name, column_map in (payload or {}).items():
        for column_name, values in (column_map or {}).items():
            for value in values or []:
                distinct_cache.add(
                    (str(table_name).lower(), str(column_name).lower(), str(value))
                )
    return distinct_cache
