"""The ToolMessage.artifact contract — the ONE place that interprets artifacts.

Tools attach structured artifacts to their ToolMessages (content_and_artifact):

    execute_sql       {"sql", "status": "success"|"error"|"rejected",
                       "row_count"?, "columns"?, "rows"?, "error"?}
    create_chart      {"type": "chart", "chart_id", "title", "url_path"}
    dashboard tools   {"type": "dashboard", "dashboard_id", "title", "url_path"}
    rejected creation {"type": "chart"|"dashboard", "status": "rejected", "error"}

The streaming runner, the turn audit, and history replay all read artifacts
through these helpers so the three views of a turn can never disagree.
"""

from typing import Optional

from langchain_core.messages import AIMessage, AnyMessage, ToolMessage

from ddpui.core.ai.messages.content import extract_text

# Artifacts that represent a created Dalgo object (vs an execute_sql result)
CREATION_ARTIFACT_TYPES = ("chart", "dashboard")


def tool_artifact(message: ToolMessage) -> dict | None:
    """The message's structured artifact, or None when the tool attached none."""
    artifact = getattr(message, "artifact", None)
    return artifact if isinstance(artifact, dict) else None


def is_creation_artifact(artifact: dict) -> bool:
    """Chart/dashboard creation artifacts carry a "type" key; execute_sql
    artifacts never do."""
    return artifact.get("type") in CREATION_ARTIFACT_TYPES


def sql_query_entry(artifact: dict) -> dict:
    """One execute_sql call as the audit row / turn-audit prompt records it."""
    return {
        "sql": artifact.get("sql"),
        "status": artifact.get("status"),
        "row_count": artifact.get("row_count"),
        "error": artifact.get("error"),
    }


def sql_result_table(artifact: dict) -> dict | None:
    """The result table the UI renders for a successful execute_sql, else None."""
    if artifact.get("status") != "success":
        return None
    return {
        "columns": artifact.get("columns", []),
        "rows": artifact.get("rows", []),
        "row_count": artifact.get("row_count", 0),
    }


def creation_chip(artifact: dict) -> dict | None:
    """The created-artifact chip the UI renders, or None for a rejected creation.

    Dashboards reuse the chart chip shape — "chart_id" is the wire-protocol key
    the frontend already renders (it picks the icon from url_path), so the key
    name stays even though the id may be a dashboard's."""
    artifact_id = artifact.get("chart_id") or artifact.get("dashboard_id")
    if not artifact_id:
        return None
    return {
        "chart_id": artifact_id,
        "title": artifact.get("title", ""),
        "url_path": artifact.get("url_path", ""),
    }


def extract_turn_results(
    messages: list[AnyMessage],
) -> tuple[list[dict], Optional[dict], str]:
    """(sql_queries, last successful result_table, final answer text) for one
    turn's messages — the turn audit's view of what the agent did."""
    sql_queries: list[dict] = []
    result_table: Optional[dict] = None
    answer = ""
    for message in messages:
        if isinstance(message, ToolMessage):
            artifact = tool_artifact(message)
            if artifact is not None and not is_creation_artifact(artifact):
                sql_queries.append(sql_query_entry(artifact))
                result_table = sql_result_table(artifact) or result_table
        elif isinstance(message, AIMessage) and not message.tool_calls:
            text = extract_text(message.content)
            if text:
                answer = text
    return sql_queries, result_table, answer
