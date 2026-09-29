"""The ToolMessage.artifact contract — the ONE place that interprets artifacts.

Tools attach structured artifacts to their ToolMessages (content_and_artifact):

    execute_sql       {"sql", "status": "success"|"error"|"rejected",
                       "row_count"?, "columns"?, "rows"?, "error"?}
    creation tools    CreatedArtifact  {"type", "object_id", "title", "url_path"}
    rejected creation RejectedArtifact {"type", "status": "rejected", "error"}

(typed as SqlArtifact / CreationArtifact in typed_dicts.py)

The streaming runner, the turn audit, and history replay all read artifacts
through these helpers so the three views of a turn can never disagree.
"""

from typing import Optional, get_args

from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from typing_extensions import TypeIs

from ddpui.core.ai.messages.content import extract_text
from ddpui.schemas.chat_with_data_schemas import ArtifactType, CreatedArtifact
from ddpui.core.ai.typed_dicts import (
    CreatedArtifactChip,
    CreationArtifact,
    ResultTable,
    SqlArtifact,
    SqlQueryEntry,
    ToolArtifact,
)

# Artifacts that represent a created Dalgo object (vs an execute_sql result)
CREATION_ARTIFACT_TYPES = get_args(ArtifactType)

# Checkpoints written before CreatedArtifact carried the id under per-type keys
_LEGACY_ID_KEYS = ("chart_id", "dashboard_id")


def tool_artifact(message: ToolMessage) -> ToolArtifact | None:
    """The message's structured artifact, or None when the tool attached none."""
    artifact = getattr(message, "artifact", None)
    return artifact if isinstance(artifact, dict) else None


def is_creation_artifact(artifact: ToolArtifact) -> TypeIs[CreationArtifact]:
    """Chart/dashboard creation artifacts carry a "type" key; execute_sql
    artifacts never do."""
    return artifact.get("type") in CREATION_ARTIFACT_TYPES


def sql_query_entry(artifact: SqlArtifact) -> SqlQueryEntry:
    """One execute_sql call as the audit row / turn-audit prompt records it."""
    return {
        "sql": artifact.get("sql"),
        "status": artifact.get("status"),
        "row_count": artifact.get("row_count"),
        "error": artifact.get("error"),
    }


def sql_result_table(artifact: SqlArtifact) -> ResultTable | None:
    """The result table the UI renders for a successful execute_sql, else None."""
    if artifact.get("status") != "success":
        return None
    return {
        "columns": artifact.get("columns", []),
        "rows": artifact.get("rows", []),
        "row_count": artifact.get("row_count", 0),
    }


def creation_chip(artifact: CreationArtifact) -> CreatedArtifactChip | None:
    """The created-artifact chip the UI renders, or None for a rejected creation."""
    if artifact.get("status") == "rejected":
        return None
    object_id = artifact.get("object_id") or next(
        (artifact[key] for key in _LEGACY_ID_KEYS if artifact.get(key)), None
    )
    if not object_id:
        return None
    return CreatedArtifact(
        type=artifact["type"],
        object_id=object_id,
        title=artifact.get("title", ""),
        url_path=artifact.get("url_path", ""),
    ).model_dump()


def extract_turn_results(
    messages: list[AnyMessage],
) -> tuple[list[SqlQueryEntry], Optional[ResultTable], str]:
    """(sql_queries, last successful result_table, final answer text) for one
    turn's messages — the turn audit's view of what the agent did."""
    sql_queries: list[SqlQueryEntry] = []
    result_table: Optional[ResultTable] = None
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
