"""Answer composition and display-shaping helpers for dashboard chat."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
    DashboardChatRetrievedDocument,
)
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.orchestration.state import (
    DashboardChatRuntimeState,
    SMALL_TALK_FAST_PATH_PATTERN,
)

logger = CustomLogger("dashboard_chat")


def serialize_tool_result(result: dict[str, Any]) -> dict[str, Any]:
    """Trim large tool payloads before feeding them back into the model."""
    serialized = dict(result)
    docs = serialized.get("docs")
    if isinstance(docs, list) and len(docs) > 6:
        serialized["docs"] = docs[:6]
    rows = serialized.get("rows")
    if isinstance(rows, list) and len(rows) > 5:
        serialized["rows"] = rows[:5]
    values = serialized.get("values")
    if isinstance(values, list) and len(values) > 20:
        serialized["values"] = values[:20]
    return serialized


def summarize_tool_call(
    *,
    tool_name: str,
    args: dict[str, Any],
    result: dict[str, Any],
    duration_ms: float | None = None,
) -> dict[str, Any]:
    """Persist a compact execution trace for one tool call."""
    entry: dict[str, Any] = {"name": tool_name, "args": args}
    if duration_ms is not None:
        entry["duration_ms"] = duration_ms
    if tool_name == "retrieve_docs":
        entry["count"] = result.get("count", 0)
        entry["doc_ids"] = [doc.get("doc_id") for doc in result.get("docs", [])[:6]]
    elif tool_name == "get_schema_snippets":
        entry["tables"] = [table.get("table") for table in result.get("tables", [])]
    elif tool_name == "search_dbt_models":
        entry["count"] = result.get("count", 0)
        entry["models"] = [
            model.get("table") or model.get("name") for model in result.get("models", [])
        ]
    elif tool_name == "get_dbt_model_info":
        entry["model"] = result.get("model")
        entry["column_count"] = len(result.get("columns") or [])
    elif tool_name == "get_distinct_values":
        entry["error"] = result.get("error")
        entry["count"] = result.get("count", 0)
        entry["values_sample"] = (result.get("values") or [])[:10]
    elif tool_name == "list_tables_by_keyword":
        entry["tables"] = [table.get("table") for table in result.get("tables", [])]
    elif tool_name == "check_table_row_count":
        entry["row_count"] = result.get("row_count")
    elif tool_name == "run_sql_query":
        entry["success"] = result.get("success", False)
        entry["row_count"] = result.get("row_count", 0)
        entry["sql_used"] = result.get("sql_used")
        entry["error"] = result.get("error")
    else:
        entry["result"] = result
    return entry


def max_turns_message(
    user_query: str,
    retrieved_documents: Sequence[DashboardChatRetrievedDocument],
) -> str:
    """Return a bounded fallback when the tool loop exhausts its budget."""
    if retrieved_documents:
        return (
            "I found relevant dashboard context, but I couldn't complete the analysis safely. "
            "Please rephrase the question or ask about a metric shown on this dashboard."
        )
    return (
        f"I couldn't find enough dashboard-backed context to answer: {user_query}. "
        "Please rephrase or ask about a metric shown on this dashboard."
    )


def compose_final_answer_text(
    llm_client,
    state: DashboardChatRuntimeState,
    execution_result: dict[str, Any],
    *,
    response_format: str,
) -> str:
    """Compose one final markdown answer for all non-trivial routes."""
    normalized_sql_results = normalize_sql_results_for_answer(execution_result.get("sql_results"))
    draft_answer = (execution_result.get("answer_text") or "").strip() or None
    if hasattr(llm_client, "compose_final_answer"):
        try:
            answer_text = llm_client.compose_final_answer(
                user_query=state["user_query"],
                intent=state["intent_decision"].intent,
                response_format=response_format,
                draft_answer=draft_answer,
                retrieved_documents=list(execution_result.get("retrieved_documents") or []),
                sql=execution_result.get("sql"),
                sql_results=normalized_sql_results,
                warnings=list(execution_result.get("warnings") or []),
            )
            if answer_text:
                return answer_text
        except Exception:
            logger.exception("Dashboard chat final answer composition failed")
    return fallback_answer_text(
        execution_result.get("retrieved_documents") or [],
        normalized_sql_results,
        response_format=response_format,
        draft_answer=draft_answer,
    )


def determine_response_format(
    *,
    user_query: str,
    sql_results: list[dict[str, Any]] | None,
) -> str:
    """Return how the frontend should present the final answer."""
    if not sql_results:
        return "text"
    first_row = sql_results[0] if sql_results else {}
    column_count = len(first_row.keys()) if isinstance(first_row, dict) else 0
    normalized_query = user_query.lower()
    tableish_keywords = [
        "breakdown",
        "split by",
        "list",
        "table",
        "tabular",
        "rank",
        "ranking",
        "top ",
        "bottom ",
        "wise",
    ]
    if "table" in normalized_query and column_count > 0:
        return "table"
    if len(sql_results) > 1 and column_count > 1:
        return "text_with_table"
    if any(keyword in normalized_query for keyword in tableish_keywords) and column_count > 1:
        return "text_with_table"
    return "text"


def sql_result_columns(sql_results: list[dict[str, Any]] | None) -> list[str]:
    """Return table columns for frontend rendering metadata."""
    if not sql_results:
        return []
    first_row = sql_results[0]
    if not isinstance(first_row, dict):
        return []
    return list(first_row.keys())


def build_usage_summary(llm_client, vector_store) -> dict[str, Any]:
    """Collect per-turn usage from the llm client and embedding provider."""
    usage: dict[str, Any] = {}
    if hasattr(llm_client, "usage_summary"):
        llm_usage = llm_client.usage_summary()
        if llm_usage:
            usage["llm"] = llm_usage
    if hasattr(vector_store, "usage_summary"):
        embedding_usage = vector_store.usage_summary()
        if embedding_usage:
            usage["embeddings"] = embedding_usage
    return usage


def compose_small_talk_response(llm_client, user_query: str) -> str:
    """Generate the small-talk response or fall back to a fixed helper."""
    if hasattr(llm_client, "compose_small_talk"):
        try:
            return llm_client.compose_small_talk(user_query)
        except Exception:
            logger.exception("Dashboard chat small-talk generation failed")
    return "Hi! I can help with your program data and metrics. What would you like to know?"


def build_fast_path_intent(user_query: str) -> DashboardChatIntentDecision | None:
    """Handle obvious greetings without an LLM round trip."""
    if not SMALL_TALK_FAST_PATH_PATTERN.match(user_query.strip()):
        return None
    return DashboardChatIntentDecision(
        intent=DashboardChatIntent.SMALL_TALK,
        confidence=1.0,
        reason="Obvious small-talk fast path",
    )


def build_fast_path_small_talk_response(user_query: str) -> str:
    """Keep basic small-talk replies instant and deterministic."""
    normalized_query = user_query.strip().lower()
    if "what can you do" in normalized_query:
        return (
            "I can explain this dashboard, describe charts and metrics, look up dbt context, "
            "and answer data questions with safe read-only SQL against this dashboard's data."
        )
    if "who are you" in normalized_query:
        return (
            "I'm the dashboard chat assistant for this dashboard. I can explain the charts, "
            "data, dbt context, and answer questions about the data behind it."
        )
    if "thank" in normalized_query:
        return "You're welcome. Ask me anything about this dashboard or its data."
    if "good morning" in normalized_query:
        return "Good morning. Ask me anything about this dashboard or the data behind it."
    if "good afternoon" in normalized_query:
        return "Good afternoon. Ask me anything about this dashboard or the data behind it."
    if "good evening" in normalized_query:
        return "Good evening. Ask me anything about this dashboard or the data behind it."
    return "Hi. Ask me anything about this dashboard or the data behind it."


def clarification_fallback(missing_info: Sequence[str]) -> str:
    """Return a specific clarification nudge when the router omits a question."""
    missing = {item.lower() for item in missing_info}
    prompts: list[str] = []
    if "metric" in missing:
        prompts.append("which metric")
    if "time_range" in missing or "time period" in missing:
        prompts.append("what time period")
    if "dimension" in missing:
        prompts.append("which breakdown or dimension")
    if not prompts:
        return "Could you be more specific about the metric, program, or time period you want?"
    return "Could you clarify " + ", ".join(prompts) + "?"


def fallback_answer_text(
    retrieved_documents: Sequence[DashboardChatRetrievedDocument],
    sql_results: list[dict[str, Any]] | None,
    *,
    response_format: str = "text",
    draft_answer: str | None = None,
) -> str:
    """Fallback response when the model returns no final text."""
    from .retrieval import compact_snippet

    if draft_answer:
        return draft_answer
    if sql_results is not None:
        if not sql_results:
            return "I didn't find any matching rows for that question."
        if response_format in {"text_with_table", "table"}:
            return (
                f"I found {len(sql_results)} matching rows. See the table below for the breakdown."
            )
        if len(sql_results) == 1:
            return single_row_summary(sql_results[0])
        return f"I found {len(sql_results)} matching rows."
    if retrieved_documents:
        return compact_snippet(retrieved_documents[0].content)
    return "I couldn't find enough context to answer that."


def single_row_summary(row: dict[str, Any]) -> str:
    """Return a readable fallback when one structured row is available."""
    parts = [f"{humanize_column_name(col)}: {value}" for col, value in row.items()]
    return "; ".join(parts)


def humanize_column_name(column_name: str) -> str:
    """Convert snake_case warehouse columns into human labels."""
    return str(column_name).replace("_", " ").strip().title()


def normalize_sql_results_for_answer(
    sql_results: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Normalize SQL results into LLM-friendly values for final answer writing."""
    if sql_results is None:
        return None
    return [
        {col: normalize_sql_value_for_answer(col, val) for col, val in row.items()}
        for row in sql_results
    ]


def normalize_sql_value_for_answer(column_name: str, value: Any) -> Any:
    """Format warehouse values into user-friendly forms for answer composition."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return format_numeric_answer_value(column_name, value)
    text_value = str(value)
    numeric_value = parse_numeric_string(text_value)
    if numeric_value is None:
        return text_value
    return format_numeric_answer_value(column_name, numeric_value)


def format_numeric_answer_value(column_name: str, value: float | int) -> str | int | float:
    """Format numeric values for answer composition."""
    if looks_like_rate_metric(column_name) and 0 <= float(value) <= 1:
        percentage_value = f"{float(value) * 100:.1f}".rstrip("0").rstrip(".")
        return f"{percentage_value}%"
    rounded_value = round(float(value), 2)
    if float(rounded_value).is_integer():
        return int(rounded_value)
    return f"{rounded_value:.2f}".rstrip("0").rstrip(".")


def parse_numeric_string(value: str) -> float | None:
    """Parse decimal-like strings emitted by DjangoJSONEncoder."""
    normalized_value = value.strip()
    if not normalized_value:
        return None
    if not re.fullmatch(r"-?\d+(?:\.\d+)?(?:E-?\d+)?", normalized_value, flags=re.IGNORECASE):
        return None
    try:
        return float(normalized_value)
    except ValueError:
        return None


def looks_like_rate_metric(column_name: str) -> bool:
    """Return whether a metric name likely represents a percentage/rate."""
    normalized_column = column_name.lower()
    return any(
        token in normalized_column
        for token in ["rate", "ratio", "percentage", "percent", "share", "pct"]
    )
