"""Answer composition and display-shaping helpers for dashboard chat."""

from collections.abc import Sequence
import re
from typing import Any

from ddpui.core.dashboard_chat.agents.final_answer_formatting import query_requests_name_list
from ddpui.core.dashboard_chat.contracts.intent_contracts import (
    DashboardChatIntent,
    DashboardChatIntentDecision,
)
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatRetrievedDocument
from ddpui.utils.custom_logger import CustomLogger

from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.pii_masking import unmask_pii_text
from ddpui.core.dashboard_chat.orchestration.retrieval_support import compact_snippet

logger = CustomLogger("dashboard_chat")

SMALL_TALK_FAST_PATH_PATTERN = re.compile(
    r"^\s*(hi|hello|hey|yo|good\s+morning|good\s+afternoon|good\s+evening|thanks|thank\s+you|what\s+can\s+you\s+do|who\s+are\s+you)\b[\s!.?]*$",
    re.IGNORECASE,
)
ADVISORY_FAST_PATH_PATTERN = re.compile(
    r"^\s*(?:please\s+)?(how can we improve|what should we do|what can we do to improve|how to improve|what do you recommend|recommend)\b",
    re.IGNORECASE,
)


def _query_explicitly_requests_numeric_evidence(user_query: str) -> bool:
    """Return whether an advisory question explicitly asks for numeric evidence."""
    normalized_query = (user_query or "").lower()
    numeric_evidence_markers = [
        "how many",
        "count",
        "number of",
        "what percent",
        "percentage",
        "trend",
        "compare",
        "ranking",
        "rank",
        "top ",
        "bottom ",
        "numeric evidence",
        "use data",
        "use the data",
        "with numbers",
        "show numbers",
        "exact figures",
        "breakdown",
    ]
    return any(marker in normalized_query for marker in numeric_evidence_markers)


def _query_looks_referential(user_query: str) -> bool:
    """Return whether the query likely depends on the immediately prior turn."""
    normalized_query = f" {(user_query or '').lower()} "
    referential_markers = [
        " that ",
        " this ",
        " those ",
        " these ",
        " it ",
        " them ",
        " they ",
        " same ",
    ]
    return any(marker in normalized_query for marker in referential_markers)


def _compact_metadata_column(column: dict[str, Any]) -> dict[str, Any]:
    """Keep only query-planning fields from a metadata column."""
    statistics = column.get("statistics") if isinstance(column.get("statistics"), dict) else {}
    compact: dict[str, Any] = {
        "column_name": column.get("column_name") or column.get("name") or "",
        "data_type": column.get("data_type") or "",
        "description": column.get("description") or "",
        "semantic_role": column.get("semantic_role") or "",
        "value_semantics": column.get("value_semantics") or "",
        "pii": bool(column.get("pii")),
    }
    if statistics:
        compact_statistics = {
            "nullable": statistics.get("nullable"),
            "null_percentage": statistics.get("null_percentage"),
            "sample_values": (statistics.get("sample_values") or [])[:10],
            "range_profile": statistics.get("range_profile"),
        }
        compact["statistics"] = {
            key: value
            for key, value in compact_statistics.items()
            if value not in (None, [], {})
        }
    return {key: value for key, value in compact.items() if value not in ("", [], {})}


def _compact_metadata_table(
    table: dict[str, Any],
    *,
    include_column_details: bool = False,
) -> dict[str, Any]:
    """Keep table semantics useful for SQL generation without the full artifact payload."""
    column_names = [
        column.get("column_name") or column.get("name")
        for column in table.get("columns") or []
        if isinstance(column, dict) and (column.get("column_name") or column.get("name"))
    ]
    pii_columns = [
        column.get("column_name") or column.get("name")
        for column in table.get("columns") or []
        if isinstance(column, dict)
        and bool(column.get("pii"))
        and (column.get("column_name") or column.get("name"))
    ]
    compact: dict[str, Any] = {
        "table_name": table.get("table_name"),
        "model_name": table.get("model_name"),
        "layer": table.get("layer"),
        "table_type": table.get("table_type"),
        "description": table.get("description"),
        "upstream_models": table.get("upstream_models") or [],
        "statistics": table.get("statistics") or {},
        "primary_entities": table.get("primary_entities") or [],
        "grain": table.get("grain") or {},
        "temporal": table.get("temporal") or {},
        "counting": table.get("counting") or {},
        "answerability": table.get("answerability") or {},
        "chart_usage": (table.get("chart_usage") or [])[:8],
        "column_names": column_names[:100],
        "pii_columns": pii_columns[:40],
    }
    if include_column_details:
        compact["columns"] = [
            _compact_metadata_column(column)
            for column in (table.get("columns") or [])[:30]
            if isinstance(column, dict)
        ]
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def _compact_column_metadata_result(result: dict[str, Any]) -> dict[str, Any]:
    """Compact get_column_metadata results for the model-facing tool transcript."""
    columns = []
    for item in result.get("columns") or []:
        if not isinstance(item, dict):
            continue
        column = item.get("column") if isinstance(item.get("column"), dict) else {}
        columns.append(
            {
                "table_name": item.get("table_name"),
                "column": _compact_metadata_column(column),
                "matched_terms": item.get("matched_terms") or [],
                "table_row_grain": item.get("table_row_grain"),
                "table_type": item.get("table_type"),
            }
        )
    return {"count": result.get("count", len(columns)), "columns": columns}


def _compact_schema_result(result: dict[str, Any]) -> dict[str, Any]:
    """Compact schema snippets while preserving exact column names and types."""
    tables = []
    for table in result.get("tables") or []:
        if not isinstance(table, dict):
            continue
        columns = []
        for column in table.get("columns") or []:
            if not isinstance(column, dict):
                continue
            columns.append(
                {
                    "name": column.get("name"),
                    "type": column.get("type") or column.get("data_type"),
                }
            )
        table_payload = {
            "table": table.get("table"),
            "columns": columns,
            "profile": table.get("profile") or {},
            "hint": table.get("hint"),
        }
        tables.append(
            {
                key: value
                for key, value in table_payload.items()
                if value not in (None, "", [], {})
            }
        )
    compact: dict[str, Any] = {"tables": tables}
    if result.get("filtered_tables"):
        compact["filtered_tables"] = result.get("filtered_tables")
        compact["filter_note"] = result.get("filter_note")
    return compact


def serialize_tool_result(result: dict[str, Any], *, tool_name: str | None = None) -> dict[str, Any]:
    """Trim large tool payloads before feeding them back into the model."""
    if tool_name in {"get_chart_table_metadata", "get_table_metadata"}:
        serialized = {
            "count": result.get("count", len(result.get("tables") or [])),
            "tables": [
                _compact_metadata_table(table)
                for table in (result.get("tables") or [])
                if isinstance(table, dict)
            ],
        }
        if tool_name == "get_chart_table_metadata":
            serialized["charts"] = (result.get("charts") or [])[:12]
        return serialized
    if tool_name == "get_column_metadata":
        return _compact_column_metadata_result(result)
    if tool_name == "search_metadata":
        return {
            "count": result.get("count", len(result.get("tables") or [])),
            "tables": [
                _compact_metadata_table(table)
                for table in (result.get("tables") or [])
                if isinstance(table, dict)
            ],
        }
    if tool_name == "get_schema_snippets":
        return _compact_schema_result(result)

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
    if tool_name == "get_chart_table_metadata":
        entry["count"] = result.get("count", 0)
        entry["charts"] = [chart.get("title") for chart in result.get("charts", [])[:6]]
        entry["tables"] = [table.get("table_name") for table in result.get("tables", [])[:6]]
    elif tool_name == "search_metadata":
        entry["count"] = result.get("count", 0)
        entry["tables"] = [table.get("table_name") for table in result.get("tables", [])[:8]]
    elif tool_name == "get_table_metadata":
        entry["count"] = result.get("count", 0)
        entry["tables"] = [table.get("table_name") for table in result.get("tables", [])[:8]]
    elif tool_name == "get_column_metadata":
        entry["count"] = result.get("count", 0)
        entry["columns"] = [
            (
                f"{item.get('table_name')}."
                f"{(item.get('column') or {}).get('column_name') or (item.get('column') or {}).get('name')}"
            )
            for item in result.get("columns", [])[:12]
        ]
    elif tool_name == "search_columns_by_name":
        entry["count"] = result.get("count", 0)
        entry["columns"] = [
            f"{item.get('table_name')}.{item.get('column_name')}"
            for item in result.get("columns", [])[:12]
        ]
    elif tool_name == "get_join_paths":
        entry["count"] = result.get("count", 0)
        entry["joins"] = [
            f"{join.get('source_table')}->{join.get('target_table')}"
            for join in result.get("joins", [])[:10]
        ]
    elif tool_name == "get_related_tables":
        entry["count"] = result.get("count", 0)
        entry["tables"] = [table.get("table_name") for table in result.get("tables", [])[:8]]
    elif tool_name == "get_table_statistics":
        entry["count"] = result.get("count", 0)
        entry["tables"] = [table.get("table_name") for table in result.get("tables", [])[:8]]
    elif tool_name == "resolve_time_scope":
        entry["resolved_ranges"] = result.get("resolved_ranges", [])
    elif tool_name == "read_full_metadata":
        entry["table_count"] = len(result.get("tables") or [])
    elif tool_name == "get_schema_snippets":
        entry["tables"] = [table.get("table") for table in result.get("tables", [])]
    elif tool_name == "get_distinct_values":
        entry["error"] = result.get("error")
        entry["count"] = result.get("count", 0)
        entry["values_sample"] = (result.get("values") or [])[:10]
    elif tool_name == "set_sql_query_plan":
        entry["success"] = result.get("success", False)
        entry["plan"] = result.get("plan")
    elif tool_name == "check_table_row_count":
        entry["row_count"] = result.get("row_count")
    elif tool_name == "run_sql_query":
        entry["success"] = result.get("success", False)
        entry["row_count"] = result.get("row_count", 0)
        entry["sql_used"] = result.get("sql_used")
        entry["error"] = result.get("error")
        entry["severity"] = result.get("severity")
        entry["reason_code"] = result.get("reason_code")
        entry["issues"] = result.get("issues")
        entry["repair_instructions"] = result.get("repair_instructions")
        entry["reasoning"] = result.get("reasoning")
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
    state: DashboardChatGraphState,
    execution_result: dict[str, Any],
    *,
    response_format: str,
) -> str:
    """Compose one final markdown answer for all non-trivial routes."""
    normalized_sql_results = normalize_sql_results_for_answer(execution_result.get("sql_results"))
    draft_answer = (execution_result.get("answer_text") or "").strip() or None
    pii_value_map = dict(execution_result.get("pii_value_map") or {})
    deterministic_answer = _deterministic_name_list_answer(
        user_query=state["user_query"],
        sql_results=normalized_sql_results,
    )
    if deterministic_answer:
        return unmask_pii_text(deterministic_answer, pii_value_map)
    if execution_result.get("sql_rejection") and not normalized_sql_results:
        return (
            draft_answer
            or "I couldn't produce a validated SQL query for this question. "
            "The generated SQL was rejected because it did not faithfully match the requested "
            "measure, grain, filters, or output shape."
        )
    if hasattr(llm_client, "compose_final_answer"):
        try:
            answer_text = llm_client.compose_final_answer(
                user_query=state["user_query"],
                intent=DashboardChatIntentDecision.model_validate(
                    state.get("intent_decision") or {}
                ).intent,
                response_format=response_format,
                draft_answer=draft_answer,
                retrieved_documents=list(execution_result.get("retrieved_documents") or []),
                sql=execution_result.get("sql"),
                sql_results=normalized_sql_results,
                warnings=list(execution_result.get("warnings") or []),
            )
            if answer_text:
                return unmask_pii_text(answer_text, pii_value_map)
        except Exception:
            logger.exception("Dashboard chat final answer composition failed")
    return unmask_pii_text(
        fallback_answer_text(
            execution_result.get("retrieved_documents") or [],
            normalized_sql_results,
            response_format=response_format,
            draft_answer=draft_answer,
        ),
        pii_value_map,
    )


def _deterministic_name_list_answer(
    *,
    user_query: str,
    sql_results: list[dict[str, Any]] | None,
) -> str | None:
    """Render simple name-list answers without a final LLM call."""
    if not sql_results or not query_requests_name_list(user_query):
        return None
    name_column = _select_name_list_column(user_query, sql_results[0])
    if not name_column:
        return None
    names: list[str] = []
    seen: set[str] = set()
    for row in sql_results:
        value = row.get(name_column)
        if value in (None, ""):
            continue
        name = str(value)
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    if not names:
        return "No matching names were found."
    return "\n".join(f"- {name}" for name in names)


def _select_name_list_column(user_query: str, first_row: dict[str, Any]) -> str | None:
    """Pick the entity-name column that best matches the user's requested list."""
    columns = [str(column) for column in first_row.keys()]
    normalized_query = user_query.lower()
    preference_groups: list[list[str]] = []
    if any(term in normalized_query for term in ["student", "learner", "child", "pupil"]):
        preference_groups.append(["student_name", "learner_name", "child_name", "pupil_name"])
    if "fellow" in normalized_query:
        preference_groups.append(["fellow_name"])
    if "pm" in normalized_query or "program manager" in normalized_query:
        preference_groups.append(["pm_name", "program_manager_name"])
    if "school" in normalized_query:
        preference_groups.append(["school_name"])
    preference_groups.append(["name"])
    for preferences in preference_groups:
        for preferred in preferences:
            for column in columns:
                normalized_column = column.lower()
                if normalized_column == preferred or normalized_column.endswith(f"_{preferred}"):
                    return column
    for column in columns:
        if "name" in column.lower():
            return column
    return None


def determine_response_format(
    *,
    user_query: str,
    sql_results: list[dict[str, Any]] | None,
) -> str:
    """Return how the frontend should present the final answer."""
    if not sql_results:
        return "text"
    if query_requests_name_list(user_query):
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


def build_usage_summary(llm_client) -> dict[str, Any]:
    """Collect per-turn usage from the LLM client."""
    usage: dict[str, Any] = {}
    if hasattr(llm_client, "usage_summary"):
        llm_usage = llm_client.usage_summary()
        if llm_usage:
            usage["llm"] = llm_usage
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
    """Handle obvious greetings and advisory asks without an LLM round trip."""
    stripped_query = user_query.strip()
    if SMALL_TALK_FAST_PATH_PATTERN.match(stripped_query):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.SMALL_TALK,
            confidence=1.0,
            reason="Obvious small-talk fast path",
        )
    if (
        ADVISORY_FAST_PATH_PATTERN.match(stripped_query)
        and not _query_explicitly_requests_numeric_evidence(stripped_query)
        and not _query_looks_referential(stripped_query)
    ):
        return DashboardChatIntentDecision(
            intent=DashboardChatIntent.QUERY_WITHOUT_SQL,
            confidence=0.95,
            reason="Advisory recommendation fast path",
            force_tool_usage=False,
        )
    return None


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
