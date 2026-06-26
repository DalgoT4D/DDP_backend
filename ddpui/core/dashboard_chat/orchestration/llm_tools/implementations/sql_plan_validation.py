"""SQL plan capture and deterministic plan-vs-SQL validation."""

from __future__ import annotations

import re
from typing import Any

from ddpui.core.dashboard_chat.contracts.sql_contracts import DashboardChatSqlVerificationResult
from ddpui.core.dashboard_chat.metadata.schemas import DashboardChatMetadataArtifactPayload
from ddpui.core.dashboard_chat.metadata.search import table_lookup
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState

STAGE_TO_STAGE_PATTERN = re.compile(
    r"\b(?:baseline|base|pre)\b.*\b(?:endline|end|post)\b"
    r"|\b(?:endline|end|post)\b.*\b(?:baseline|base|pre)\b",
    re.IGNORECASE | re.DOTALL,
)
GROWTH_OR_CHANGE_PATTERN = re.compile(
    r"\b(growth|grow|change|trend|improve|improvement|increase|decrease|baseline to endline|from .* to .*)\b",
    re.IGNORECASE,
)
RANKING_PATTERN = re.compile(
    r"\b(highest|lowest|top|bottom|rank|ranking|best|worst|max|min|most|least)\b",
    re.IGNORECASE,
)
NAME_LIST_PATTERN = re.compile(
    r"\b(give me the names?|show me the names?|list(?:\s+the)?\s+names?|names?\s+of|who are)\b",
    re.IGNORECASE,
)
THRESHOLD_PATTERN = re.compile(r"(?:[<>]=?|below|above|under|over|at least|less than|more than)", re.IGNORECASE)
ASSESSED_FILTER_REQUEST_PATTERN = re.compile(
    r"\b(attend|attendance|attended|assessed|completed|completion|only assessed|only attended)\b",
    re.IGNORECASE,
)


def handle_set_sql_query_plan_tool(args: dict[str, Any], turn_context) -> dict[str, Any]:
    """Store a compact SQL plan for deterministic validation before query execution."""
    plan = {
        "metric_intent": str(args.get("metric_intent") or "").strip(),
        "entity_grain": str(args.get("entity_grain") or "").strip(),
        "comparison_axes": _string_list(args.get("comparison_axes")),
        "stage_scope": str(args.get("stage_scope") or "").strip(),
        "cohort_filter_stage": str(args.get("cohort_filter_stage") or "").strip(),
        "required_measure_columns": _string_list(args.get("required_measure_columns")),
        "null_handling": str(args.get("null_handling") or "").strip(),
        "disallowed_assumptions": _string_list(args.get("disallowed_assumptions")),
        "candidate_tables": _string_list(args.get("candidate_tables")),
        "chosen_tables": _string_list(args.get("chosen_tables")),
        "why_chosen_tables_answer_directly": str(
            args.get("why_chosen_tables_answer_directly") or ""
        ).strip(),
    }
    turn_context.sql_query_plan = plan
    return {
        "success": True,
        "message": "SQL query plan recorded. Now write SQL that follows this plan exactly.",
        "plan": plan,
    }


def query_requires_sql_plan(user_query: str) -> bool:
    """Return whether the question is complex enough to require a pre-SQL plan."""
    query = user_query or ""
    return bool(
        (GROWTH_OR_CHANGE_PATTERN.search(query) and (STAGE_TO_STAGE_PATTERN.search(query) or RANKING_PATTERN.search(query)))
        or NAME_LIST_PATTERN.search(query)
        or (THRESHOLD_PATTERN.search(query) and RANKING_PATTERN.search(query))
    )


def validate_sql_against_query_plan(
    *,
    sql: str,
    state: DashboardChatGraphState,
    turn_context,
    referenced_tables: list[str],
) -> DashboardChatSqlVerificationResult | None:
    """Return a concrete repair result when SQL contradicts the plan/question."""
    user_query = state["user_query"]
    plan = getattr(turn_context, "sql_query_plan", None)
    if query_requires_sql_plan(user_query) and not plan:
        return DashboardChatSqlVerificationResult(
            is_valid=False,
            severity="repair_once",
            reason_code="missing_sql_query_plan",
            reasoning="Complex growth, ranking, threshold, or name-list SQL needs an explicit query plan before execution.",
            issues=["SQL was attempted without first recording metric/grain/stage/null-handling assumptions."],
            repair_instructions=[
                "Call set_sql_query_plan with the intended metric, grain, stage scope, cohort filter stage, required measure columns, null handling, chosen tables, and disallowed assumptions.",
                "Then regenerate SQL to match that plan.",
            ],
        )

    stage_result = _validate_stage_to_stage_growth_sql(sql=sql, user_query=user_query, plan=plan)
    if stage_result is not None:
        return stage_result

    aggregate_result = _validate_aggregate_distribution_proxy(
        sql=sql,
        user_query=user_query,
        state=state,
        referenced_tables=referenced_tables,
    )
    if aggregate_result is not None:
        return aggregate_result

    return None


def _validate_stage_to_stage_growth_sql(
    *,
    sql: str,
    user_query: str,
    plan: dict[str, Any] | None,
) -> DashboardChatSqlVerificationResult | None:
    if not (STAGE_TO_STAGE_PATTERN.search(user_query) and GROWTH_OR_CHANGE_PATTERN.search(user_query)):
        return None
    sql_lower = sql.lower()
    query_lower = user_query.lower()
    explicit_starting_stage = bool(re.search(r"\b(starting|baseline|base|pre)\s+(?:cohort|grade|class|group)\b", query_lower))

    issues: list[str] = []
    repair_instructions: list[str] = []
    reason_codes: list[str] = []
    reasoning: list[str] = []

    if not explicit_starting_stage and re.search(r"\b(?:grade|class|standard)_[a-z_]*base\s*=", sql_lower):
        reason_codes.append("stage_filter_uses_starting_stage")
        reasoning.append(
            "A stage-to-stage growth question with a grade/class filter should bind that cohort filter "
            "to the terminal/output stage unless the user explicitly asks for the starting-stage cohort."
        )
        issues.append("SQL filters a baseline/base grade/class column for a baseline-to-endline growth question.")
        repair_instructions.extend(
            [
                "Filter grade/class/cohort on the terminal/output stage column, such as an endline/end/post column.",
                "Do not also require the same grade/class at the starting stage unless the user explicitly asks for that starting-stage cohort.",
            ]
        )

    if not ASSESSED_FILTER_REQUEST_PATTERN.search(user_query) and re.search(
        r"\b[a-z0-9_]*(?:attendance|attendence|attended|completion|completed|assessed)[a-z0-9_]*\s*=\s*(?:true|1)",
        sql_lower,
    ):
        reason_codes.append("unrequested_completion_filter")
        reasoning.append("The SQL adds attendance/completion/assessed filters that the user did not ask for.")
        issues.append("Unrequested attendance/completion filters can change the growth cohort.")
        repair_instructions.extend(
            [
                "Remove attendance, completion, or assessed filters unless the user explicitly asks for them.",
                "Use the requested entity/stage/dimension filters only.",
            ]
        )

    null_handling = str((plan or {}).get("null_handling") or "").lower()
    if (
        "coalesce" not in null_handling
        and not ASSESSED_FILTER_REQUEST_PATTERN.search(user_query)
        and re.search(r"\b[a-z0-9_]*(?:base|baseline|pre)[a-z0-9_]*\s+is\s+not\s+null\b", sql_lower)
        and re.search(r"\b[a-z0-9_]*(?:end|endline|post)[a-z0-9_]*\s+is\s+not\s+null\b", sql_lower)
    ):
        reason_codes.append("growth_drops_missing_stage_values")
        reasoning.append("The SQL silently excludes rows with missing start/end measures from a growth calculation.")
        issues.append("Dropping NULL start/end values changes the growth population.")
        repair_instructions.extend(
            [
                "Use explicit null handling for paired-stage growth, normally COALESCE(start_metric, 0) and COALESCE(end_metric, 0), unless metadata or the user says to restrict to assessed/completed rows.",
                "Do not add IS NOT NULL filters on both start and end measures for a general growth question.",
            ]
        )

    if issues:
        return DashboardChatSqlVerificationResult(
            is_valid=False,
            severity="repair_once",
            reason_code="+".join(reason_codes),
            reasoning=" ".join(reasoning),
            issues=issues,
            repair_instructions=_deduplicate_strings(repair_instructions),
        )
    return None


def _validate_aggregate_distribution_proxy(
    *,
    sql: str,
    user_query: str,
    state: DashboardChatGraphState,
    referenced_tables: list[str],
) -> DashboardChatSqlVerificationResult | None:
    if not (STAGE_TO_STAGE_PATTERN.search(user_query) and GROWTH_OR_CHANGE_PATTERN.search(user_query)):
        return None
    physical_tables = [table for table in referenced_tables if "." in table]
    if not physical_tables:
        return None
    if not _all_tables_are_aggregate(state, physical_tables):
        return None
    sql_lower = sql.lower()
    uses_distribution_proxy = bool(
        re.search(r"\b[a-z0-9_]*(?:level|status|bucket|band|category)[a-z0-9_]*\b", sql_lower)
        and re.search(r"\b[a-z0-9_]*(?:count|student_count|beneficiary_count|farmer_count)[a-z0-9_]*\b", sql_lower)
    )
    has_direct_metric_columns = bool(
        re.search(r"\b(perc|percent|percentage|score|mastery|rate|avg|average)[a-z0-9_]*(?:base|baseline|pre|end|endline|post)\b", sql_lower)
    )
    if not uses_distribution_proxy or has_direct_metric_columns:
        return None
    return DashboardChatSqlVerificationResult(
        is_valid=False,
        severity="repair_once",
        reason_code="aggregate_distribution_proxy_for_stage_growth",
        reasoning="The SQL answers a stage-to-stage growth question from aggregate distribution buckets/counts instead of direct paired-stage metric columns.",
        issues=[
            "Aggregate distribution tables can describe a chart-level shift, but they are a proxy for growth magnitude when lower-grain paired-stage metric tables are available.",
        ],
        repair_instructions=[
            "Search metadata for lower-grain or intermediate tables with paired baseline/endline metric columns and the requested dimensions.",
            "Use direct paired-stage measure columns for the growth calculation when available.",
            "Use the aggregate distribution table only if metadata shows it directly stores the requested growth metric.",
        ],
    )


def _all_tables_are_aggregate(state: DashboardChatGraphState, table_names: list[str]) -> bool:
    payload = state.get("metadata_artifact_payload") or {}
    try:
        artifact = DashboardChatMetadataArtifactPayload.model_validate(payload)
    except Exception:
        return all(".overall_" in table_name.lower() or ".agg" in table_name.lower() for table_name in table_names)
    lookup = table_lookup(artifact)
    normalized_lookup = {name.lower(): table for name, table in lookup.items()}
    for table_name in table_names:
        table = lookup.get(table_name) or normalized_lookup.get(table_name.lower())
        table_type = str(getattr(table, "table_type", "") or "").lower() if table is not None else ""
        if table_type != "aggregate" and ".overall_" not in table_name.lower():
            return False
    return True


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def _deduplicate_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduplicated: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduplicated.append(value)
    return deduplicated
