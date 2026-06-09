"""SQL execution orchestration for the LLM tool loop."""

import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts.event_contracts import DashboardChatProgressStage
from ddpui.core.dashboard_chat.orchestration.state import DashboardChatGraphState
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_corrections import (
    missing_columns_in_primary_table,
    structured_sql_execution_error,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_validation import (
    find_missing_distinct_filters,
    validate_follow_up_dimension_usage,
    validate_sql_allowlist,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_verifier import (
    verify_sql_against_question,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
    get_turn_warehouse_tools,
    record_validated_filters_from_sql,
)
from ddpui.core.dashboard_chat.orchestration.pii_masking import mask_sql_rows_for_llm
from ddpui.core.dashboard_chat.orchestration.runtime_signals import (
    publish_runtime_progress,
    raise_if_runtime_cancelled,
)


MAX_SEMANTIC_VERIFIER_REJECTIONS = 2
VERIFIER_WARNING_SEVERITY = "warning"
VERIFIER_REPAIR_ONCE_SEVERITY = "repair_once"
VERIFIER_HARD_BLOCK_SEVERITY = "hard_block"


def handle_run_sql_query_tool(
    llm_client,
    warehouse_tools_factory,
    runtime_config,
    args: dict[str, Any],
    state: DashboardChatGraphState,
    turn_context: DashboardChatTurnContext,
) -> dict[str, Any]:
    """Validate SQL and let the tool loop self-correct on structured failures."""
    allowlist = DashboardChatAllowlist.model_validate(state.get("allowlist_payload") or {})
    sql = str(args.get("sql") or "").strip()
    if not sql:
        return {"error": "sql_missing", "message": "SQL is required"}

    allowlist_validation = validate_sql_allowlist(sql, allowlist)
    if not allowlist_validation["valid"]:
        return {
            "error": "table_not_allowed",
            "invalid_tables": allowlist_validation["invalid_tables"],
            "message": allowlist_validation["message"],
        }

    follow_up_dimension_validation = validate_follow_up_dimension_usage(
        warehouse_tools_factory,
        sql=sql,
        state=state,
        turn_context=turn_context,
    )
    if follow_up_dimension_validation is not None:
        return follow_up_dimension_validation

    missing_distinct = find_missing_distinct_filters(
        warehouse_tools_factory,
        sql,
        state,
        turn_context,
    )
    if missing_distinct:
        return {
            "error": "must_fetch_distinct_values",
            "missing": missing_distinct,
            "message": (
                "Call get_distinct_values for these columns, then regenerate the SQL using one of the returned values."
            ),
        }

    validation = DashboardChatSqlGuard(
        allowlist=allowlist,
        max_rows=runtime_config.max_query_rows,
    ).validate(sql)
    turn_context.last_sql_validation = validation
    if not validation.is_valid or not validation.sanitized_sql:
        return {
            "error": "sql_validation_failed",
            "issues": validation.errors,
            "warnings": validation.warnings,
        }

    missing_columns = missing_columns_in_primary_table(
        warehouse_tools_factory,
        sql=validation.sanitized_sql,
        state=state,
        turn_context=turn_context,
    )
    if missing_columns is not None:
        return missing_columns

    verification = verify_sql_against_question(
        llm_client,
        sql=validation.sanitized_sql,
        state=state,
    )
    if not verification.is_valid:
        severity = (verification.severity or VERIFIER_HARD_BLOCK_SEVERITY).strip().lower()
        reason_code = (verification.reason_code or "semantic_mismatch").strip().lower()
        if severity == VERIFIER_WARNING_SEVERITY:
            _record_verifier_warning(turn_context, verification.reasoning, verification.warnings)
        elif (
            severity == VERIFIER_REPAIR_ONCE_SEVERITY
            and reason_code in turn_context.repaired_reason_codes
        ):
            _record_verifier_warning(
                turn_context,
                verification.reasoning,
                [
                    "SQL verifier requested the same repair more than once; executing the "
                    "bounded-retry SQL and surfacing the semantic assumption.",
                    *verification.warnings,
                ],
            )
        elif (
            severity != VERIFIER_HARD_BLOCK_SEVERITY
            and turn_context.semantic_verifier_rejections >= MAX_SEMANTIC_VERIFIER_REJECTIONS
        ):
            _record_verifier_warning(
                turn_context,
                verification.reasoning,
                [
                    "SQL verifier retry budget was exhausted; executing the bounded-retry SQL "
                    "and surfacing the semantic assumption.",
                    *verification.warnings,
                ],
            )
        else:
            turn_context.semantic_verifier_rejections += 1
            if severity == VERIFIER_REPAIR_ONCE_SEVERITY:
                turn_context.repaired_reason_codes.add(reason_code)
            retry_message = (
                "Regenerate the SQL using the repair instructions."
                if severity == VERIFIER_REPAIR_ONCE_SEVERITY
                else (
                    "Choose a different table, grain, measure, time basis, or output shape. "
                    "Do not retry the same rejected SQL path."
                )
            )
            if turn_context.semantic_verifier_rejections >= MAX_SEMANTIC_VERIFIER_REJECTIONS:
                retry_message += (
                    " The semantic verifier retry budget is exhausted after this rejection; "
                    "make the next SQL attempt use a different concrete plan."
                )
            return {
                "error": "sql_question_mismatch",
                "severity": verification.severity,
                "reason_code": verification.reason_code,
                "issues": verification.issues,
                "repair_instructions": verification.repair_instructions,
                "reasoning": verification.reasoning,
                "risk_flags": verification.risk_flags,
                "warnings": verification.warnings,
                "semantic_rejections": turn_context.semantic_verifier_rejections,
                "message": (
                    "The generated SQL does not faithfully answer the question yet. "
                    f"{retry_message}"
                ),
            }

    if verification.is_valid and verification.severity == VERIFIER_WARNING_SEVERITY:
        _record_verifier_warning(turn_context, verification.reasoning, verification.warnings)

    turn_context.last_sql = validation.sanitized_sql
    table_label = ", ".join(validation.tables[:2]) if validation.tables else "allowlisted table"
    publish_runtime_progress(
        f"Querying data from {table_label}",
        DashboardChatProgressStage.QUERYING_DATA,
    )
    raise_if_runtime_cancelled()
    try:
        rows = get_turn_warehouse_tools(
            warehouse_tools_factory,
            turn_context,
            state,
        ).execute_sql(validation.sanitized_sql)
    except Exception as error:
        structured_error = structured_sql_execution_error(
            warehouse_tools_factory,
            sql=validation.sanitized_sql,
            error=error,
            state=state,
            turn_context=turn_context,
        )
        if structured_error is not None:
            return structured_error
        return {
            "success": False,
            "error": str(error),
            "sql_used": validation.sanitized_sql,
        }

    serialized_rows = json.loads(json.dumps(rows, cls=DjangoJSONEncoder))
    masked_rows = mask_sql_rows_for_llm(
        state=state,
        turn_context=turn_context,
        sql=validation.sanitized_sql,
        rows=serialized_rows,
    )
    turn_context.last_sql_results = masked_rows
    record_validated_filters_from_sql(
        turn_context=turn_context,
        sql=validation.sanitized_sql,
    )
    return {
        "success": True,
        "row_count": len(masked_rows),
        "error": None,
        "sql_used": validation.sanitized_sql,
        "columns": list(masked_rows[0].keys()) if masked_rows else [],
        "rows": masked_rows,
    }


def _record_verifier_warning(
    turn_context: DashboardChatTurnContext,
    reasoning: str,
    warnings: list[str],
) -> None:
    """Expose non-blocking semantic verifier concerns to final answer composition."""
    for warning in [reasoning, *warnings]:
        normalized_warning = str(warning or "").strip()
        if normalized_warning and normalized_warning not in turn_context.warnings:
            turn_context.warnings.append(normalized_warning)
