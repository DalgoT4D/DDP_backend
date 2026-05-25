"""Focused regression tests for dashboard-chat SQL identifier handling."""

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.contracts.retrieval_contracts import DashboardChatSchemaSnippet
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.schema_tools import (
    handle_get_distinct_values_tool,
    handle_get_schema_snippets_tool,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_corrections import (
    missing_columns_in_primary_table,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.implementations.sql_parsing import (
    referenced_sql_identifier_refs,
    table_references,
)
from ddpui.core.dashboard_chat.orchestration.llm_tools.runtime.turn_context import (
    DashboardChatTurnContext,
)


def _schema_snippet(table_name: str, columns: list[dict]) -> DashboardChatSchemaSnippet:
    """Build one schema snippet payload for unit tests."""
    return DashboardChatSchemaSnippet(table_name=table_name, columns=columns)


def _build_turn_context(
    *,
    schema_snippets_by_table=None,
    warehouse_tools=None,
) -> DashboardChatTurnContext:
    """Build the minimal turn context required by tool handlers."""
    return DashboardChatTurnContext(
        validated_distinct_values=set(),
        schema_snippets_by_table=dict(schema_snippets_by_table or {}),
        warnings=[],
        warehouse_tools=warehouse_tools,
        timing_breakdown={"tool_calls_ms": []},
    )


def _build_state(allowlist: DashboardChatAllowlist) -> dict:
    """Build the minimal graph state payload required by tool handlers."""
    return {"allowlist_payload": allowlist.model_dump(mode="json")}


class MixedCaseWarehouseTools:
    """Warehouse stub that preserves mixed-case Postgres physical table names."""

    def __init__(self):
        self.schema_requests = []
        self.distinct_requests = []
        self.schemas = {
            "dev_prod.overall_RC_analysis_levels_2525": _schema_snippet(
                "dev_prod.overall_RC_analysis_levels_2525",
                [
                    {"name": "grade", "data_type": "text", "nullable": False},
                    {"name": "rc_level", "data_type": "text", "nullable": False},
                    {"name": "student_count_mid", "data_type": "integer", "nullable": False},
                ],
            )
        }

    def get_schema_snippets(self, tables):
        self.schema_requests.append(list(tables))
        return {
            table_name: self.schemas[table_name]
            for table_name in tables
            if table_name in self.schemas
        }

    def get_distinct_values(self, table_name, column_name, limit=50):
        self.distinct_requests.append((table_name, column_name, limit))
        if (
            table_name == "dev_prod.overall_RC_analysis_levels_2525"
            and column_name == "rc_level"
        ):
            return ["Emergent", "Developing"]
        return []


def test_sql_identifier_parser_ignores_is_not_null_and_where_keyword():
    """Valid IS NOT NULL filters should not create fake identifiers or aliases."""
    sql = (
        "SELECT COUNT(DISTINCT fellow_name) AS number_of_fellows "
        "FROM dev_prod.student_distribution_2425 "
        "WHERE student_count_base IS NOT NULL AND fellow_name IS NOT NULL"
    )

    assert referenced_sql_identifier_refs(sql) == [
        (None, "fellow_name"),
        (None, "student_count_base"),
    ]
    assert table_references(sql) == [
        {
            "table_name": "dev_prod.student_distribution_2425",
            "alias": None,
            "short_name": "student_distribution_2425",
        }
    ]


def test_missing_columns_check_allows_valid_is_not_null_query():
    """A valid COUNT DISTINCT query should not be rejected with column_not_in_table."""
    table_name = "dev_prod.student_distribution_2425"
    state = _build_state(DashboardChatAllowlist(allowed_tables={table_name}))
    turn_context = _build_turn_context(
        schema_snippets_by_table={
            table_name: _schema_snippet(
                table_name,
                [
                    {"name": "fellow_name", "data_type": "text", "nullable": True},
                    {"name": "student_count_base", "data_type": "integer", "nullable": True},
                ],
            )
        }
    )

    missing = missing_columns_in_primary_table(
        lambda org: None,
        sql=(
            "SELECT COUNT(DISTINCT fellow_name) AS number_of_fellows "
            "FROM dev_prod.student_distribution_2425 "
            "WHERE student_count_base IS NOT NULL AND fellow_name IS NOT NULL"
        ),
        state=state,
        turn_context=turn_context,
    )

    assert missing is None


def test_schema_tools_preserve_mixed_case_table_names():
    """Schema and distinct-value tool handlers should return canonical mixed-case table names."""
    fake_warehouse = MixedCaseWarehouseTools()
    table_name = "dev_prod.overall_RC_analysis_levels_2525"
    state = _build_state(
        DashboardChatAllowlist(
            allowed_tables={table_name},
            chart_tables={table_name},
        )
    )
    turn_context = _build_turn_context(warehouse_tools=fake_warehouse)

    schema_result = handle_get_schema_snippets_tool(
        lambda org: fake_warehouse,
        {"tables": ["dev_prod.overall_rc_analysis_levels_2525"]},
        state,
        turn_context,
    )
    distinct_result = handle_get_distinct_values_tool(
        lambda org: fake_warehouse,
        {
            "table": "dev_prod.overall_rc_analysis_levels_2525",
            "column": "rc_level",
            "limit": 20,
        },
        state,
        turn_context,
    )
    assert fake_warehouse.schema_requests[0] == [table_name]
    assert schema_result["tables"][0]["table"] == table_name
    assert distinct_result["table"] == table_name
    assert fake_warehouse.distinct_requests[0][0] == table_name


def test_sql_identifier_parser_ignores_schema_table_refs_inside_subqueries():
    """Schema-qualified tables inside nested SELECTs should not be treated as column refs."""
    sql = """
        SELECT pm_name_mid, COUNT(DISTINCT student_id) AS students_below_grade_level
        FROM dev_intermediate.base_mid_end_comb_students_2425_dim
        WHERE student_id IN (
            SELECT student_id
            FROM dev_intermediate.base_mid_end_comb_scores_2425_fct
            WHERE rc_level_mid IN ('0', '0.2', '0.5')
        )
        GROUP BY pm_name_mid
        ORDER BY students_below_grade_level DESC
        LIMIT 1
    """

    assert referenced_sql_identifier_refs(sql) == [
        (None, "pm_name_mid"),
        (None, "student_id"),
    ]


def test_missing_columns_check_allows_cte_aliases_and_subquery_outputs():
    """Valid CTE-derived aliases should not be treated as missing physical columns."""
    table_name = "dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25"
    state = _build_state(DashboardChatAllowlist(allowed_tables={table_name}))
    turn_context = _build_turn_context(
        schema_snippets_by_table={
            table_name: _schema_snippet(
                table_name,
                [
                    {"name": "date_time", "data_type": "timestamp", "nullable": True},
                    {"name": "silt_achieved", "data_type": "numeric", "nullable": True},
                ],
            )
        }
    )

    sql = """
        WITH latest AS (
            SELECT MAX(date_time) AS max_date_time
            FROM dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25
        )
        SELECT SUM(w.silt_achieved) AS total_silt_achieved
        FROM dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25 w
        WHERE w.date_time = (SELECT max_date_time FROM latest)
    """

    assert referenced_sql_identifier_refs(sql) == [
        ("w", "silt_achieved"),
        ("w", "date_time"),
    ]
    assert missing_columns_in_primary_table(
        lambda org: None,
        sql=sql,
        state=state,
        turn_context=turn_context,
    ) is None


def test_missing_columns_check_allows_outer_query_filters_on_cte_output_columns():
    """Outer-query filters on CTE aliases like rn should not trigger false column errors."""
    base_table = "analytics.status_updates"
    state = _build_state(DashboardChatAllowlist(allowed_tables={base_table}))
    turn_context = _build_turn_context(
        schema_snippets_by_table={
            base_table: _schema_snippet(
                base_table,
                [
                    {"name": "district_name", "data_type": "text", "nullable": False},
                    {"name": "updated_at", "data_type": "timestamp", "nullable": False},
                ],
            )
        }
    )

    sql = """
        WITH ranked AS (
            SELECT
                district_name,
                ROW_NUMBER() OVER (
                    PARTITION BY district_name
                    ORDER BY updated_at DESC
                ) AS rn
            FROM analytics.status_updates
        )
        SELECT district_name
        FROM ranked
        WHERE rn = 1
    """

    assert referenced_sql_identifier_refs(sql) == [
        (None, "district_name"),
        (None, "rn"),
    ]
    assert missing_columns_in_primary_table(
        lambda org: None,
        sql=sql,
        state=state,
        turn_context=turn_context,
    ) is None
