"""Column-profiling tool: find how a user's filter value is actually stored."""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools import catalog, rendering
from ddpui.core.ai.tools.registry import register_tool

MATCH_LIMIT = 10


def _safe(value: str) -> str:
    """Escape single quotes to prevent SQL injection in LIKE patterns."""
    return value.replace("'", "''")


@register_tool
@tool
def lookup_column_values(
    schema_name: str,
    table_name: str,
    column_name: str,
    search_value: str,
    runtime: ToolRuntime[RunContext],
) -> str:
    """Look up how a specific value is stored in a column before filtering on it.
    Use this when the user's value might differ from what is stored
    (e.g. user says 'Maharashtra' but the column stores 'MH').

    Pass the user's value as search_value — searches case-insensitively.
    If the values come back as long hex strings, the user has marked this column
    as personal data. Do not retry — continue without profiling it."""
    ctx = runtime.context
    try:
        catalog.check_table(ctx, schema_name, table_name)
    except catalog.ToolInputError as err:
        return str(err)

    if not ctx.warehouse.column_exists(schema_name, table_name, column_name):
        return (
            f"Column '{column_name}' does not exist on {schema_name}.{table_name}. "
            "Use get_table_details to see columns."
        )

    qualified = catalog.qualified(ctx.dialect, schema_name, table_name)
    quoted_col = f"`{column_name}`" if ctx.dialect == "bigquery" else f'"{column_name}"'

    if f"{schema_name}.{table_name}.{column_name}" in ctx.pii_columns:
        quoted_col = (
            f"TO_HEX(MD5(CAST({quoted_col} AS STRING)))"
            if ctx.dialect == "bigquery"
            else f"md5({quoted_col}::text)"
        )

    safe_value = _safe(search_value)
    if ctx.dialect == "bigquery":
        where = f"LOWER(CAST({quoted_col} AS STRING)) LIKE LOWER('%{safe_value}%')"
    else:
        # %% is psycopg2's escape for a literal % in a raw SQL string
        where = f"LOWER({quoted_col}::text) LIKE LOWER('%%{safe_value}%%')"

    sql = (
        f"SELECT DISTINCT {quoted_col} AS value FROM {qualified} WHERE {where} LIMIT {MATCH_LIMIT}"
    )
    rows = ctx.warehouse.execute(sql)
    if not rows:
        return (
            f"No values matching '{search_value}' found in "
            f"{schema_name}.{table_name}.{column_name}. "
            "The exact value may differ — ask the user to clarify or try a broader term."
        )
    return (
        f"Values matching '{search_value}' in {schema_name}.{table_name}.{column_name}:\n"
        + rendering.render_rows(rows, MATCH_LIMIT)
    )
