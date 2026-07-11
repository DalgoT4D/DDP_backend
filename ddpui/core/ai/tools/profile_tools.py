"""Column-profiling tool: check real filter values before writing SQL."""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools import common
from ddpui.core.ai.tools.registry import register_tool

# Distinct values returned — enough to catch 'MH' vs 'Maharashtra', small enough for context
TOP_VALUES_COUNT = 20


@register_tool
@tool
def profile_column(
    schema_name: str, table_name: str, column_name: str, runtime: ToolRuntime[RunContext]
) -> str:
    """See a column's most common distinct values. ALWAYS use this before filtering
    on a text column — the stored values often differ from what the user said
    (e.g. the user says 'Maharashtra' but the column stores 'MH')."""
    ctx = runtime.context
    try:
        common.check_table(ctx, schema_name, table_name)
    except common.ToolInputError as err:
        return str(err)

    if not ctx.warehouse.column_exists(schema_name, table_name, column_name):
        return f"Column '{column_name}' does not exist on {schema_name}.{table_name}. Use get_table_details to see columns."

    qualified = common.qualified(ctx.dialect, schema_name, table_name)
    quoted_col = f"`{column_name}`" if ctx.dialect == "bigquery" else f'"{column_name}"'
    sql = (
        f"SELECT {quoted_col} AS value, COUNT(*) AS occurrences FROM {qualified} "
        f"GROUP BY 1 ORDER BY 2 DESC LIMIT {TOP_VALUES_COUNT}"
    )
    rows = ctx.warehouse.execute(sql)
    if not rows:
        return f"Column {schema_name}.{table_name}.{column_name} has no values (empty table)."
    return f"Top values in {schema_name}.{table_name}.{column_name}:\n" + common.render_rows(
        rows, TOP_VALUES_COUNT
    )
