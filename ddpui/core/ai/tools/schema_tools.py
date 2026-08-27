"""Schema-discovery tools: what data exists and what shape it has.

These tools return METADATA only (schema/table/column names and types) —
never row data. Actual values reach the model solely through profile_column
and execute_sql, so PII controls only have those two surfaces to cover."""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools import catalog
from ddpui.core.ai.tools.registry import register_tool


@register_tool
@tool
def list_schemas(runtime: ToolRuntime[RunContext]) -> str:
    """List the warehouse schemas you may query. Always start here (or at
    list_tables) before writing SQL."""
    ctx = runtime.context
    if not ctx.allowed_schemas:
        return "No schemas are available for this organization."
    return "Available schemas:\n" + "\n".join(sorted(ctx.allowed_schemas))


@register_tool
@tool
def list_tables(schema_name: str, runtime: ToolRuntime[RunContext]) -> str:
    """List tables (with approximate row counts) in one schema."""
    ctx = runtime.context
    try:
        tables = catalog.list_table_names(ctx, schema_name)
    except catalog.ToolInputError as err:
        return str(err)
    if not tables:
        return f"Schema '{schema_name}' has no tables."
    lines = [f"Tables in {schema_name}:"]
    for name, approx in sorted(tables.items()):
        suffix = f" (~{int(approx)} rows)" if approx is not None and approx >= 0 else ""
        lines.append(f"{name}{suffix}")
    return "\n".join(lines)


@register_tool
@tool
def get_table_details(schema_name: str, table_name: str, runtime: ToolRuntime[RunContext]) -> str:
    """Get a table's columns with types. Use this before writing SQL against
    the table — column names must match exactly. To learn what values a text
    column holds before filtering on it, use profile_column."""
    ctx = runtime.context
    try:
        catalog.check_table(ctx, schema_name, table_name)
    except catalog.ToolInputError as err:
        return str(err)

    columns = ctx.warehouse.get_table_columns(schema_name, table_name)
    col_lines = [f"{col['name']}: {col['data_type']}" for col in columns]

    return f"Table {schema_name}.{table_name}\n\nColumns:\n" + "\n".join(col_lines)
