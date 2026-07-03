"""Schema-discovery tools: what data exists and what shape it has."""

from langchain.tools import ToolRuntime, tool

from ddpui.core.chat_with_data.state import RunContext
from ddpui.core.chat_with_data.tools import common
from ddpui.core.chat_with_data.tools.registry import register_tool

# Sample rows shown per table in get_table_details — enough to convey shape, not data
SAMPLE_ROW_COUNT = 3


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
        tables = common.list_table_names(ctx, schema_name)
    except common.ToolInputError as err:
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
    """Get a table's columns with types, plus a few sample rows. Use this before
    writing SQL against the table — column names must match exactly."""
    ctx = runtime.context
    try:
        common.check_table(ctx, schema_name, table_name)
    except common.ToolInputError as err:
        return str(err)

    columns = ctx.warehouse.get_table_columns(schema_name, table_name)
    col_lines = [f"{col['name']}: {col['data_type']}" for col in columns]

    qualified = common.qualified(ctx.dialect, schema_name, table_name)
    try:
        sample_rows = ctx.warehouse.execute(f"SELECT * FROM {qualified} LIMIT {SAMPLE_ROW_COUNT}")
        sample = common.render_rows(sample_rows, SAMPLE_ROW_COUNT)
    except Exception:  # pylint: disable=broad-except
        sample = "(samples unavailable)"

    return (
        f"Table {schema_name}.{table_name}\n\nColumns:\n"
        + "\n".join(col_lines)
        + f"\n\nSample rows:\n{sample}"
    )
