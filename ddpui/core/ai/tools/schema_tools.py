"""Schema-discovery tools: what data exists and what shape it has.

These tools return METADATA only (schema/table/column names and types) —
never row data. Actual values reach the model solely through profile_column
and execute_sql, so PII controls only have those two surfaces to cover."""

from langchain.tools import ToolRuntime, tool

from ddpui.core.ai.agent.context_builder import priority_sorted_schemas
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools import catalog
from ddpui.core.ai.tools.registry import register_tool

# Quick-scan window: enough to spot the real tables in a curated schema, small
# enough that a 140-table scratch schema doesn't flood the context. Largest
# tables first — real data dwarfs leftover scratch/test tables.
MAX_TABLES_LISTED = 20


@register_tool
@tool
def list_schemas(runtime: ToolRuntime[RunContext]) -> str:
    """List the warehouse schemas you may query. Always start here (or at
    list_tables) before writing SQL."""
    ctx = runtime.context
    if not ctx.allowed_schemas:
        return "No schemas are available for this organization."
    return "Available schemas (scan them in this order):\n" + "\n".join(
        priority_sorted_schemas(ctx.allowed_schemas)
    )


@register_tool
@tool
def list_tables(schema_name: str, runtime: ToolRuntime[RunContext]) -> str:
    """List a schema's tables (largest first, with approximate row counts).
    Shows at most 20 — if none of them match what the user asked for, ask the
    user for the exact table name rather than hunting through more schemas."""
    ctx = runtime.context
    try:
        tables = catalog.list_table_names(ctx, schema_name)
    except catalog.ToolInputError as err:
        return str(err)
    if not tables:
        return f"Schema '{schema_name}' has no tables."

    def size(approx) -> int:
        return int(approx) if approx is not None and approx >= 0 else -1

    entries = sorted(tables.items(), key=lambda kv: (-size(kv[1]), kv[0]))
    lines = [f"Tables in {schema_name} (largest first):"]
    for name, approx in entries[:MAX_TABLES_LISTED]:
        suffix = f" (~{int(approx)} rows)" if approx is not None and approx >= 0 else ""
        lines.append(f"{name}{suffix}")
    hidden = len(entries) - MAX_TABLES_LISTED
    if hidden > 0:
        lines.append(
            f"...and {hidden} more tables not shown. If none of the above match "
            "the question, ask the user for the exact table name instead of "
            "searching further."
        )
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

    details = f"Table {schema_name}.{table_name}\n\nColumns:\n" + "\n".join(col_lines)

    # Mixed-case identifiers (Airbyte raw tables like TLM26_StudentDetails)
    # fold to lowercase in postgres unless double-quoted — an unquoted
    # reference errors with "column does not exist" and burns a retry.
    if ctx.dialect == "postgres":
        mixed = [n for n in [table_name] + [c["name"] for c in columns] if n != n.lower()]
        if mixed:
            details += (
                "\n\nNOTE: these identifiers are case-sensitive and MUST be "
                'double-quoted in SQL, e.g. '
                f'{schema_name}."{table_name}" and "{mixed[-1]}": '
                + ", ".join(f'"{n}"' for n in mixed)
            )
    return details
