"""Shared helpers for Chat with Data tools.

Identifier safety model: schema names are validated against the server-derived
allowlist, and table names against the live catalog, BEFORE they are ever
interpolated into introspection SQL. A name the warehouse doesn't already know
never reaches a query string.
"""

from ddpui.core.chat_with_data.state import RunContext

# Cap on characters per cell when rendering results/samples for the LLM
MAX_CELL_CHARS = 120


class ToolInputError(Exception):
    """Invalid tool input (bad schema/table/column). Message is LLM-readable."""


def check_schema(ctx: RunContext, schema: str) -> None:
    if schema not in ctx.allowed_schemas:
        raise ToolInputError(
            f"Schema '{schema}' is not available. Available schemas: {sorted(ctx.allowed_schemas)}"
        )


def list_table_names(ctx: RunContext, schema: str) -> dict[str, int | None]:
    """{table_name: approx_rows} for a validated schema, via dialect catalog SQL."""
    check_schema(ctx, schema)
    if ctx.dialect == "bigquery":
        sql = f"SELECT table_id AS table_name, row_count AS approx_rows FROM `{schema}.__TABLES__`"
    else:
        sql = (
            "SELECT c.relname AS table_name, c.reltuples::bigint AS approx_rows "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = '{schema}' AND c.relkind IN ('r', 'v', 'm', 'p') "
            "ORDER BY 1"
        )
    rows = ctx.warehouse.execute(sql)
    return {row["table_name"]: row.get("approx_rows") for row in rows}


def check_table(ctx: RunContext, schema: str, table: str) -> None:
    """Validate table by membership in the live catalog (also validates schema)."""
    tables = list_table_names(ctx, schema)
    if table not in tables:
        raise ToolInputError(
            f"Table '{table}' not found in schema '{schema}'. "
            f"Tables there: {sorted(tables)[:50]}"
        )


def qualified(dialect: str, schema: str, table: str) -> str:
    """Quote a validated schema.table for the dialect."""
    if dialect == "bigquery":
        return f"`{schema}.{table}`"
    return f'"{schema}"."{table}"'


def truncate_cell(value) -> str:
    text = "" if value is None else str(value)
    if len(text) > MAX_CELL_CHARS:
        return text[: MAX_CELL_CHARS - 1] + "…"
    return text


def render_rows(rows: list[dict], max_rows: int) -> str:
    """Compact pipe-separated rendering of query rows for the LLM."""
    if not rows:
        return "(no rows)"
    shown = rows[:max_rows]
    columns = list(shown[0].keys())
    lines = [" | ".join(columns)]
    for row in shown:
        lines.append(" | ".join(truncate_cell(row.get(col)) for col in columns))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows not shown)")
    return "\n".join(lines)
