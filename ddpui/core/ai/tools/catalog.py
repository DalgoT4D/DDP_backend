"""Identifier safety for warehouse tools.

Schema names are validated against the server-derived allowlist, and table
names against the live catalog, BEFORE they are ever interpolated into
introspection SQL. A name the warehouse doesn't already know never reaches a
query string.
"""

from ddpui.core.ai.agent.run_context import RunContext


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
    tables = {row["table_name"]: row.get("approx_rows") for row in rows}
    if ctx.allowed_tables is not None:
        # scoped session: discovery shows only the scope's tables, so the model
        # never plans SQL the guard would then reject. check_table() inherits
        # this filter, which also keeps get_table_details/profile_column sample
        # queries inside the scope.
        allowed = {ref.lower() for ref in ctx.allowed_tables}
        tables = {
            name: approx for name, approx in tables.items() if f"{schema}.{name}".lower() in allowed
        }
    return tables


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
