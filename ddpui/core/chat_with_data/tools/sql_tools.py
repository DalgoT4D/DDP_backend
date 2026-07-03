"""The execute_sql tool — the agent's only path to running SQL.

Every guard runs here: AST validation (SELECT-only, schema allowlist, LIMIT
clamp) and execution timeout. Warehouse errors come back as the tool's text so
the model can read them and self-correct.

Returns content_and_artifact: the content is a compact text rendering for the
LLM; the artifact is the structured result table the UI renders (columns/rows),
carried on the ToolMessage without ever entering the model's context.
"""

from langchain.tools import ToolRuntime, tool

from ddpui.core.chat_with_data.guards import sql_guard
from ddpui.core.chat_with_data.state import RunContext
from ddpui.core.chat_with_data.tools import common
from ddpui.core.chat_with_data.tools.registry import register_tool


@register_tool
@tool(response_format="content_and_artifact")
def execute_sql(sql: str, runtime: ToolRuntime[RunContext]) -> tuple[str, dict]:
    """Run ONE read-only SELECT against the warehouse. Table names must be
    schema-qualified (schema.table). If this returns an error, read it carefully,
    fix the SQL (re-check table details if needed), and try again."""
    ctx = runtime.context

    try:
        guarded = sql_guard.validate(
            sql,
            dialect=ctx.dialect,
            allowed_schemas=ctx.allowed_schemas,
            max_rows=ctx.max_result_rows,
        )
    except sql_guard.GuardError as err:
        return f"SQL rejected: {err}", {"sql": sql, "status": "rejected", "error": str(err)}

    try:
        rows = _execute_with_timeout(ctx, guarded.sql)
    except Exception as err:  # pylint: disable=broad-except
        # warehouse errors are the model's feedback loop — return, don't raise
        message = str(err).split("\n", maxsplit=1)[0][:500]
        return (
            f"Query failed: {message}",
            {"sql": guarded.sql, "status": "error", "error": message},
        )

    artifact = {
        "sql": guarded.sql,
        "status": "success",
        "row_count": len(rows),
        "columns": list(rows[0].keys()) if rows else [],
        "rows": [
            [common.truncate_cell(value) for value in row.values()]
            for row in rows[: ctx.max_result_rows]
        ],
    }
    content = f"Query returned {len(rows)} rows.\n" + common.render_rows(rows, ctx.max_result_rows)
    return content, artifact


def _execute_with_timeout(ctx: RunContext, sql: str) -> list[dict]:
    """Execute with a statement timeout where the dialect supports it.

    Postgres: SET statement_timeout on the same pooled connection, then query.
    BigQuery: no per-query timeout via the current client — the LIMIT clamp
    bounds result size; job-level timeout is a noted follow-up.
    """
    if ctx.dialect == "postgres" and hasattr(ctx.warehouse, "engine"):
        timeout_ms = int(ctx.query_timeout_s * 1000)
        with ctx.warehouse.engine.connect() as connection:
            connection.execute(f"SET statement_timeout = {timeout_ms}")
            result = connection.execute(sql)
            return [dict(row) for row in result.fetchall()]
    return ctx.warehouse.execute(sql)
