"""Human-in-the-loop pauses for the chat agent — config and wire translation.

Two kinds of pause ride one mechanism (HumanInTheLoopMiddleware + the
Postgres checkpointer, so a paused turn survives disconnects and restarts):

- approval — warehouse reads (execute_sql) and chart/dashboard writes wait
  for the user to approve or cancel the tool call before it runs.
- question — the ask_user tool is never executed; the interrupt carries the
  agent's question and the user's typed reply becomes the tool result (the
  middleware's "respond" decision).

This module owns both directions of the translation:
  interrupt payload (HITLRequest)  → WS `input_required` event
  user's decision                  → HITLResponse for Command(resume=...)
"""

import sqlglot
from langchain.agents.middleware import HumanInTheLoopMiddleware
from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.guards import pii_rewrite, sql_guard
from ddpui.core.ai.tools import catalog
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.chat_with_data")

# Tool calls that pause for user approval before executing. Metadata lookups
# (list_tables, get_table_details, profile_column) are deliberately NOT gated —
# gating them would cost several clicks before any question can be answered.
APPROVAL_TOOLS = (
    "execute_sql",
    "create_chart",
    "create_dashboard",
    "add_charts_to_dashboard",
)

# The clarification tool: respond-only, the human's answer IS the tool result
QUESTION_TOOL = "ask_user"

# Tools whose card carries a PII checkbox list: the only two that return real
# warehouse values to the model.
PII_REVIEW_TOOLS = ("execute_sql", "profile_column")


class _SyncHumanInTheLoopMiddleware(HumanInTheLoopMiddleware):
    """Python 3.10 shim so interrupt() works inside the async agent stream.

    On 3.10, langgraph's async runner never enters the runnable config context
    (asyncio.create_task(context=...) needs 3.11), so `interrupt()` inside the
    hook dies with "Called get_config outside of a runnable context". Two-part
    fix: (1) un-override `aafter_model`, making the agent factory wrap the hook
    sync-only (RunnableCallable.ainvoke falls back to invoke() when afunc is
    None); (2) declare a `config` parameter — RunnableCallable injects the
    task's config into it — and set the contextvar interrupt() reads ourselves.
    Safe to delete once the deployment moves to Python >= 3.11."""

    aafter_model = AgentMiddleware.aafter_model

    def after_model(self, state, runtime, config: RunnableConfig | None = None):
        token = var_child_runnable_config.set(config) if config is not None else None
        try:
            return super().after_model(state, runtime)
        finally:
            if token is not None:
                var_child_runnable_config.reset(token)


def build_hitl_middleware(
    approval_tools: tuple[str, ...] = APPROVAL_TOOLS,
) -> HumanInTheLoopMiddleware:
    """One middleware gating the approval tools and the ask_user tool.

    Each agent passes its own `approval_tools` set (the SQL agent gates
    execute_sql; the platform guide agent gates its creation tools)."""
    interrupt_on: dict = {
        name: {"allowed_decisions": ["approve", "reject"]} for name in approval_tools
    }
    interrupt_on[QUESTION_TOOL] = {"allowed_decisions": ["respond"]}
    return _SyncHumanInTheLoopMiddleware(
        interrupt_on=interrupt_on,
        description_prefix="Waiting for your go-ahead",
    )


def _card_columns(sql: str, ctx: RunContext) -> tuple[list[dict] | None, str]:
    """The card's checkbox list, or (None, reason) when it cannot be built.

    Fail-closed on purpose: the tool builds its own schema map when it runs, so a
    transient catalog error here must not produce an empty card that the user
    approves and the tool then executes unhashed."""
    try:
        tree = sqlglot.parse_one(sql, dialect=ctx.dialect)
        schema_map = catalog.schema_map_for(ctx, sql_guard.referenced_tables(tree))
        resolved = pii_rewrite.resolve_projection(sql, ctx.dialect, schema_map)
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"could not build the PII column list for the approval card: {err}")
        return None, str(err)
    return [
        {
            "schema": column.schema,
            "table": column.table,
            "column": column.column,
            "has_literal": column.has_literal,
        }
        for column in resolved
    ], ""


def _reviewable_sql(request: dict, ctx: RunContext) -> str:
    """The SQL whose columns the card reviews. execute_sql carries it as an
    argument; profile_column names a single column, so synthesize the
    equivalent SELECT rather than duplicating the tool's query-building."""
    args = request.get("args", {})
    if request["name"] == "execute_sql":
        return str(args.get("sql", ""))
    column = args.get("column_name")
    quoted = f"`{column}`" if ctx.dialect == "bigquery" else f'"{column}"'
    qualified = catalog.qualified(ctx.dialect, args.get("schema_name"), args.get("table_name"))
    return f"SELECT {quoted} FROM {qualified}"


def input_required_event(interrupt_value: dict, ctx: RunContext) -> dict:
    """Translate a HITLRequest interrupt payload into the WS event the UI renders.

    kind="question" when the pause is a lone ask_user call (the UI shows the
    question as a normal assistant message and the composer answers it);
    kind="approval" otherwise (the UI shows approve/cancel cards). Requests for a
    value-returning tool also carry the PII checkbox list."""
    requests = []
    for request in interrupt_value.get("action_requests", []):
        sql = request.get("args", {}).get("sql")
        entry = {
            "tool": request["name"],
            "args": request.get("args", {}),
            "description": request.get("description", ""),
            "sql": sql,
        }
        if request["name"] in PII_REVIEW_TOOLS:
            columns, error = _card_columns(_reviewable_sql(request, ctx), ctx)
            entry["columns"] = columns
            if columns is None:
                entry["columns_error"] = error
        requests.append(entry)

    if len(requests) == 1 and requests[0]["tool"] == QUESTION_TOOL:
        return {
            "type": "input_required",
            "kind": "question",
            "question": str(requests[0]["args"].get("question", "")),
            "requests": requests,
        }
    return {"type": "input_required", "kind": "approval", "requests": requests}


def build_resume_payload(requests: list[dict], approve: bool, answer: str | None = None) -> dict:
    """The HITLResponse for Command(resume=...): one decision per pending request,
    in order. ask_user requests always get a respond decision (their only allowed
    one); everything else gets approve/reject."""
    decisions = []
    for request in requests:
        if request.get("tool") == QUESTION_TOOL:
            decisions.append({"type": "respond", "message": answer or "(the user did not answer)"})
        elif approve:
            decisions.append({"type": "approve"})
        else:
            decisions.append({"type": "reject"})
    return {"decisions": decisions}
