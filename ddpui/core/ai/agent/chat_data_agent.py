"""The Chat with Data agent — its system prompt and its assembly.

One model node bound to the tool registry, one ToolNode, loop until the model
stops calling tools (LangGraph's prebuilt agent loop). All customization is
middleware + context; the topology is never modified (spec §4). Each AI feature
gets a module like this one under agent/ — the loop infrastructure it shares
lives in middleware.py / run_context.py / checkpointer.py.
"""

import os

from langchain.agents import create_agent
from langchain.agents.middleware import dynamic_prompt
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from ddpui.core.ai.agent.base import build_model_by_id, resolve_model_name
from ddpui.core.ai.agent.hitl import build_hitl_middleware
from ddpui.core.ai.agent.middleware import (
    MAX_SQL_ATTEMPTS,
    clear_old_tool_results,
    sql_retry_limiter,
    trim_history,
)
from ddpui.core.ai.agent.pii import build_pii_middleware
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import get_tools

# Upper bound on GRAPH STEPS per turn — backstop against runaway loops.
# Every middleware hook is its own graph node, so one model⇄tool cycle costs
# ~12 steps with the current stack (7 before_model hooks incl. 5 PII rules +
# model + 3 after_model + tools) — plus one more per org-defined PII rule.
# 120 ≈ headroom for ~10 tool calls; a legitimate heavy turn uses ~9
# (schemas → tables → details ×2 → profile ×2 → sql ×2 → chart). The real
# runaway guard is sql_retry_limiter (3 failed queries), not this ceiling.
# If you add middleware, re-check test_realistic_discovery_turn_fits_in_the_recursion_limit.
RECURSION_LIMIT = 120

# Max tokens per model response; answers are short prose + small tables
MODEL_MAX_TOKENS = 4096

MODEL_ENV_VAR = "CHAT_WITH_DATA_MODEL"
DEFAULT_MODEL = "claude-sonnet-5"

# Models a user may pick in the chat UI. Only entries whose provider key is
# present in the environment are offered (credentials move to org settings
# later). The id doubles as the init_chat_model spec — provider inferred.
MODEL_OPTIONS = [
    {"id": "claude-sonnet-5", "label": "Claude Sonnet", "key_env": "ANTHROPIC_API_KEY"},
    {"id": "gpt-5.5", "label": "OpenAI GPT", "key_env": "OPENAI_API_KEY"},
]

_DIALECT_LABELS = {"postgres": "PostgreSQL", "bigquery": "BigQuery"}


def available_models() -> list[dict]:
    """User-selectable models whose provider credentials exist, as {id, label}."""
    return [
        {"id": option["id"], "label": option["label"]}
        for option in MODEL_OPTIONS
        if os.getenv(option["key_env"])
    ]


def default_model_id() -> str:
    """The model used when the user picks nothing: the env override if it is
    offerable, else the first available option, else the hard default."""
    configured = resolve_model_name(MODEL_ENV_VAR, DEFAULT_MODEL)
    offered = [m["id"] for m in available_models()]
    if configured in offered or not offered:
        return configured
    return offered[0]


def resolve_selected_model(model_id: str | None) -> str:
    """Validate a user-supplied model id against the allowlist; None or an
    unknown/unavailable id falls back to the default. Never trusts the client."""
    if model_id and any(m["id"] == model_id for m in available_models()):
        return model_id
    return default_model_id()


def get_chat_model(model_id: str | None = None) -> BaseChatModel:
    """The production chat model, optionally the user's selected one."""
    return build_model_by_id(resolve_selected_model(model_id), MODEL_MAX_TOKENS)


def build_system_prompt(ctx: RunContext) -> str:
    """The agent's operating instructions, specialized to the org's warehouse."""
    dialect_label = _DIALECT_LABELS.get(ctx.dialect, ctx.dialect)
    schemas = ", ".join(sorted(ctx.allowed_schemas)) or "(none)"

    return f"""You are Dalgo's data assistant. You answer questions from NGO staff about \
their organization's data by querying their {dialect_label} warehouse. Your users are \
program managers, not engineers — they know their programs deeply but do not know SQL.

## Your warehouse
- Dialect: {dialect_label}. Write SQL valid for this dialect only.
- Schemas you may query: {schemas}. Nothing else is accessible.
- Access is strictly read-only. Every query must be a single SELECT, and every table \
reference must be schema-qualified (schema.table).

## How to work
1. Discover before you write: use list_tables and get_table_details to learn exact \
table and column names. Never guess a column name.
2. Validate filter values: before filtering on a text column, use profile_column to \
see the real stored values (users say "Maharashtra"; the column may store "MH").
3. Query with execute_sql. Results are capped at {ctx.max_result_rows} rows — use \
aggregation (GROUP BY, COUNT, SUM) rather than fetching raw rows whenever possible.
4. If a query fails, read the error, fix your SQL (re-check table details if needed), \
and retry. After {MAX_SQL_ATTEMPTS} failed attempts, stop and explain simply what you \
tried and what the user could ask instead.
5. If you cannot find tables or columns matching what the user asked, or their \
question could mean two different things in a way that changes the answer (which \
table, which time period, which program), use ask_user to ask ONE short clarifying \
question instead of guessing or giving up. Their reply comes back as the tool result.
6. Running a query and creating charts or dashboards each wait for the user's \
approval in the chat. If the user cancels one, do not retry the same action — adjust \
your approach or ask what they would prefer.

## Creating charts
- When the user asks to chart, plot, graph, or visualize something, use \
create_chart to save a real chart in their chart library (types: bar, line, \
pie, number). Do not just describe what a chart would look like.
- Bar and line charts can plot several metrics at once (e.g. target vs \
achieved per state) — pass multiple entries in `metrics` instead of making \
separate charts for values that belong together.
- Verify the exact column names with get_table_details first, same as for a query.
- After creating it, tell the user the chart's name and that it is on their \
Charts page and can be added to a dashboard. If create_chart reports a \
permission problem, say so plainly and answer with a small table of numbers instead.

## Dashboards
- When the user wants a chart on a dashboard, FIRST call list_dashboards, then ASK: add it to one of their existing dashboards (name them) or create a new one? Do not pick for them. Act only after they choose.
- If they have no dashboards yet, say so and offer to create one.
- Create the chart(s) first (create_chart returns the chart id), then use create_dashboard or add_charts_to_dashboard with those ids.

## How to answer
- Lead with the headline: the direct answer in one or two sentences, with the key \
number(s) in **bold**. Write numbers with thousands separators (1,284).
- Scale the structure to the answer. A single fact stays a single sentence — no \
bullets, no headings. Use "- " bullets for breakdowns of 3 or more items \
(one item per line, the number in **bold**). For long answers covering several \
topics, add a short "### " heading line before each topic.
- If there is ONE finding the user must not miss (a spike, a sudden drop, a data \
gap), put it on its own line starting with "> " — the chat shows it as a \
highlighted callout. At most one per answer; skip it for routine answers.
- End with one short line on how you got the answer (which table, what filter).
- Mention data caveats only when they change the interpretation (e.g. "3 rows have \
no district recorded").
- Never invent data. If the tables can't answer the question, say so plainly and \
suggest the closest answerable question.
- Use the user's language and terms. No SQL jargon in the answer itself.
- Formatting allowed: **bold**, "- " bullets, "1." numbered lists, "### " headings, \
"> " callouts. NOTHING else — no code blocks, no links, no markdown tables (query \
results already appear as a real table below your answer, so never repeat them).
"""


@dynamic_prompt
def org_system_prompt(request) -> str:
    """System prompt rebuilt per model call from the run's org context."""
    return build_system_prompt(request.runtime.context)


def build_agent(
    checkpointer: BaseCheckpointSaver | None = None,
    model: BaseChatModel | None = None,
    human_in_the_loop: bool = True,
    pii_rules: list[dict] | None = None,
):
    """Compile the agent graph. `model` is overridable for tests and the REPL.

    `human_in_the_loop=False` disables the approval/clarification interrupts for
    contexts with no human to answer them (evals, REPL) — there ask_user falls
    back to its tool body and gated tools run without approval.

    `pii_rules` are the org's extra PII detectors (RunContext.pii_rules),
    layered on top of the immovable defaults — see agent/pii.py."""
    middleware = [
        sql_retry_limiter,  # must precede other before_model hooks: it can jump to end
        *build_pii_middleware(pii_rules),  # mask PII before anything downstream sees it
        org_system_prompt,
        trim_history,
        clear_old_tool_results(),
    ]
    if human_in_the_loop:
        middleware.append(build_hitl_middleware())
    return create_agent(
        model=model or get_chat_model(),
        tools=get_tools(),
        middleware=middleware,
        context_schema=RunContext,
        checkpointer=checkpointer,
    )
