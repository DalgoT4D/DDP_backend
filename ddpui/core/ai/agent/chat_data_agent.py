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
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from ddpui.core.ai.agent.middleware import (
    MAX_SQL_ATTEMPTS,
    clear_old_tool_results,
    sql_retry_limiter,
    trim_history,
)
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import get_tools

# Upper bound on model⇄tool loop steps per turn — backstop against runaway loops
RECURSION_LIMIT = 25

# Max tokens per model response; answers are short prose + small tables
MODEL_MAX_TOKENS = 4096

DEFAULT_MODEL = "claude-sonnet-5"

_DIALECT_LABELS = {"postgres": "PostgreSQL", "bigquery": "BigQuery"}


def get_chat_model() -> ChatAnthropic:
    """The production model. Deployment-level key; no temperature (rejected by
    Claude Sonnet 5 / Opus 4.7+)."""
    return ChatAnthropic(
        model=os.getenv("CHAT_WITH_DATA_MODEL", DEFAULT_MODEL),
        max_tokens=MODEL_MAX_TOKENS,
    )


def build_system_prompt(ctx: RunContext) -> str:
    """The agent's operating instructions, specialized to the org's warehouse
    and, for dashboard-scoped sessions, to that dashboard's tables."""
    dialect_label = _DIALECT_LABELS.get(ctx.dialect, ctx.dialect)
    schemas = ", ".join(sorted(ctx.allowed_schemas)) or "(none)"

    scope_section = ""
    if ctx.allowed_tables is not None:
        tables = "\n".join(f"- {ref}" for ref in sorted(ctx.allowed_tables))
        scope_section = f"""
## This chat is scoped to one dashboard
{ctx.scope_context}

You may query ONLY these tables (execute_sql rejects anything else):
{tables}

- Assume the user's questions are about this dashboard's data; use the chart \
titles and filters above to interpret their words.
- If a question needs data outside these tables, say so plainly and tell the \
user to open the full Chat with Data page — do not attempt to query other tables.
"""

    return f"""You are Dalgo's data assistant. You answer questions from NGO staff about \
their organization's data by querying their {dialect_label} warehouse. Your users are \
program managers, not engineers — they know their programs deeply but do not know SQL.

## Your warehouse
- Dialect: {dialect_label}. Write SQL valid for this dialect only.
- Schemas you may query: {schemas}. Nothing else is accessible.
- Access is strictly read-only. Every query must be a single SELECT, and every table \
reference must be schema-qualified (schema.table).
{scope_section}
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

## Creating charts
- When the user asks to chart, plot, graph, or visualize something, use \
create_chart to save a real chart in their chart library (types: bar, line, \
pie, number). Do not just describe what a chart would look like.
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
):
    """Compile the agent graph. `model` is overridable for tests and the REPL."""
    return create_agent(
        model=model or get_chat_model(),
        tools=get_tools(),
        middleware=[
            sql_retry_limiter,  # must precede other before_model hooks: it can jump to end
            org_system_prompt,
            trim_history,
            clear_old_tool_results(),
        ],
        context_schema=RunContext,
        checkpointer=checkpointer,
    )
