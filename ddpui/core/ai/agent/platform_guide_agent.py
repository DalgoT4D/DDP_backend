"""The Platform Guide agent — creates and explains Dalgo platform objects.

The second agent in the TurnGraph (route intent "platform_help"). Where the
SQL agent answers questions FROM the org's data, this agent works ON the
platform itself: it creates charts, dashboards, KPIs, metrics, and reports
in-chat (behind the same approval cards), guides the user through object
dependencies (a KPI is built on a metric; a report is a snapshot of a
dashboard), and points to the docs.dalgo.org page for every feature it
touches. It has NO data-querying tools — execute_sql and profile_column
stay with the SQL agent.

Same assembly pattern as chat_data_agent.build_agent: create_agent + the
shared middleware stack, minus sql_retry_limiter (no SQL to retry).
"""

from langchain.agents import create_agent
from langchain.agents.middleware import dynamic_prompt
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from ddpui.core.ai.agent.hitl import build_hitl_middleware
from ddpui.core.ai.agent.middleware import clear_old_tool_results, trim_history
from ddpui.core.ai.agent.pii import build_pii_middleware
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.tools.registry import get_tools

# The guide agent's toolbox: docs + inventory + discovery (for real column
# names during chart/metric creation) + the creation tools + ask_user.
GUIDE_AGENT_TOOLS = (
    "get_dalgo_help",
    "list_metrics",
    "list_kpis",
    "list_charts",
    "list_reports",
    "list_schemas",
    "list_tables",
    "get_table_details",
    "list_dashboards",
    "create_chart",
    "create_dashboard",
    "add_charts_to_dashboard",
    "create_metric",
    "create_kpi",
    "create_report",
    "ask_user",
)

# Creation tools pause for user approval (same cards as the SQL agent's)
GUIDE_APPROVAL_TOOLS = (
    "create_chart",
    "create_dashboard",
    "add_charts_to_dashboard",
    "create_metric",
    "create_kpi",
    "create_report",
)


def build_guide_system_prompt(ctx: RunContext) -> str:
    """Operating instructions for platform guidance and creation."""
    return f"""You are Dalgo's platform guide. You help NGO staff use Dalgo's \
features — charts, dashboards, KPIs, metrics, and reports — by explaining how \
they work and by creating them in-chat when asked. Your users are program \
managers, not engineers.

## How Dalgo's objects fit together
- A **metric** is a saved calculation over a warehouse table (e.g. "count of \
surveys"). Metrics are the building blocks.
- A **KPI** is a metric promoted with a target, direction, and red/amber/green \
thresholds. A KPI ALWAYS needs a metric first.
- A **chart** is a visualization of a table's columns (bar, line, pie, number).
- A **dashboard** is a collection of charts arranged on a page.
- A **report** is a frozen snapshot of a dashboard for a date range — it needs \
an existing dashboard.

## How to work
1. ALWAYS check what already exists before creating: list_metrics before a \
metric or KPI, list_charts and list_dashboards before dashboard work, \
list_reports before a report. Reuse before recreating.
2. Respect the dependencies. If the user wants a KPI and no suitable metric \
exists, say so and offer to create the metric first, then the KPI on it. If \
they want a report, ask which dashboard it should snapshot (name their \
dashboards from list_dashboards).
3. For charts and metrics you need REAL column names — verify with \
get_table_details first. Never guess a column name.
4. Creating anything waits for the user's approval card in the chat. If the \
user cancels, do not retry the same action — ask what they'd prefer.
5. When explaining HOW to do something in the Dalgo interface, read the \
relevant page with get_dalgo_help first and give the steps using the exact \
button and menu names from the docs.
6. If the user's request is ambiguous, use ask_user to ask ONE short question.
7. If the user asks a question about their data itself (counts, trends, \
comparisons), tell them to ask it directly — the data assistant handles those.

## How to answer
- Lead with what you did or the direct answer, in one or two sentences.
- For step-by-step guidance use a short numbered list with the exact UI \
labels in **bold** (e.g. 1. Select **Charts** in the left menu).
- End guidance answers with the docs link on its own line: \
"Read more: <url from get_dalgo_help>".
- Formatting allowed: **bold**, "- " bullets, "1." numbered lists, "### " \
headings, plain URLs. No code blocks, no markdown tables.
- Use the user's language and terms. No jargon.
"""


@dynamic_prompt
def guide_system_prompt(request) -> str:
    """System prompt rebuilt per model call from the run's org context."""
    return build_guide_system_prompt(request.runtime.context)


def build_guide_agent(
    checkpointer: BaseCheckpointSaver | None = None,
    model: BaseChatModel | None = None,
    human_in_the_loop: bool = True,
    pii_rules: list[dict] | None = None,
):
    """Compile the guide agent graph. Same contract as build_agent: `model`
    overridable for tests, `human_in_the_loop=False` for evals/REPL, org
    `pii_rules` layered over the default PII middleware."""
    from ddpui.core.ai.agent.chat_data_agent import get_chat_model

    middleware = [
        *build_pii_middleware(pii_rules),
        guide_system_prompt,
        trim_history,
        clear_old_tool_results(),
    ]
    if human_in_the_loop:
        middleware.append(build_hitl_middleware(approval_tools=GUIDE_APPROVAL_TOOLS))
    return create_agent(
        model=model or get_chat_model(),
        tools=get_tools(names=GUIDE_AGENT_TOOLS),
        middleware=middleware,
        context_schema=RunContext,
        checkpointer=checkpointer,
    )
