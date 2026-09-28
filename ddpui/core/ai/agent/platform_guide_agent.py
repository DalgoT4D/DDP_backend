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

from ddpui.core.ai.agent.chat_data_agent import get_chat_model
from ddpui.core.ai.agent.hitl import build_hitl_middleware
from ddpui.core.ai.agent.middleware import (
    clear_old_tool_results,
    trim_history,
)
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.prompts import build_guide_system_prompt
from ddpui.core.ai.tools.registry import get_tools
from ddpui.core.ai.toolsets import GUIDE_AGENT_TOOLS, GUIDE_APPROVAL_TOOLS


@dynamic_prompt
def guide_system_prompt(request) -> str:
    """System prompt rebuilt per model call from the run's org context."""
    return build_guide_system_prompt(request.runtime.context)


def build_guide_agent(
    checkpointer: BaseCheckpointSaver | None = None,
    model: BaseChatModel | None = None,
    human_in_the_loop: bool = True,
):
    """Compile the guide agent graph. Same contract as build_agent: `model`
    overridable for tests, `human_in_the_loop=False` for evals/REPL."""
    middleware = [
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
