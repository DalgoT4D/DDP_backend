"""Builds the Chat with Data agent — LangGraph's prebuilt agent loop.

One model node bound to the tool registry, one ToolNode, loop until the model
stops calling tools. All customization is middleware + context; the topology is
never modified (spec §4).
"""

import os

from langchain.agents import create_agent
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.checkpoint.base import BaseCheckpointSaver

from ddpui.core.chat_with_data.middleware import (
    clear_old_tool_results,
    org_system_prompt,
    sql_retry_limiter,
    trim_history,
)
from ddpui.core.chat_with_data.state import RunContext
from ddpui.core.chat_with_data.tools.registry import get_tools

# Upper bound on model⇄tool loop steps per turn — backstop against runaway loops
RECURSION_LIMIT = 25

# Max tokens per model response; answers are short prose + small tables
MODEL_MAX_TOKENS = 4096

DEFAULT_MODEL = "claude-sonnet-5"


def get_chat_model() -> ChatAnthropic:
    """The production model. Deployment-level key; no temperature (rejected by
    Claude Sonnet 5 / Opus 4.7+)."""
    return ChatAnthropic(
        model=os.getenv("CHAT_WITH_DATA_MODEL", DEFAULT_MODEL),
        max_tokens=MODEL_MAX_TOKENS,
    )


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
