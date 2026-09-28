"""The Chat with Data agent — model selection and its assembly.

Its system prompt lives in prompts.py; its toolbox in toolsets.py.

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
    clear_old_tool_results,
    sql_retry_limiter,
    trim_history,
)
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.constants import DEFAULT_MODEL, MODEL_ENV_VAR, MODEL_MAX_TOKENS, MODEL_OPTIONS
from ddpui.core.ai.prompts import build_system_prompt
from ddpui.core.ai.tools.registry import get_tools
from ddpui.core.ai.toolsets import SQL_AGENT_TOOLS, SQL_APPROVAL_TOOLS


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


@dynamic_prompt
def org_system_prompt(request) -> str:
    """System prompt rebuilt per model call from the run's org context."""
    return build_system_prompt(request.runtime.context)


def build_agent(
    checkpointer: BaseCheckpointSaver | None = None,
    model: BaseChatModel | None = None,
    human_in_the_loop: bool = True,
):
    """Compile the agent graph. `model` is overridable for tests and the REPL.

    `human_in_the_loop=False` disables the approval/clarification interrupts for
    contexts with no human to answer them (evals, REPL) — there ask_user falls
    back to its tool body and gated tools run without approval."""
    middleware = [
        sql_retry_limiter,  # must precede other before_model hooks: it can jump to end
        org_system_prompt,
        trim_history,
        clear_old_tool_results(),
    ]
    if human_in_the_loop:
        middleware.append(build_hitl_middleware(approval_tools=SQL_APPROVAL_TOOLS))
    return create_agent(
        model=model or get_chat_model(),
        tools=get_tools(names=SQL_AGENT_TOOLS),
        middleware=middleware,
        context_schema=RunContext,
        checkpointer=checkpointer,
    )
