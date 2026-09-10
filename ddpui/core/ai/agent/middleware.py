"""Shared agent middleware: history trimming, tool-result clearing, SQL-retry
limiter, cross-agent tool-error repair.

These are the sanctioned customization points of the prebuilt agent loop — the
graph topology itself is never modified. Feature-specific middleware (like the
chat agent's dynamic system prompt) lives in that feature's agent module.
"""

import re

from langchain.agents.middleware import (
    ClearToolUsesEdit,
    ContextEditingMiddleware,
    before_model,
)
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.messages.utils import count_tokens_approximately, trim_messages

# Failed execute_sql calls allowed per user question before the loop is stopped;
# the system prompt tells the model the same number so it stops gracefully first.
# 5 (was 3): real warehouses with case-sensitive Airbyte tables and non-obvious
# join paths burn 2-3 attempts on discovery before the query that works.
MAX_SQL_ATTEMPTS = 5

# Token budget for the model request; old turns beyond this are trimmed from the
# request (NOT from the checkpointed conversation, which the UI renders in full)
HISTORY_TOKEN_BUDGET = 60_000

# Clear bulky old query results from the request once total context passes this
TOOL_RESULT_CLEAR_TRIGGER_TOKENS = 40_000
# ...but always keep the most recent tool results intact
TOOL_RESULTS_KEPT = 5

_FAILURE_PREFIXES = ("Query failed:", "SQL rejected:")


def count_failed_sql_attempts(messages: list[BaseMessage]) -> int:
    """Failed execute_sql attempts since the user's latest message."""
    failures = 0
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            break
        if (
            isinstance(message, ToolMessage)
            and message.name == "execute_sql"
            and isinstance(message.content, str)
            and message.content.startswith(_FAILURE_PREFIXES)
        ):
            failures += 1
    return failures


@before_model(can_jump_to=["end"])
def sql_retry_limiter(state, runtime):  # pylint: disable=unused-argument
    """Hard stop after MAX_SQL_ATTEMPTS failed queries for one user question.

    The system prompt asks the model to stop by itself; this middleware makes it
    deterministic — a final apology message is appended and the run ends.
    """
    if count_failed_sql_attempts(state["messages"]) < MAX_SQL_ATTEMPTS:
        return None
    return {
        "messages": [
            AIMessage(
                content=(
                    "I tried a few ways to query this but couldn't get a working "
                    "result. Could you rephrase the question, or tell me which "
                    "table it should come from? You can also ask me what data is "
                    "available."
                )
            )
        ],
        "jump_to": "end",
    }


@before_model
def trim_history(state, runtime):  # pylint: disable=unused-argument
    """Cap the model request at HISTORY_TOKEN_BUDGET tokens of recent history.

    Uses llm_input_messages so the trim affects only this model call — the full
    conversation stays in the checkpoint for the UI and for later turns.
    """
    messages = state["messages"]
    trimmed = trim_messages(
        messages,
        token_counter=count_tokens_approximately,
        max_tokens=HISTORY_TOKEN_BUDGET,
        start_on="human",
        include_system=True,
        allow_partial=False,
    )
    if len(trimmed) == len(messages):
        return None
    return {"llm_input_messages": trimmed}


# LangChain's error when a model hallucinates a tool it doesn't have:
# "Error: create_metric is not a valid tool, try one of [...]."
_INVALID_TOOL_RE = re.compile(r"Error:\s*(\w+) is not a valid tool")


def repair_invalid_tool_messages(
    messages: list[BaseMessage], own_tools: frozenset[str]
) -> list[ToolMessage]:
    """Replacements for invalid-tool errors that LIE to the current agent.

    The two agents share one message history. When the SQL agent hallucinates
    create_metric, the "not a valid tool" error it earns stays in the thread —
    and the guide agent (which really has create_metric) reads it as proof its
    own tool is broken ("I don't have write access"). Rewrite exactly those
    errors: ones naming a tool the CURRENT agent owns. Errors about tools this
    agent does NOT own are left intact — they are true here, and they are the
    corrective signal that makes the erring agent hand off instead of retrying.
    """
    replacements = []
    for message in messages:
        if not (isinstance(message, ToolMessage) and isinstance(message.content, str)):
            continue
        match = _INVALID_TOOL_RE.search(message.content)
        if match and match.group(1) in own_tools:
            tool = match.group(1)
            replacements.append(
                message.model_copy(
                    update={
                        "content": (
                            f"(An earlier attempt to call {tool} was made by a "
                            f"different assistant that does not have that tool. "
                            f"{tool} IS available to you — use it normally.)"
                        )
                    }
                )
            )
    return replacements


def repair_foreign_tool_errors(own_tools: tuple[str, ...] | frozenset[str]):
    """Middleware: durably rewrite cross-agent invalid-tool errors before the
    model sees them. Replacements carry the original message ids, so
    add_messages swaps them in the checkpoint — a one-time repair, not a
    per-call rewrite."""
    own = frozenset(own_tools)

    @before_model
    def repair_tool_errors(state, runtime):  # pylint: disable=unused-argument
        replacements = repair_invalid_tool_messages(state["messages"], own)
        if not replacements:
            return None
        return {"messages": replacements}

    return repair_tool_errors


def clear_old_tool_results() -> ContextEditingMiddleware:
    """Drop bulky old query outputs from the request once context grows large."""
    return ContextEditingMiddleware(
        edits=[
            ClearToolUsesEdit(
                trigger=TOOL_RESULT_CLEAR_TRIGGER_TOKENS,
                keep=TOOL_RESULTS_KEPT,
            )
        ]
    )
