"""Shared agent middleware: history trimming, tool-result clearing, SQL-retry limiter.

These are the sanctioned customization points of the prebuilt agent loop — the
graph topology itself is never modified. Feature-specific middleware (like the
chat agent's dynamic system prompt) lives in that feature's agent module.
"""

from langchain.agents.middleware import (
    ClearToolUsesEdit,
    ContextEditingMiddleware,
    before_model,
)
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.messages.utils import count_tokens_approximately, trim_messages

# Failed execute_sql calls allowed per user question before the loop is stopped;
# the system prompt tells the model the same number so it stops gracefully first
MAX_SQL_ATTEMPTS = 3

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
