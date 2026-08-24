"""ask_user tool — the agent's way to ask a clarifying question mid-run.

In production this tool never executes: the HITL middleware (agent/hitl.py)
intercepts every ask_user call with a respond-only interrupt, the question is
shown in the chat, and the user's typed reply is returned to the model as the
tool result. The body below is a fallback for contexts that run without the
middleware (evals, REPL), where no human is available to answer.
"""

from langchain.tools import tool

from ddpui.core.ai.tools.registry import register_tool


@register_tool
@tool
def ask_user(question: str) -> str:
    """Ask the user ONE short clarifying question and wait for their answer.
    Use this when you cannot find data matching their request, or when the
    question could mean two different things in a way that changes the SQL
    (which table, which time period, which program). Ask only what you need
    to proceed, in plain non-technical language — never SQL or column names
    alone, and never more than one question at a time."""
    return (
        f"(No user is available to answer: {question!r}. Proceed with your "
        "most reasonable assumption and state that assumption clearly in "
        "your answer.)"
    )
