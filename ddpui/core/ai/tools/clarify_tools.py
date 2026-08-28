"""ask_user + handoff tools — the agent's ways to yield control mid-run.

ask_user: in production this tool never executes — the HITL middleware
(agent/hitl.py) intercepts every call with a respond-only interrupt, the
question is shown in the chat, and the user's typed reply is returned to the
model as the tool result. The body below is a fallback for contexts that run
without the middleware (evals, REPL), where no human is available to answer.

handoff_to_platform_guide: the SQL agent's escape hatch when a creation
request lands on it anyway (the router sends most to the guide agent, but
short confirmations like "go ahead" can slip through). The TurnGraph watches
for this tool call after the sql_agent node and continues the SAME turn in
the guide agent — the user never has to re-ask.
"""

from langchain.tools import tool

from ddpui.core.ai.tools.registry import register_tool

HANDOFF_TOOL = "handoff_to_platform_guide"


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


@register_tool
@tool
def handoff_to_platform_guide(request_summary: str) -> str:
    """Hand the conversation to the platform guide, which creates charts,
    dashboards, KPIs, metrics, and reports. Call this the moment the user
    asks for (or agrees to) creating any of those — do NOT describe what
    you can't do, do NOT ask them to re-send their request. Summarize what
    they want created in request_summary. After calling this, stop — the
    guide continues the conversation."""
    return (
        f"(Handing off to the platform guide: {request_summary}. "
        "It will continue this conversation and create what was asked.)"
    )
