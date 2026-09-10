"""Views over the conversation message list.

The checkpointed conversation holds every message (tool calls, tool results,
answers). These helpers cut the views the pipeline stages need: a compact
history tail for the router, and the current turn's slice for the audit.
"""

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage

from ddpui.core.ai.messages.content import extract_text

# History lines shown to the router; each clipped so the prompt stays small
TAIL_MESSAGES = 6
TAIL_LINE_CHARS = 300


def history_lines(messages: list[AnyMessage]) -> list[str]:
    """Compact "User:/Assistant:" tail of the conversation for the router.
    Excludes the current (last) user message; tool chatter is noise for routing."""
    lines: list[str] = []
    for message in messages[:-1]:
        if isinstance(message, HumanMessage):
            role = "User"
        elif isinstance(message, AIMessage) and not message.tool_calls:
            role = "Assistant"
        else:
            continue
        text = extract_text(message.content).strip()
        if text:
            lines.append(f"{role}: {text[:TAIL_LINE_CHARS]}")
    return lines[-TAIL_MESSAGES:]


def turn_segment(messages: list[AnyMessage]) -> list[AnyMessage]:
    """The messages of the CURRENT turn — everything after the last HumanMessage.
    The turn audit must never see a previous turn's SQL."""
    for i in range(len(messages) - 1, -1, -1):
        if isinstance(messages[i], HumanMessage):
            return messages[i + 1 :]
    return messages
