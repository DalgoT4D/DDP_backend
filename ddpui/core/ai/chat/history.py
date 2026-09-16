"""Replay a checkpointer thread as UI-shaped chat history.

The checkpointer stores the raw LangChain message list (including tool calls
and tool results). The UI wants bubbles: user question, assistant answer, with
any executed SQL (and its result table) and created charts/dashboards attached
to the answer.
"""

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from ddpui.core.ai.agent.checkpointer import get_checkpointer
from ddpui.core.ai.messages.artifacts import (
    creation_chip,
    is_creation_artifact,
    tool_artifact,
)
from ddpui.core.ai.messages.content import extract_text
from ddpui.schemas.chat_with_data_schemas import MessageOut, SqlAttachment


def map_messages(messages: list[BaseMessage]) -> list[MessageOut]:
    """Collapse the raw message list into user/assistant bubbles. execute_sql
    results and created charts/dashboards attach to the next assistant answer;
    other tool chatter is hidden."""
    out: list[MessageOut] = []
    pending_sql: list[SqlAttachment] = []
    pending_charts: list[dict] = []

    for message in messages:
        if isinstance(message, HumanMessage):
            out.append(MessageOut(role="user", content=extract_text(message.content)))
        elif isinstance(message, ToolMessage):
            artifact = tool_artifact(message)
            if artifact is None:
                continue
            if is_creation_artifact(artifact):
                chip = creation_chip(artifact)
                if chip:
                    pending_charts.append(chip)
            elif artifact.get("sql"):
                pending_sql.append(
                    SqlAttachment(
                        sql=artifact["sql"],
                        status=artifact.get("status", "unknown"),
                        row_count=artifact.get("row_count"),
                        columns=artifact.get("columns"),
                        rows=artifact.get("rows"),
                    )
                )
        elif isinstance(message, AIMessage) and message.content:
            text = extract_text(message.content)
            if not text:
                continue  # thinking-only content — nothing to show
            out.append(
                MessageOut(
                    role="assistant",
                    content=text,
                    sql_attachments=pending_sql,
                    charts=pending_charts,
                )
            )
            pending_sql = []
            pending_charts = []

    return out


async def read_thread_messages(thread_id: str) -> list[MessageOut]:
    """Load a thread's messages straight from the checkpointer (no graph needed)."""
    saver = await get_checkpointer()
    checkpoint_tuple = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    if checkpoint_tuple is None:
        return []
    messages = checkpoint_tuple.checkpoint.get("channel_values", {}).get("messages", [])
    return map_messages(messages)
