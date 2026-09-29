"""Replay a checkpointer thread as UI-shaped chat history.

The checkpointer stores the raw LangChain message list (including tool calls
and tool results). The UI wants bubbles: user question, assistant answer, with
any executed SQL (and its result table) and created charts/dashboards attached
to the answer.
"""

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from ddpui.core.ai.agent.checkpointer import get_checkpointer
from ddpui.core.ai.agent.hitl import NO_ANSWER, redirect_text
from ddpui.core.ai.messages.artifacts import (
    creation_chip,
    is_creation_artifact,
    tool_artifact,
)
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.toolsets import QUESTION_TOOL
from ddpui.schemas.chat_with_data_schemas import MessageOut, SqlAttachment
from ddpui.core.ai.typed_dicts import CreatedArtifactChip


def _asked_question(message: AIMessage) -> str:
    """The question an ask_user call puts to the user ("" when it asks none).
    The live UI shows it as the assistant's words, but the checkpoint holds it
    only as a tool-call argument."""
    for call in message.tool_calls:
        if call.get("name") == QUESTION_TOOL:
            return str(call.get("args", {}).get("question", ""))
    return ""


def _user_reply(message: ToolMessage) -> str | None:
    """What the user typed that the checkpoint keeps only as a tool result: an
    answer to ask_user, or a message sent instead of approving a step."""
    content = extract_text(message.content)
    if message.name == QUESTION_TOOL:
        return None if content == NO_ANSWER else content
    return redirect_text(content)


def map_messages(messages: list[BaseMessage]) -> list[MessageOut]:
    """Collapse the raw message list into user/assistant bubbles. execute_sql
    results and created charts/dashboards attach to the next assistant answer;
    other tool chatter is hidden."""
    out: list[MessageOut] = []
    pending_sql: list[SqlAttachment] = []
    pending_artifacts: list[CreatedArtifactChip] = []

    for message in messages:
        if isinstance(message, HumanMessage):
            out.append(MessageOut(role="user", content=extract_text(message.content)))
        elif isinstance(message, ToolMessage):
            reply = _user_reply(message)
            if reply is not None:
                # one typed message can resolve an ask_user AND a rejection in
                # the same pause — it was still sent once
                already_shown = out and out[-1].role == "user" and out[-1].content == reply
                if not already_shown:
                    out.append(MessageOut(role="user", content=reply))
                continue
            artifact = tool_artifact(message)
            if artifact is None:
                continue
            if is_creation_artifact(artifact):
                chip = creation_chip(artifact)
                if chip:
                    pending_artifacts.append(chip)
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
        elif isinstance(message, AIMessage):
            # a lone ask_user call reads as the assistant asking, as it did live
            text = extract_text(message.content) if message.content else ""
            text = text or _asked_question(message)
            if not text:
                continue  # tool calls or thinking only — nothing to show
            out.append(
                MessageOut(
                    role="assistant",
                    content=text,
                    sql_attachments=pending_sql,
                    artifacts=pending_artifacts,
                )
            )
            pending_sql = []
            pending_artifacts = []

    return out


async def read_thread_messages(thread_id: str) -> list[MessageOut]:
    """Load a thread's messages straight from the checkpointer (no graph needed)."""
    saver = await get_checkpointer()
    checkpoint_tuple = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    if checkpoint_tuple is None:
        return []
    messages = checkpoint_tuple.checkpoint.get("channel_values", {}).get("messages", [])
    return map_messages(messages)
