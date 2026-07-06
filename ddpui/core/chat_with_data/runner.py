"""Runs one chat turn and yields typed events — the transport-independent core.

The WebSocket consumer (and any future transport) forwards these events
verbatim. Event shapes are the WS protocol from plan §4.4:

    {"type": "token", "text": str}
    {"type": "tool_start", "tool": str, "label": str, "sql": str|None}
    {"type": "tool_end", "tool": str, "status": "success"|"error"}
    {"type": "message_complete", "message": str, "result_table": dict|None,
     "usage": {"input_tokens": int, "output_tokens": int}}
    {"type": "error", "message": str}

After the stream ends a ChatWithDataTurnAudit row is written (spec §7 layer 5).
"""

import time
import uuid
from typing import AsyncIterator

from asgiref.sync import sync_to_async
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

from ddpui.core.chat_with_data.agent import RECURSION_LIMIT
from ddpui.core.chat_with_data.content import extract_text
from ddpui.core.chat_with_data.state import RunContext
from ddpui.models.chat_with_data import ChatWithDataSession, ChatWithDataTurnAudit
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# Plain-language activity labels shown to non-technical users while tools run
TOOL_LABELS = {
    "list_schemas": "Looking at your data…",
    "list_tables": "Looking at your tables…",
    "get_table_details": "Reading table structure…",
    "profile_column": "Checking data values…",
    "execute_sql": "Running query…",
    "create_chart": "Creating chart…",
}
GENERIC_TOOL_LABEL = "Working…"

USER_FACING_ERROR = (
    "Something went wrong while answering this. Please try again — "
    "if it keeps failing, let your Dalgo support contact know."
)


async def run_turn(
    agent,
    session: ChatWithDataSession,
    orguser: OrgUser,
    question: str,
    context: RunContext,
) -> AsyncIterator[dict]:
    """Stream one turn of the agent. Always ends with message_complete or error,
    and always writes the audit row."""
    request_uuid = uuid.uuid4()
    started = time.monotonic()
    config = {
        "configurable": {"thread_id": str(session.thread_id)},
        "recursion_limit": RECURSION_LIMIT,
    }

    final_message = ""
    usage = {"input_tokens": 0, "output_tokens": 0}
    sql_queries: list[dict] = []
    tools_called: list[str] = []
    last_result_table: dict | None = None
    created_charts: list[dict] = []
    status = "completed"

    try:
        async for mode, chunk in agent.astream(
            {"messages": [("user", question)]},
            config=config,
            context=context,
            stream_mode=["messages", "updates"],
        ):
            if mode == "messages":
                message_chunk, _meta = chunk
                if isinstance(message_chunk, AIMessageChunk):
                    # content may be a block list (thinking/signature + text);
                    # only the text belongs on the wire
                    text = extract_text(message_chunk.content)
                    if text:
                        yield {"type": "token", "text": text}
                continue

            for update in (chunk or {}).values():
                for message in (update or {}).get("messages", []):
                    if isinstance(message, AIMessage) and message.tool_calls:
                        for tool_call in message.tool_calls:
                            tools_called.append(tool_call["name"])
                            yield {
                                "type": "tool_start",
                                "tool": tool_call["name"],
                                "label": TOOL_LABELS.get(tool_call["name"], GENERIC_TOOL_LABEL),
                                "sql": tool_call["args"].get("sql"),
                            }
                    elif isinstance(message, ToolMessage):
                        artifact = getattr(message, "artifact", None)
                        tool_status = "success"
                        if isinstance(artifact, dict) and artifact.get("type") == "chart":
                            # create_chart artifact — a saved chart (or a rejection)
                            if artifact.get("chart_id"):
                                created_charts.append(
                                    {
                                        "chart_id": artifact["chart_id"],
                                        "title": artifact.get("title", ""),
                                        "url_path": artifact.get("url_path", ""),
                                    }
                                )
                            else:
                                tool_status = "error"
                        elif isinstance(artifact, dict):
                            # execute_sql artifact — query + result table
                            tool_status = (
                                "success" if artifact.get("status") == "success" else "error"
                            )
                            sql_queries.append(
                                {
                                    "sql": artifact.get("sql"),
                                    "status": artifact.get("status"),
                                    "row_count": artifact.get("row_count"),
                                    "error": artifact.get("error"),
                                }
                            )
                            if artifact.get("status") == "success":
                                last_result_table = {
                                    "columns": artifact.get("columns", []),
                                    "rows": artifact.get("rows", []),
                                    "row_count": artifact.get("row_count", 0),
                                }
                        yield {"type": "tool_end", "tool": message.name, "status": tool_status}
                    if isinstance(message, AIMessage) and message.content:
                        text = extract_text(message.content)
                        if text:
                            final_message = text
                        if message.usage_metadata:
                            usage["input_tokens"] += message.usage_metadata.get("input_tokens", 0)
                            usage["output_tokens"] += message.usage_metadata.get("output_tokens", 0)

        yield {
            "type": "message_complete",
            "message": final_message,
            "result_table": last_result_table,
            "charts": created_charts,
            "usage": usage,
        }
    except Exception:  # pylint: disable=broad-except
        status = "failed"
        logger.exception(f"chat_with_data turn failed request_uuid={request_uuid}")
        yield {"type": "error", "message": USER_FACING_ERROR}
    finally:
        latency_ms = int((time.monotonic() - started) * 1000)
        try:
            await sync_to_async(ChatWithDataTurnAudit.objects.create)(
                org=orguser.org,
                orguser=orguser,
                session=session,
                request_uuid=request_uuid,
                user_message=question,
                sql_queries=sql_queries,
                tools_called=tools_called,
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
                latency_ms=latency_ms,
                status=status,
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"failed to write turn audit request_uuid={request_uuid}")
