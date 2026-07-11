"""Runs one chat turn and yields typed events — the transport-independent core.

The turn pipeline itself (route → retrieve → agent → validate) is the
TurnGraph (graph.py); this module streams it and translates LangGraph chunks
into the WS event protocol. The WebSocket consumer (and any future transport)
forwards these events verbatim. Event shapes are the WS protocol from plan §4.4:

    {"type": "token", "text": str}
    {"type": "tool_start", "tool": str, "label": str, "sql": str|None}
    {"type": "tool_end", "tool": str, "status": "success"|"error"}
    {"type": "message_complete", "message": str, "result_table": dict|None,
     "charts": list, "usage": {"input_tokens": int, "output_tokens": int}}
    {"type": "validation", "verdict": "ok"|"warn", "assumptions": list,
     "caveat": str|None}   — post-execution audit, arrives after message_complete
    {"type": "error", "message": str}

After the stream ends a ChatWithDataTurnAudit row is written (spec §7 layer 5).
"""

import os
import time
import uuid
from typing import AsyncIterator

from asgiref.sync import sync_to_async
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

from ddpui.core.ai.agent.chat_data_agent import DEFAULT_MODEL, RECURSION_LIMIT
from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.llm_calls.router import casual_reply, route_question
from ddpui.core.ai.llm_calls.turn_audit import validate_turn
from ddpui.core.ai.chat.turn_graph import build_turn_graph
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.tracing import start_turn_trace
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
    "list_dashboards": "Checking your dashboards…",
    "create_dashboard": "Creating dashboard…",
    "add_charts_to_dashboard": "Adding to dashboard…",
}
GENERIC_TOOL_LABEL = "Working…"

USER_FACING_ERROR = (
    "Something went wrong while answering this. Please try again — "
    "if it keeps failing, let your Dalgo support contact know."
)

# Parent-graph nodes that end a turn with a plain reply (no SQL agent involved)
_SHORT_CIRCUIT_NODES = ("casual_reply_node", "clarify_node")


async def run_turn(
    agent,
    session: ChatWithDataSession,
    orguser: OrgUser,
    question: str,
    context: RunContext,
) -> AsyncIterator[dict]:
    """Stream one turn of the TurnGraph. Always ends with message_complete or
    error, and always writes the audit row.

    The brains are passed as this module's globals (route_question, casual_reply,
    validate_turn) at call time, so tests can patch them per turn. The parent
    graph reuses the agent's checkpointer — one saver, one thread namespace."""
    request_uuid = uuid.uuid4()
    started = time.monotonic()

    graph = build_turn_graph(
        agent,
        route_fn=route_question,
        casual_reply_fn=casual_reply,
        validate_fn=validate_turn,
        checkpointer=agent.checkpointer,
    )

    trace_handler = start_turn_trace(
        session=session,
        orguser=orguser,
        context=context,
        question=question,
        request_uuid=request_uuid,
        model_name=os.getenv("CHAT_WITH_DATA_MODEL", DEFAULT_MODEL),
    )
    config = {
        "configurable": {"thread_id": str(session.thread_id)},
        "recursion_limit": RECURSION_LIMIT,
    }
    if trace_handler is not None:
        config["callbacks"] = [trace_handler]

    final_message = ""
    route_dict: dict | None = None
    validation: dict | None = None
    usage = {"input_tokens": 0, "output_tokens": 0}
    sql_queries: list[dict] = []
    tools_called: list[str] = []
    last_result_table: dict | None = None
    created_charts: list[dict] = []
    status = "completed"

    def _message_complete() -> dict:
        return {
            "type": "message_complete",
            "message": final_message,
            "result_table": last_result_table,
            "charts": created_charts,
            "usage": usage,
        }

    try:
        async for namespace, mode, chunk in graph.astream(
            {"messages": [("user", question)], "question": question},
            config=config,
            context=context,
            stream_mode=["messages", "updates"],
            subgraphs=True,
        ):
            if mode == "messages":
                message_chunk, meta = chunk
                # Only the agent's model node streams to the user — the router/
                # validator/casual-reply calls inside other nodes never leak
                if (
                    isinstance(message_chunk, AIMessageChunk)
                    and meta.get("langgraph_node") == "model"
                ):
                    text = extract_text(message_chunk.content)
                    if text:
                        yield {"type": "token", "text": text}
                continue

            if namespace:
                # inside the sql_agent subgraph: tool activity + the answer
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
                            if isinstance(artifact, dict) and artifact.get("type") in (
                                "chart",
                                "dashboard",
                            ):
                                # created-artifact chip (saved chart or dashboard), or a rejection
                                artifact_id = artifact.get("chart_id") or artifact.get(
                                    "dashboard_id"
                                )
                                if artifact_id:
                                    created_charts.append(
                                        {
                                            "chart_id": artifact_id,
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
                                usage["input_tokens"] += message.usage_metadata.get(
                                    "input_tokens", 0
                                )
                                usage["output_tokens"] += message.usage_metadata.get(
                                    "output_tokens", 0
                                )
                continue

            # parent-level updates: stage boundaries
            for node, update in (chunk or {}).items():
                if node == "route_node":
                    route_dict = (update or {}).get("route")
                elif node in _SHORT_CIRCUIT_NODES:
                    reply_messages = (update or {}).get("messages", [])
                    if reply_messages:
                        final_message = extract_text(reply_messages[-1].content)
                    yield _message_complete()
                elif node == "sql_agent":
                    # the subgraph finished — the answer is complete; validation
                    # (which never blocks the answer) streams as its own event
                    yield _message_complete()
                elif node == "validate_node":
                    validation = (update or {}).get("validation")
                    if validation is not None:
                        yield {"type": "validation", **validation}
                        if trace_handler is not None:
                            trace_handler.score(
                                name="result_validation",
                                value=1 if validation["verdict"] == "ok" else 0,
                                comment=validation.get("caveat"),
                            )
    except Exception:  # pylint: disable=broad-except
        status = "failed"
        logger.exception(f"chat_with_data turn failed request_uuid={request_uuid}")
        yield {"type": "error", "message": USER_FACING_ERROR}
    finally:
        latency_ms = int((time.monotonic() - started) * 1000)
        if trace_handler is not None:
            trace_handler.finish(output=final_message, status=status)
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
                intent=route_dict,
                validation=validation,
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"failed to write turn audit request_uuid={request_uuid}")
