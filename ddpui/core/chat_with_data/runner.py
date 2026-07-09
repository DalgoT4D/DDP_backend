"""Runs one chat turn and yields typed events — the transport-independent core.

The WebSocket consumer (and any future transport) forwards these events
verbatim. Event shapes are the WS protocol from plan §4.4:

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

import dataclasses
import os
import time
import uuid
from typing import AsyncIterator

from asgiref.sync import sync_to_async
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

from ddpui.core.chat_with_data.agent.build import DEFAULT_MODEL, RECURSION_LIMIT
from ddpui.core.chat_with_data.messages.content import extract_text
from ddpui.core.chat_with_data.observability import start_turn_trace
from ddpui.core.chat_with_data.calls.router import casual_reply, route_question
from ddpui.core.chat_with_data.agent.state import RunContext
from ddpui.core.chat_with_data.calls.validator import validate_turn
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

    # Stage 1: query understanding — one cheap call; fails open to data_question.
    # The router sees a compact history tail: without it, follow-ups that say
    # "this"/"that" look ambiguous in isolation and get wrongly diverted.
    history_tail = await _thread_tail(agent, session)
    route = await route_question(question, history=history_tail)
    context.question = question
    context.complexity = route.complexity

    # needs_clarification may only divert the FIRST turn — with any history the
    # agent (which holds the full conversation) handles ambiguity itself.
    diverts = route.intent == "small_talk" or (
        route.intent == "needs_clarification" and not history_tail
    )
    if diverts:
        async for event in _short_circuit_turn(
            agent=agent,
            session=session,
            orguser=orguser,
            question=question,
            route=route,
            request_uuid=request_uuid,
            started=started,
        ):
            yield event
        return

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
    validation: dict | None = None
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
                        if isinstance(artifact, dict) and artifact.get("type") in (
                            "chart",
                            "dashboard",
                        ):
                            # created-artifact chip (saved chart or dashboard), or a rejection
                            artifact_id = artifact.get("chart_id") or artifact.get("dashboard_id")
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
                            usage["input_tokens"] += message.usage_metadata.get("input_tokens", 0)
                            usage["output_tokens"] += message.usage_metadata.get("output_tokens", 0)

        yield {
            "type": "message_complete",
            "message": final_message,
            "result_table": last_result_table,
            "charts": created_charts,
            "usage": usage,
        }

        # Stage 5b: post-execution validation — annotates, never blocks
        validation = await validate_turn(
            question=question,
            sql_queries=sql_queries,
            result_table=last_result_table,
            answer=final_message,
        )
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
                intent=dataclasses.asdict(route),
                validation=validation,
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception(f"failed to write turn audit request_uuid={request_uuid}")


async def _short_circuit_turn(
    *, agent, session, orguser, question, route, request_uuid, started
) -> AsyncIterator[dict]:
    """Answer small talk / ask for clarification without running the SQL agent.

    The exchange is still written into the checkpointer thread so follow-up
    questions keep their conversational context, and the turn is audited with
    tools_called=[] — routing effectiveness stays measurable.
    """
    from langchain_core.messages import HumanMessage

    if route.intent == "needs_clarification" and route.clarification:
        reply = route.clarification
    else:
        reply = await casual_reply(question)

    yield {
        "type": "message_complete",
        "message": reply,
        "result_table": None,
        "charts": [],
        "usage": {"input_tokens": 0, "output_tokens": 0},
    }

    config = {"configurable": {"thread_id": str(session.thread_id)}}
    try:
        await agent.aupdate_state(
            config, {"messages": [HumanMessage(question), AIMessage(content=reply)]}
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: failed to record short-circuit turn in thread")

    try:
        await sync_to_async(ChatWithDataTurnAudit.objects.create)(
            org=orguser.org,
            orguser=orguser,
            session=session,
            request_uuid=request_uuid,
            user_message=question,
            sql_queries=[],
            tools_called=[],
            latency_ms=int((time.monotonic() - started) * 1000),
            status="completed",
            intent=dataclasses.asdict(route),
        )
    except Exception:  # pylint: disable=broad-except
        logger.exception(f"failed to write turn audit request_uuid={request_uuid}")


# History lines shown to the router; each clipped so the prompt stays small
_TAIL_MESSAGES = 6
_TAIL_LINE_CHARS = 300


async def _thread_tail(agent, session) -> list[str]:
    """Compact "User:/Assistant:" tail of the conversation for the router.
    Empty list on a fresh thread or on any failure (routing then treats the
    turn as a first turn)."""
    from langchain_core.messages import HumanMessage

    try:
        state = await agent.aget_state({"configurable": {"thread_id": str(session.thread_id)}})
        messages = (state.values or {}).get("messages", [])
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: failed to read thread tail (routing without history)")
        return []

    lines: list[str] = []
    for message in messages:
        if isinstance(message, HumanMessage):
            role = "User"
        elif isinstance(message, AIMessage) and not message.tool_calls:
            role = "Assistant"
        else:
            continue  # tool chatter is noise for routing
        text = extract_text(message.content).strip()
        if text:
            lines.append(f"{role}: {text[:_TAIL_LINE_CHARS]}")
    return lines[-_TAIL_MESSAGES:]
