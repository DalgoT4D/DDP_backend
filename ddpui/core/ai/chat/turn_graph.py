"""The TurnGraph — the turn pipeline as a hand-built LangGraph (approach 2).

Stages that were Python control flow in runner.py become named nodes and
edges, so they show up in traces, checkpoints, and get_graph() diagrams:

    START → route_node ──┬─ small talk        → casual_reply_node → END
                         ├─ needs clarify*    → clarify_node      → END
                         └─ data question     → retrieve_context_node
                                                (*first turn only)   ↓
                                                sql_agent (subgraph node)
                                                                     ↓
                                                validate_node → END

The stage brains stay in calls/ — nodes are thin adapters. They are INJECTED
(route_fn, casual_reply_fn) rather than imported so the runner can pass its
own module globals, keeping them patchable per-turn and avoiding a circular
import with runner.py.
"""

import dataclasses
from typing import Annotated, Any, Optional, TypedDict

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.runtime import Runtime

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.messages.content import extract_text

# History lines shown to the router; each clipped so the prompt stays small
TAIL_MESSAGES = 6
TAIL_LINE_CHARS = 300


class TurnState(TypedDict):
    """Parent-graph state. `messages` is shared with the agent subgraph by
    channel name; the other keys are per-stage outputs kept in the checkpoint."""

    messages: Annotated[list[AnyMessage], add_messages]
    question: str
    route: dict
    has_history: bool
    validation: Optional[dict]


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
    Validation must never see a previous turn's SQL."""
    for i in range(len(messages) - 1, -1, -1):
        if isinstance(messages[i], HumanMessage):
            return messages[i + 1 :]
    return messages


def extract_turn_results(messages: list[AnyMessage]) -> tuple[list[dict], Optional[dict], str]:
    """(sql_queries, last successful result_table, final answer text) for one
    turn's messages. Chart/dashboard artifacts carry a "type" key; any other
    dict artifact is an execute_sql result — the runner uses the same rule."""
    sql_queries: list[dict] = []
    result_table: Optional[dict] = None
    answer = ""
    for message in messages:
        if isinstance(message, ToolMessage):
            artifact = getattr(message, "artifact", None)
            if isinstance(artifact, dict) and artifact.get("type") not in ("chart", "dashboard"):
                sql_queries.append(
                    {
                        "sql": artifact.get("sql"),
                        "status": artifact.get("status"),
                        "row_count": artifact.get("row_count"),
                        "error": artifact.get("error"),
                    }
                )
                if artifact.get("status") == "success":
                    result_table = {
                        "columns": artifact.get("columns", []),
                        "rows": artifact.get("rows", []),
                        "row_count": artifact.get("row_count", 0),
                    }
        elif isinstance(message, AIMessage) and not message.tool_calls:
            text = extract_text(message.content)
            if text:
                answer = text
    return sql_queries, result_table, answer


def build_turn_graph(
    agent: Any,
    *,
    route_fn,
    casual_reply_fn,
    validate_fn=None,
    checkpointer: BaseCheckpointSaver | None = None,
):
    """Assemble and compile the TurnGraph around a compiled agent.

    `agent` is the create_agent graph, mounted as a subgraph node — only the
    parent is compiled with a checkpointer (the subgraph inherits it)."""

    async def route_node(state: TurnState, runtime: Runtime[RunContext]) -> dict:
        question = state["question"]
        history = history_lines(state["messages"])
        route = await route_fn(question, history=history)
        # reflection gate + tool context read these off the runtime context
        if runtime.context is not None:
            runtime.context.question = question
            runtime.context.complexity = route.complexity
        return {"route": dataclasses.asdict(route), "has_history": bool(history)}

    async def casual_reply_node(state: TurnState) -> dict:
        reply = await casual_reply_fn(state["question"])
        return {"messages": [AIMessage(content=reply)]}

    async def clarify_node(state: TurnState) -> dict:
        return {"messages": [AIMessage(content=state["route"]["clarification"])]}

    async def retrieve_context_node(_state: TurnState) -> dict:
        # M5 fills this: BM25 over table cards → system-prompt context block.
        # A named no-op until then, so the pipeline shape is already the
        # approach-2 diagram and cards plug in without rewiring.
        return {}

    async def validate_node(state: TurnState) -> dict:
        if validate_fn is None:
            return {"validation": None}
        sql_queries, result_table, answer = extract_turn_results(turn_segment(state["messages"]))
        validation = await validate_fn(
            question=state["question"],
            sql_queries=sql_queries,
            result_table=result_table,
            answer=answer,
        )
        return {"validation": validation}

    def route_decision(state: TurnState) -> str:
        route = state["route"]
        if route["intent"] == "small_talk":
            return "casual_reply_node"
        # clarification may only divert the FIRST turn — with any history the
        # agent (which holds the full conversation) handles ambiguity itself
        if route["intent"] == "needs_clarification" and not state["has_history"]:
            return "clarify_node" if route.get("clarification") else "casual_reply_node"
        return "retrieve_context_node"

    graph = StateGraph(TurnState, context_schema=RunContext)
    graph.add_node("route_node", route_node)
    graph.add_node("casual_reply_node", casual_reply_node)
    graph.add_node("clarify_node", clarify_node)
    graph.add_node("retrieve_context_node", retrieve_context_node)
    graph.add_node("sql_agent", agent)
    graph.add_node("validate_node", validate_node)

    graph.add_edge(START, "route_node")
    graph.add_conditional_edges(
        "route_node",
        route_decision,
        ["casual_reply_node", "clarify_node", "retrieve_context_node"],
    )
    graph.add_edge("retrieve_context_node", "sql_agent")
    graph.add_edge("casual_reply_node", END)
    graph.add_edge("clarify_node", END)
    graph.add_edge("sql_agent", "validate_node")
    graph.add_edge("validate_node", END)

    return graph.compile(checkpointer=checkpointer)
