"""The TurnGraph — the turn pipeline as a hand-built LangGraph (approach 2).

Stages that were Python control flow in the runner become named nodes and
edges, so they show up in traces, checkpoints, and get_graph() diagrams:

    START → route_node ──┬─ small talk        → casual_reply_node → END
                         ├─ needs clarify*    → clarify_node      → END
                         ├─ platform help     → guide_agent       → END
                         └─ data question     → retrieve_context_node
                                                (*first turn only)   ↓
                                                sql_agent (subgraph node)
                                                          ↓
                                          handed off? ── yes → guide_agent → END
                                                          ↓ no
                                                validate_node → END

The guide_agent path skips validate_node on purpose: the validator is a
text-to-SQL audit (grain, filters, false zeros) and has nothing to say about
a guidance/creation answer. The handed-off edge fires when the SQL agent
called handoff_to_platform_guide — a creation request that reached it anyway
continues in the guide agent instead of dead-ending in an apology.

The stage brains stay in llm_calls/ — nodes are thin adapters. They are
INJECTED (route_fn, casual_reply_fn) rather than imported so the runner can
pass its own module globals, keeping them patchable per-turn and avoiding a
circular import with turn_runner.py.
"""

from typing import Annotated, Any, Literal, Optional, Protocol, TypedDict

from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import Runtime
from langgraph.types import Checkpointer

from ddpui.core.ai.agent.run_context import RunContext
from ddpui.core.ai.messages.artifacts import extract_turn_results
from ddpui.core.ai.messages.conversation import history_lines, turn_segment
from ddpui.core.ai.prompts import DATA_RESPONDER_LINE, GUIDE_RESPONDER_LINE
from ddpui.core.ai.toolsets import HANDOFF_TOOL
from ddpui.core.ai.typed_dicts import (
    ResultTable,
    RouteState,
    SqlQueryEntry,
    TurnValidation,
)
from ddpui.schemas.chat_with_data_schemas import RouteResult


def turn_handed_off(messages: list[AnyMessage]) -> bool:
    """True when the CURRENT turn contains a handoff_to_platform_guide call —
    the SQL agent yielded the turn to the guide agent."""
    return any(
        isinstance(message, ToolMessage) and message.name == HANDOFF_TOOL
        for message in turn_segment(messages)
    )


def last_responder_line(state: "TurnState") -> str | None:
    """Which agent produced the previous turn's answer, as an annotation line
    appended to the router's history. Short follow-ups ("yes", "make it
    monthly") route far better when the router knows who the user is replying
    to. None on the first turn or after casual/clarify turns (no signal)."""
    prior = state["messages"][:-1]  # everything before the current user message
    if not turn_segment(prior):
        return None
    if turn_handed_off(prior):
        return GUIDE_RESPONDER_LINE
    intent = (state.get("route") or {}).get("intent")
    if intent == "platform_help":
        return GUIDE_RESPONDER_LINE
    if intent == "data_question":
        return DATA_RESPONDER_LINE
    return None


class TurnState(TypedDict):
    """Parent-graph state. `messages` is shared with the agent subgraph by
    channel name; the other keys are per-stage outputs kept in the checkpoint."""

    messages: Annotated[list[AnyMessage], add_messages]
    question: str
    route: RouteState
    has_history: bool
    validation: Optional[TurnValidation]


class RouteFn(Protocol):
    """Shape of llm_calls/router.route_question (and its test fakes)."""

    async def __call__(self, question: str, *, history: list[str] | None = None) -> RouteResult:
        ...


class CasualReplyFn(Protocol):
    """Shape of llm_calls/router.casual_reply (and its test fakes)."""

    async def __call__(self, question: str) -> str:
        ...


class ValidateFn(Protocol):
    """Shape of llm_calls/turn_audit.audit_turn (and its test fakes)."""

    async def __call__(
        self,
        *,
        question: str,
        sql_queries: list[SqlQueryEntry],
        result_table: ResultTable | None,
        answer: str,
    ) -> TurnValidation | None:
        ...


# A compiled create_agent graph (state/input/output types vary per agent;
# the run context is always ours)
AgentGraph = CompiledStateGraph[Any, RunContext, Any, Any]

RouteDestination = Literal[
    "casual_reply_node", "clarify_node", "guide_agent", "retrieve_context_node"
]


def build_turn_graph(
    agent: AgentGraph,
    guide_agent: AgentGraph,
    *,
    route_fn: RouteFn,
    casual_reply_fn: CasualReplyFn,
    validate_fn: ValidateFn | None = None,
    checkpointer: Checkpointer = None,
) -> "CompiledStateGraph[TurnState, RunContext, TurnState, TurnState]":
    """Assemble and compile the TurnGraph around the compiled agents.

    `agent` (SQL) and `guide_agent` are create_agent graphs mounted as subgraph
    nodes — only the parent is compiled with a checkpointer (subgraphs inherit
    it). `validate_fn=None` skips the audit (evals score answers themselves).

    Actual state is TurnState class above, the nodes only update a part of the state. hence return dict and not state.
    """

    async def route_node(state: TurnState, runtime: Runtime[RunContext]) -> dict:
        question = state["question"]
        history = history_lines(state["messages"])
        # tell the router who wrote the last answer — rides inside `history`
        # so injected route_fns (and their test fakes) keep their signature
        responder = last_responder_line(state)
        if history and responder:
            history = [*history, responder]
        route = await route_fn(question, history=history)
        # reflection gate + tool context read these off the runtime context
        if runtime.context is not None:
            runtime.context.question = question
            runtime.context.complexity = route.complexity
        return {"route": route.model_dump(), "has_history": bool(history)}

    async def casual_reply_node(state: TurnState) -> dict:
        reply = await casual_reply_fn(state["question"])
        return {"messages": [AIMessage(content=reply)]}

    async def clarify_node(state: TurnState) -> dict:
        return {"messages": [AIMessage(content=state["route"]["clarification"])]}

    async def retrieve_context_node(state: TurnState) -> dict:  # pylint: disable=unused-argument
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

    def route_decision(state: TurnState) -> RouteDestination:
        route = state["route"]
        if route["intent"] == "small_talk":
            return "casual_reply_node"
        # clarification may only divert the FIRST turn — with any history the
        # agent (which holds the full conversation) handles ambiguity itself
        if route["intent"] == "needs_clarification" and not state["has_history"]:
            return "clarify_node" if route.get("clarification") else "casual_reply_node"
        if route["intent"] == "platform_help":
            return "guide_agent"
        return "retrieve_context_node"

    # mid-turn handoff: a creation request that landed on the SQL agent
    # anyway (e.g. a "go ahead" confirming a creation offer) continues in
    # the guide agent instead of dead-ending in an "I can't do that" reply
    def after_sql_agent(state: TurnState) -> Literal["guide_agent", "validate_node"]:
        return "guide_agent" if turn_handed_off(state["messages"]) else "validate_node"

    graph = StateGraph(TurnState, context_schema=RunContext)
    graph.add_node("route_node", route_node)
    graph.add_node("casual_reply_node", casual_reply_node)
    graph.add_node("clarify_node", clarify_node)
    graph.add_node("retrieve_context_node", retrieve_context_node)
    graph.add_node("sql_agent", agent)
    graph.add_node("guide_agent", guide_agent)
    graph.add_node("validate_node", validate_node)

    graph.add_edge(START, "route_node")
    graph.add_conditional_edges(
        "route_node",
        route_decision,
        ["casual_reply_node", "clarify_node", "guide_agent", "retrieve_context_node"],
    )
    graph.add_edge("retrieve_context_node", "sql_agent")
    graph.add_conditional_edges("sql_agent", after_sql_agent, ["guide_agent", "validate_node"])
    graph.add_edge("casual_reply_node", END)
    graph.add_edge("clarify_node", END)
    graph.add_edge("guide_agent", END)
    graph.add_edge("validate_node", END)

    return graph.compile(checkpointer=checkpointer)
