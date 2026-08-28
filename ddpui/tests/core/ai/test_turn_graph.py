"""Tests for the TurnGraph — the hand-built pipeline graph (approach 2).

The graph wires route → (casual | clarify | data path) as real nodes and
edges. Brains (router/validator) are injected functions, so these tests run
with no API key and no Django models — pure graph behavior.
"""

import asyncio

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver

from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.llm_calls.router import RouteResult
from ddpui.core.ai.chat.turn_graph import build_turn_graph
from ddpui.tests.core.ai.test_agent_loop import (
    ScriptedChatModel,
    make_context,
    sql_call,
)
from ddpui.tests.core.ai.test_tools import FakeWarehouse


class MustNotRun(ScriptedChatModel):
    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        raise AssertionError("the SQL agent must not run on this path")


def run_graph(graph, question: str, thread_id: str = "t1", context=None):
    config = {"configurable": {"thread_id": thread_id}}

    async def _run():
        return await graph.ainvoke(
            {"messages": [HumanMessage(question)], "question": question},
            config=config,
            context=context or make_context(),
        )

    return asyncio.run(_run())


def test_small_talk_ends_at_casual_reply_without_the_agent():
    agent = build_agent(checkpointer=InMemorySaver(), model=MustNotRun(script=[]))

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent="small_talk")

    async def fake_reply(question, model=None):
        return "You're welcome! Ask me anything about your data."

    graph = build_turn_graph(agent, route_fn=fake_route, casual_reply_fn=fake_reply)
    result = run_graph(graph, "thanks!")

    final = result["messages"][-1]
    assert isinstance(final, AIMessage)
    assert final.content == "You're welcome! Ask me anything about your data."
    assert result["route"]["intent"] == "small_talk"


def test_first_turn_clarification_ends_at_clarify_node():
    agent = build_agent(checkpointer=InMemorySaver(), model=MustNotRun(script=[]))

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent="needs_clarification", clarification="Compare what to what?")

    async def must_not_reply(question, model=None):
        raise AssertionError("casual_reply must not run when a clarification exists")

    graph = build_turn_graph(agent, route_fn=fake_route, casual_reply_fn=must_not_reply)
    result = run_graph(graph, "compare them")

    assert result["messages"][-1].content == "Compare what to what?"


def test_clarification_with_history_falls_through_to_the_agent():
    """With any prior exchange in the thread, a clarify route must NOT divert —
    the agent holds the conversation and resolves references itself."""
    agent = build_agent(
        checkpointer=InMemorySaver(),
        model=ScriptedChatModel(script=[AIMessage(content="Here is the chart answer.")]),
    )

    seen = {}

    async def fake_route(question, model=None, history=None):
        seen["history"] = history
        return RouteResult(intent="needs_clarification", clarification="Chart of what?")

    async def fake_reply(question, model=None):
        return "hi"

    checkpointer = InMemorySaver()
    graph = build_turn_graph(
        agent, route_fn=fake_route, casual_reply_fn=fake_reply, checkpointer=checkpointer
    )

    async def _run():
        config = {"configurable": {"thread_id": "t-hist"}}
        # a prior exchange exists in the thread
        await graph.aupdate_state(
            config,
            {
                "messages": [
                    HumanMessage("list of top donors"),
                    AIMessage(content="Top donors: ..."),
                ]
            },
        )
        return await graph.ainvoke(
            {"messages": [HumanMessage("chart this")], "question": "chart this"},
            config=config,
            context=make_context(),
        )

    result = asyncio.run(_run())

    assert result["messages"][-1].content == "Here is the chart answer."
    # the router was shown the recent conversation
    assert any("top donors" in line.lower() for line in seen["history"])


def test_thread_continuity_with_checkpointer_on_parent_only():
    """The agent is compiled WITHOUT a checkpointer; the parent's saver is
    inherited per-invocation. A second turn on the same thread must show the
    agent the first turn's exchange."""

    class RecordingModel(ScriptedChatModel):
        seen: list = []

        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            self.seen.append(list(messages))
            return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    model = RecordingModel(
        script=[
            AIMessage(content="You ran 1,284 surveys."),
            AIMessage(content="Pune leads with 700."),
        ],
        seen=[],
    )
    agent = build_agent(checkpointer=None, model=model)

    async def fake_route(question, model=None, history=None):
        return RouteResult()  # data_question

    async def fake_reply(question, model=None):
        return "hi"

    graph = build_turn_graph(
        agent, route_fn=fake_route, casual_reply_fn=fake_reply, checkpointer=InMemorySaver()
    )

    run_graph(graph, "how many surveys?", thread_id="t-cont")
    run_graph(graph, "and per district?", thread_id="t-cont")

    second_turn_input = [m.content for m in model.seen[1]]
    assert "how many surveys?" in second_turn_input
    assert "You ran 1,284 surveys." in second_turn_input
    assert "and per district?" in second_turn_input


async def _data_route(question, model=None, history=None):
    return RouteResult()  # data_question / simple


async def _canned_reply(question, model=None):
    return "hi"


def test_validate_node_writes_validation_into_state():
    """After the agent answers, validate_node feeds THIS turn's SQL and result
    to the validator and the verdict lands in state (hence the checkpoint)."""
    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="1,284 surveys."),
        ]
    )
    agent = build_agent(model=model, human_in_the_loop=False)

    captured = {}

    async def fake_validate(**kwargs):
        captured.update(kwargs)
        return {"verdict": "warn", "assumptions": [], "caveat": "Counts visits, not farmers."}

    graph = build_turn_graph(
        agent,
        route_fn=_data_route,
        casual_reply_fn=_canned_reply,
        validate_fn=fake_validate,
        checkpointer=InMemorySaver(),
    )
    result = run_graph(graph, "how many farmers?", context=make_context(warehouse))

    assert result["validation"]["verdict"] == "warn"
    assert captured["question"] == "how many farmers?"
    assert captured["sql_queries"][0]["status"] == "success"
    assert captured["result_table"]["rows"] == [["1284"]]
    assert captured["answer"] == "1,284 surveys."


def test_graph_shape_matches_the_approach_2_diagram():
    """The pipeline is visible: every stage is a named node, and the data path
    is route → retrieve_context → sql_agent → validate. M5 fills the retrieve
    node with BM25 table cards; until then it is a no-op placeholder."""
    agent = build_agent(model=ScriptedChatModel(script=[]))
    graph = build_turn_graph(agent, route_fn=_data_route, casual_reply_fn=_canned_reply)
    drawable = graph.get_graph()

    assert {
        "route_node",
        "retrieve_context_node",
        "casual_reply_node",
        "clarify_node",
        "sql_agent",
        "validate_node",
    } <= set(drawable.nodes)

    edges = {(edge.source, edge.target) for edge in drawable.edges}
    assert ("__start__", "route_node") in edges
    assert ("route_node", "retrieve_context_node") in edges  # conditional: data path
    assert ("retrieve_context_node", "sql_agent") in edges
    assert ("sql_agent", "validate_node") in edges
    assert ("validate_node", "__end__") in edges
    assert ("casual_reply_node", "__end__") in edges
    assert ("clarify_node", "__end__") in edges


def test_platform_help_routes_to_guide_agent_and_skips_validation():
    """platform_help runs the guide agent and ends the turn — the SQL agent
    and the text-to-SQL validator must never fire."""
    from ddpui.core.ai.agent.platform_guide_agent import build_guide_agent

    saver = InMemorySaver()
    sql_agent = build_agent(checkpointer=saver, model=MustNotRun(script=[]))
    guide_agent = build_guide_agent(
        checkpointer=saver,
        model=ScriptedChatModel(script=[AIMessage(content="Here's how KPIs work.")]),
        human_in_the_loop=False,
    )

    async def help_route(question, model=None, history=None):
        return RouteResult(intent="platform_help")

    validations = []

    async def must_not_validate(**kwargs):
        validations.append(kwargs)
        return {"verdict": "ok"}

    graph = build_turn_graph(
        sql_agent,
        guide_agent,
        route_fn=help_route,
        casual_reply_fn=_canned_reply,
        validate_fn=must_not_validate,
        checkpointer=saver,
    )
    result = run_graph(graph, "how do I create a KPI?")

    assert result["messages"][-1].content == "Here's how KPIs work."
    assert validations == []  # guide path ends at END, not validate_node


def test_platform_help_without_guide_agent_falls_through_to_sql_agent():
    """Older callers (and focused tests) that pass no guide agent keep the v1
    behavior: platform_help degrades to the data path instead of crashing."""
    agent = build_agent(
        checkpointer=InMemorySaver(),
        model=ScriptedChatModel(script=[AIMessage(content="I can look at your data.")]),
        human_in_the_loop=False,
    )

    async def help_route(question, model=None, history=None):
        return RouteResult(intent="platform_help")

    graph = build_turn_graph(agent, route_fn=help_route, casual_reply_fn=_canned_reply)
    result = run_graph(graph, "how do I create a KPI?")

    assert result["messages"][-1].content == "I can look at your data."
