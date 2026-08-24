"""Human-in-the-loop tests: approval pauses, ask_user questions, resume shapes.

Same recipe as test_agent_loop — real graph, real tools, scripted model — plus
an InMemorySaver so interrupts can checkpoint and Command(resume=...) works.
"""

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.agent.hitl import build_resume_payload, input_required_event
from ddpui.tests.core.ai.test_agent_loop import ScriptedChatModel, make_context, sql_call
from ddpui.tests.core.ai.test_tools import FakeWarehouse


def _invoke(agent, question, context, thread="t1"):
    config = {"configurable": {"thread_id": thread}}
    result = agent.invoke({"messages": [HumanMessage(question)]}, context=context, config=config)
    return result, config


def test_execute_sql_pauses_and_runs_after_approval():
    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="1,284 surveys."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model)
    context = make_context(warehouse)

    result, config = _invoke(agent, "how many surveys?", context)

    # paused: the interrupt carries the pending call, nothing has executed
    interrupt = result["__interrupt__"][0]
    requests = interrupt.value["action_requests"]
    assert [r["name"] for r in requests] == ["execute_sql"]
    assert warehouse.executed == []

    result = agent.invoke(
        Command(resume={"decisions": [{"type": "approve"}]}), context=context, config=config
    )
    assert result["messages"][-1].content == "1,284 surveys."
    assert len(warehouse.executed) == 1


def test_rejected_query_never_executes():
    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="Okay, I won't run that."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model)
    context = make_context(warehouse)

    result, config = _invoke(agent, "how many surveys?", context)
    assert "__interrupt__" in result

    result = agent.invoke(
        Command(resume={"decisions": [{"type": "reject"}]}), context=context, config=config
    )
    assert warehouse.executed == []  # the cancelled query never reached the warehouse
    # the model was told the call was not executed and answered without it
    assert result["messages"][-1].content == "Okay, I won't run that."


def test_ask_user_pauses_and_the_answer_becomes_the_tool_result():
    model = ScriptedChatModel(
        script=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ask_user",
                        "args": {"question": "Which program do you mean?"},
                        "id": "q1",
                    }
                ],
            ),
            AIMessage(content="For Girls' Education: 312 enrollments."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model)
    context = make_context()

    result, config = _invoke(agent, "how many enrollments?", context)

    interrupt = result["__interrupt__"][0]
    event = input_required_event(interrupt.value)
    assert event["kind"] == "question"
    assert event["question"] == "Which program do you mean?"

    resume = build_resume_payload(event["requests"], approve=True, answer="Girls' Education")
    result = agent.invoke(Command(resume=resume), context=context, config=config)

    # the human's reply came back to the model as the ask_user tool result
    tool_messages = [m for m in result["messages"] if m.type == "tool" and m.name == "ask_user"]
    assert tool_messages and tool_messages[-1].content == "Girls' Education"
    assert result["messages"][-1].content == "For Girls' Education: 312 enrollments."


def test_input_required_event_approval_kind_carries_sql():
    event = input_required_event(
        {
            "action_requests": [
                {
                    "name": "execute_sql",
                    "args": {"sql": "SELECT 1"},
                    "description": "Waiting for your go-ahead",
                }
            ],
            "review_configs": [],
        }
    )
    assert event == {
        "type": "input_required",
        "kind": "approval",
        "requests": [
            {
                "tool": "execute_sql",
                "args": {"sql": "SELECT 1"},
                "description": "Waiting for your go-ahead",
                "sql": "SELECT 1",
            }
        ],
    }


def test_build_resume_payload_matches_request_order_and_kinds():
    requests = [
        {"tool": "execute_sql", "args": {"sql": "SELECT 1"}},
        {"tool": "ask_user", "args": {"question": "which month?"}},
        {"tool": "create_chart", "args": {"title": "T"}},
    ]
    approved = build_resume_payload(requests, approve=True, answer="June")
    assert [d["type"] for d in approved["decisions"]] == ["approve", "respond", "approve"]
    assert approved["decisions"][1]["message"] == "June"

    rejected = build_resume_payload(requests, approve=False)
    assert [d["type"] for d in rejected["decisions"]] == ["reject", "respond", "reject"]


def test_ask_user_without_middleware_falls_back_to_its_body():
    """Evals and the REPL run with human_in_the_loop=False — ask_user must not
    hang them; its body tells the model to proceed on an assumption."""
    model = ScriptedChatModel(
        script=[
            AIMessage(
                content="",
                tool_calls=[{"name": "ask_user", "args": {"question": "Which year?"}, "id": "q1"}],
            ),
            AIMessage(content="Assuming 2026: 41 surveys."),
        ]
    )
    agent = build_agent(model=model, human_in_the_loop=False)
    result = agent.invoke({"messages": [HumanMessage("surveys?")]}, context=make_context())

    tool_messages = [m for m in result["messages"] if m.type == "tool" and m.name == "ask_user"]
    assert tool_messages and "No user is available" in tool_messages[-1].content
    assert result["messages"][-1].content == "Assuming 2026: 41 surveys."
