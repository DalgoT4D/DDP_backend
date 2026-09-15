"""Tests for the turn runner — the transport-independent streaming core.

Uses the scripted model + InMemorySaver, so the full event pipeline (tokens,
tool events, completion, audit row) runs without a WebSocket or API key.
"""

import asyncio
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.chat.turn_runner import run_turn
from ddpui.models.chat_with_data import ChatWithDataSession, ChatWithDataTurnAudit
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ddpui.tests.core.ai.test_agent_loop import (
    ScriptedChatModel,
    make_context,
    sql_call,
)
from ddpui.tests.core.ai.test_tools import FakeWarehouse

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def hermetic_router(monkeypatch):
    """Tests never construct the real router model (would hit the API when a
    key is present). Individual tests override with their own routes."""
    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import FAIL_OPEN

    async def fail_open_route(question, model=None, history=None):
        return FAIL_OPEN

    async def no_validation(**kwargs):
        return None

    monkeypatch.setattr(runner_module, "route_question", fail_open_route)
    monkeypatch.setattr(runner_module, "audit_turn", no_validation)


@pytest.fixture
def orguser(seed_db):
    user = User.objects.create(username="cwdrunner", email="cwdrunner@test.com", password="x")
    org = Org.objects.create(name="Runner Org", slug="runner-org", airbyte_workspace_id="w")
    ou = OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    )
    yield ou


@pytest.fixture
def session(orguser):
    return ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)


def collect_events(
    agent, session, orguser, question, context, resume_payload=None, guide_agent=None
):
    """Drive the async runner from sync tests (pytest-asyncio is inert on this
    pytest version — it needs pytest>=8)."""

    async def _collect():
        return [
            event
            async for event in run_turn(
                agent=agent,
                session=session,
                orguser=orguser,
                question=question,
                context=context,
                resume_payload=resume_payload,
                guide_agent=guide_agent,
            )
        ]

    return asyncio.run(_collect())


def test_run_turn_surfaces_friendly_error_and_audits_failure(orguser, session):
    class ExplodingModel(ScriptedChatModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            raise RuntimeError("anthropic 529: overloaded")

    agent = build_agent(checkpointer=InMemorySaver(), model=ExplodingModel(script=[]))
    events = collect_events(agent, session, orguser, "q?", make_context())

    assert events[-1]["type"] == "error"
    assert "529" not in events[-1]["message"]  # raw provider errors never reach users

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.status == "failed"


def test_run_turn_streams_events_and_writes_audit(orguser, session):
    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="You ran 1,284 surveys."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model, human_in_the_loop=False)

    events = collect_events(agent, session, orguser, "how many surveys?", make_context(warehouse))

    types = [e["type"] for e in events]
    # one tool round-trip then the final message
    assert types.count("tool_start") == 1
    assert types.count("tool_end") == 1
    assert types[-1] == "message_complete"
    assert types.index("tool_start") < types.index("tool_end") < types.index("message_complete")

    tool_start = events[types.index("tool_start")]
    assert tool_start["tool"] == "execute_sql"
    assert "SELECT" in tool_start["sql"]
    assert tool_start["label"]  # human-friendly label present

    complete = events[-1]
    assert complete["message"] == "You ran 1,284 surveys."
    assert complete["result_table"]["rows"] == [["1284"]]

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.user_message == "how many surveys?"
    assert audit.tools_called == ["execute_sql"]
    assert audit.sql_queries[0]["status"] == "success"
    assert audit.status == "completed"


def test_small_talk_short_circuits_the_agent(orguser, session, monkeypatch):
    """Greetings never reach the SQL agent: the router diverts them, a cheap
    reply comes back, and the exchange still lands in conversation memory."""
    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import RouteResult

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent="small_talk")

    async def fake_reply(question, model=None):
        return "You're welcome! Ask me anything about your data."

    monkeypatch.setattr(runner_module, "route_question", fake_route)
    monkeypatch.setattr(runner_module, "casual_reply", fake_reply)

    class MustNotRun(ScriptedChatModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            raise AssertionError("the SQL agent must not run for small talk")

    agent = build_agent(checkpointer=InMemorySaver(), model=MustNotRun(script=[]))
    events = collect_events(agent, session, orguser, "thanks!", make_context())

    assert [e["type"] for e in events] == ["message_complete"]
    assert "ask me anything" in events[0]["message"].lower()

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.intent["intent"] == "small_talk"
    assert audit.tools_called == []

    # the exchange is recorded in the thread so follow-ups keep context
    import asyncio

    state = asyncio.run(agent.aget_state({"configurable": {"thread_id": str(session.thread_id)}}))
    contents = [m.content for m in state.values["messages"]]
    assert "thanks!" in contents
    assert any("ask me anything" in str(c).lower() for c in contents)


def test_data_question_records_intent_on_audit(orguser, session, monkeypatch):
    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import RouteResult

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent="data_question", complexity="complex", entities=["surveys"])

    monkeypatch.setattr(runner_module, "route_question", fake_route)

    agent = build_agent(
        checkpointer=InMemorySaver(),
        model=ScriptedChatModel(script=[AIMessage(content="There are 12 surveys.")]),
    )
    context = make_context()
    events = collect_events(agent, session, orguser, "how many surveys?", context)

    assert events[-1]["type"] == "message_complete"
    assert context.complexity == "complex"  # reflection gate input (M4)

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.intent["complexity"] == "complex"
    assert audit.intent["entities"] == ["surveys"]


def test_validation_event_follows_message_complete(orguser, session, monkeypatch):
    from ddpui.core.ai.chat import turn_runner as runner_module

    captured = {}

    async def fake_validate(**kwargs):
        captured.update(kwargs)
        return {
            "verdict": "warn",
            "assumptions": ["counts rows"],
            "caveat": "This counts visit records, not unique farmers.",
        }

    monkeypatch.setattr(runner_module, "audit_turn", fake_validate)

    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="1,284 surveys."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model, human_in_the_loop=False)
    events = collect_events(agent, session, orguser, "how many farmers?", make_context(warehouse))

    types = [e["type"] for e in events]
    assert types.index("message_complete") < types.index("validation")
    validation = events[types.index("validation")]
    assert validation["verdict"] == "warn"
    assert "unique farmers" in validation["caveat"]

    assert captured["question"] == "how many farmers?"
    assert captured["sql_queries"][0]["status"] == "success"

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.validation["verdict"] == "warn"


def test_clarification_never_short_circuits_a_follow_up(orguser, session, monkeypatch):
    """Regression: 'can we create a chart of this?' after a prior answer was
    diverted by the context-blind router and the agent (which holds the
    memory) never ran. With history present, clarify routes fall through to
    the agent."""
    import asyncio

    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import RouteResult
    from langchain_core.messages import HumanMessage

    seen_history = {}

    async def fake_route(question, model=None, history=None):
        seen_history["history"] = history
        return RouteResult(intent="needs_clarification", clarification="Chart of what?")

    monkeypatch.setattr(runner_module, "route_question", fake_route)

    agent = build_agent(
        checkpointer=InMemorySaver(),
        model=ScriptedChatModel(script=[AIMessage(content="Here is the chart answer.")]),
    )
    config = {"configurable": {"thread_id": str(session.thread_id)}}
    # a prior exchange exists in the thread
    asyncio.run(
        agent.aupdate_state(
            config,
            {
                "messages": [
                    HumanMessage("list of top donors"),
                    AIMessage(content="Top donors: ..."),
                ]
            },
        )
    )

    events = collect_events(
        agent, session, orguser, "can we create a chart of this?", make_context()
    )

    # the agent ran (it answered) instead of the clarify short-circuit
    assert events[-1]["type"] == "message_complete"
    assert events[-1]["message"] == "Here is the chart answer."
    # and the router was shown the recent conversation
    assert any("top donors" in line.lower() for line in seen_history["history"])


def test_clarification_still_short_circuits_the_first_turn(orguser, session, monkeypatch):
    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import RouteResult

    async def fake_route(question, model=None, history=None):
        return RouteResult(intent="needs_clarification", clarification="Compare what to what?")

    monkeypatch.setattr(runner_module, "route_question", fake_route)

    class MustNotRun(ScriptedChatModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            raise AssertionError("agent must not run on a first-turn clarification")

    agent = build_agent(checkpointer=InMemorySaver(), model=MustNotRun(script=[]))
    events = collect_events(agent, session, orguser, "compare them", make_context())
    assert [e["type"] for e in events] == ["message_complete"]
    assert "Compare what to what?" in events[0]["message"]


def test_run_turn_attaches_created_charts_via_guide_agent(orguser, session, monkeypatch):
    """Creation lives on the guide agent now: a platform_help route runs the
    guide subgraph, and its create_chart call surfaces the chart chip on
    message_complete — same wire shape as before the split."""
    from ddpui.core.ai.agent.platform_guide_agent import build_guide_agent
    from ddpui.core.ai.chat import turn_runner as runner_module
    from ddpui.core.ai.llm_calls.router import RouteResult
    from ddpui.core.ai.tools import chart_tools

    class FakeChart:
        id = 42
        title = "Surveys by district"

    monkeypatch.setattr(chart_tools, "_save_chart", lambda ctx, data: FakeChart())

    async def platform_route(question, model=None, history=None):
        return RouteResult(intent="platform_help")

    monkeypatch.setattr(runner_module, "route_question", platform_route)

    context = make_context()
    context.orguser_id = orguser.id
    context.can_create_charts = True

    model = ScriptedChatModel(
        script=[
            AIMessage(
                "",
                tool_calls=[
                    {
                        "name": "create_chart",
                        "args": {
                            "title": "Surveys by district",
                            "chart_type": "bar",
                            "schema_name": "prod",
                            "table_name": "surveys",
                            "dimension_column": "district",
                        },
                        "id": "c1",
                    }
                ],
            ),
            AIMessage(content="Done — the chart is in your Charts page."),
        ]
    )
    saver = InMemorySaver()
    # the SQL agent is required but must never run on this route
    sql_agent = build_agent(
        checkpointer=saver, model=ScriptedChatModel(script=[]), human_in_the_loop=False
    )
    guide_agent = build_guide_agent(checkpointer=saver, model=model, human_in_the_loop=False)

    events = collect_events(
        sql_agent, session, orguser, "chart surveys by district", context, guide_agent=guide_agent
    )

    complete = events[-1]
    assert complete["type"] == "message_complete"
    assert complete["charts"] == [
        {"chart_id": 42, "title": "Surveys by district", "url_path": "/charts/42"}
    ]
    # the guide path never emits a validation event (validator is a SQL audit)
    assert not any(e["type"] == "validation" for e in events)

    audit = ChatWithDataTurnAudit.objects.get(session=session)
    assert audit.tools_called == ["create_chart"]


def test_run_turn_extracts_text_from_content_blocks(orguser, session):
    """Claude Sonnet 5 runs adaptive thinking by default: AIMessage.content is a
    LIST of blocks (a signed thinking block, then text). Only the text may reach
    the user — never the raw block reprs."""
    model = ScriptedChatModel(
        script=[
            AIMessage(
                content=[
                    {"type": "thinking", "thinking": "", "signature": "Eq8FCkYIBxgCKkB..."},
                    {"type": "text", "text": "You ran 1,284 surveys."},
                ]
            ),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model, human_in_the_loop=False)

    events = collect_events(agent, session, orguser, "how many surveys?", make_context())

    complete = events[-1]
    assert complete["type"] == "message_complete"
    assert complete["message"] == "You ran 1,284 surveys."
    assert "signature" not in complete["message"]

    for event in events:
        if event["type"] == "token":
            assert "signature" not in event["text"]
            assert not event["text"].startswith("[")  # no stringified block lists


def test_turn_pauses_for_approval_and_resume_completes_it(orguser, session):
    """A gated tool pauses the turn: input_required is the final event, the
    audit row records paused, and a resume run on the same session thread
    executes the approved query and finishes with message_complete."""
    from ddpui.core.ai.agent.hitl import build_resume_payload

    warehouse = FakeWarehouse(rows=[{"n": 1284}])
    model = ScriptedChatModel(
        script=[
            sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
            AIMessage(content="1,284 surveys."),
        ]
    )
    agent = build_agent(checkpointer=InMemorySaver(), model=model)
    context = make_context(warehouse)

    events = collect_events(agent, session, orguser, "how many surveys?", context)
    pause = events[-1]
    assert pause["type"] == "input_required"
    assert pause["kind"] == "approval"
    assert pause["requests"][0]["tool"] == "execute_sql"
    assert warehouse.executed == []  # nothing ran before approval
    assert ChatWithDataTurnAudit.objects.get(session=session).status == "paused"

    resume = build_resume_payload(pause["requests"], approve=True)
    events = collect_events(
        agent,
        session,
        orguser,
        "[user approved the action]",
        context,
        resume_payload=resume,
    )
    types = [e["type"] for e in events]
    assert "message_complete" in types
    assert events[types.index("message_complete")]["message"] == "1,284 surveys."
    assert len(warehouse.executed) == 1
