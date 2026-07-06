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
from ddpui.core.chat_with_data.agent import build_agent
from ddpui.core.chat_with_data.runner import run_turn
from ddpui.models.chat_with_data import ChatWithDataSession, ChatWithDataTurnAudit
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ddpui.tests.core.chat_with_data.test_agent_loop import (
    ScriptedChatModel,
    make_context,
    sql_call,
)
from ddpui.tests.core.chat_with_data.test_tools import FakeWarehouse

pytestmark = pytest.mark.django_db(transaction=True)


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


def collect_events(agent, session, orguser, question, context):
    """Drive the async runner from sync tests (pytest-asyncio is inert on this
    pytest version — it needs pytest>=8)."""

    async def _collect():
        return [
            event
            async for event in run_turn(
                agent=agent, session=session, orguser=orguser, question=question, context=context
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
    agent = build_agent(checkpointer=InMemorySaver(), model=model)

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


def test_run_turn_attaches_created_charts(orguser, session, monkeypatch):
    """A create_chart tool call surfaces the chart link on message_complete."""
    from ddpui.core.chat_with_data.tools import chart_tools

    class FakeChart:
        id = 42
        title = "Surveys by district"

    monkeypatch.setattr(chart_tools, "_save_chart", lambda ctx, data: FakeChart())

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
    agent = build_agent(checkpointer=InMemorySaver(), model=model)

    events = collect_events(agent, session, orguser, "chart surveys by district", context)

    complete = events[-1]
    assert complete["type"] == "message_complete"
    assert complete["charts"] == [
        {"chart_id": 42, "title": "Surveys by district", "url_path": "/charts/42"}
    ]

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
    agent = build_agent(checkpointer=InMemorySaver(), model=model)

    events = collect_events(agent, session, orguser, "how many surveys?", make_context())

    complete = events[-1]
    assert complete["type"] == "message_complete"
    assert complete["message"] == "You ran 1,284 surveys."
    assert "signature" not in complete["message"]

    for event in events:
        if event["type"] == "token":
            assert "signature" not in event["text"]
            assert not event["text"].startswith("[")  # no stringified block lists
