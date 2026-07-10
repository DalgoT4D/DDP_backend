"""WebSocket consumer tests for Chat with Data.

Driven through Channels' WebsocketCommunicator against the real ASGI app
(auth via the access_token cookie, as webapp_v2 sends it). The agent itself is
scripted; the checkpointer is in-memory.
"""

import asyncio
import os
import uuid as uuid_lib

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from channels.testing import WebsocketCommunicator
from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import RefreshToken

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from django.core.management import call_command

from ddpui.websockets.chat_with_data_consumer import ChatWithDataConsumer
from ddpui.websockets.schemas import WebsocketCloseCodes

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def seed_db():
    """Re-seed roles/permissions per test — transaction=True flushes tables
    after every test, so the session-scoped seed_db from test_user_org_api
    would vanish after the first test in this module."""
    call_command("loaddata", "001_roles.json")
    call_command("loaddata", "002_permissions.json")
    call_command("loaddata", "003_role_permissions.json")


def make_communicator(session_id: int, token: str | None = None, orgslug: str = ""):
    headers = []
    if token:
        headers.append((b"cookie", f"access_token={token}".encode()))
    communicator = WebsocketCommunicator(
        ChatWithDataConsumer.as_asgi(),
        f"/wss/chat-with-data/{session_id}/?orgslug={orgslug}",
        headers=headers,
    )
    communicator.scope["url_route"] = {"kwargs": {"session_id": session_id}}
    return communicator


def run(coro):
    try:
        return asyncio.run(coro)
    finally:
        # async consumer work opens per-thread DB connections; close them so
        # the transaction=True flush isn't blocked by lingering sessions
        from django.db import connections

        connections.close_all()


@pytest.fixture
def orguser(seed_db):
    suffix = uuid_lib.uuid4().hex[:8]
    user = User.objects.create(
        username=f"cwdws-{suffix}", email=f"cwdws-{suffix}@test.com", password="x"
    )
    org = Org.objects.create(name="WS Org", slug=f"ws-org-{suffix}", airbyte_workspace_id="w")
    ou = OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    )
    yield ou
    ou.delete()
    org.delete()
    user.delete()


@pytest.fixture
def enabled_org(orguser):
    """Org with flag + consent + warehouse — the fully-enabled state."""
    from ddpui.models.org import OrgWarehouse
    from ddpui.models.org_preferences import OrgPreferences
    from ddpui.utils import feature_flags

    feature_flags.enable_feature_flag("CHAT_WITH_DATA", orguser.org)
    OrgPreferences.objects.create(org=orguser.org, llm_optin=True)
    OrgWarehouse.objects.create(org=orguser.org, wtype="postgres")
    return orguser.org


def token_for(orguser) -> str:
    return str(RefreshToken.for_user(orguser.user).access_token)


def test_connect_with_invalid_token_is_closed(seed_db):
    async def scenario():
        communicator = make_communicator(session_id=1, token="garbage")
        await communicator.connect()
        close = await communicator.receive_output()
        assert close["code"] == WebsocketCloseCodes.INVALID_TOKEN
        await communicator.disconnect()

    run(scenario())


def test_connect_closed_when_feature_not_enabled(orguser):
    # authenticated, permitted, but org flag is off
    session = ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(orguser), orgslug=orguser.org.slug
        )
        await communicator.connect()
        close = await communicator.receive_output()
        assert close["code"] == WebsocketCloseCodes.FORBIDDEN
        await communicator.disconnect()

    run(scenario())


def test_connect_closed_for_someone_elses_session(orguser, enabled_org):
    other_user = User.objects.create(
        username=f"cwdws2-{uuid_lib.uuid4().hex[:8]}", email="cwdws2@test.com", password="x"
    )
    other = OrgUser.objects.create(
        user=other_user,
        org=orguser.org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    session = ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(other), orgslug=orguser.org.slug
        )
        await communicator.connect()
        close = await communicator.receive_output()
        assert close["code"] == WebsocketCloseCodes.FORBIDDEN
        await communicator.disconnect()

    run(scenario())


class FakeRedis:
    """In-memory stand-in for RedisClient (rate counter + turn lock)."""

    def __init__(self):
        self.store = {}

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def expire(self, key, ttl):
        return True

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def delete(self, key):
        self.store.pop(key, None)


@pytest.fixture
def scripted_turn(monkeypatch, orguser, enabled_org):
    """Patch the consumer's seams: scripted agent, in-memory saver, fake redis,
    fixed title. Returns the session to chat against."""
    from langchain_core.messages import AIMessage
    from langgraph.checkpoint.memory import InMemorySaver

    from ddpui.core.chat_with_data.agent.build import build_agent as real_build_agent
    from ddpui.core.chat_with_data.agent.state import RunContext
    from ddpui.tests.core.chat_with_data.test_agent_loop import ScriptedChatModel, sql_call
    from ddpui.tests.core.chat_with_data.test_tools import FakeWarehouse
    from ddpui.websockets import chat_with_data_consumer as consumer_module

    session = ChatWithDataSession.objects.create(org=orguser.org, orguser=orguser)
    saver = InMemorySaver()

    def fake_build_agent(checkpointer=None, model=None):
        scripted = ScriptedChatModel(
            script=[
                sql_call("SELECT COUNT(*) AS n FROM prod.surveys", "c1"),
                AIMessage(content="1,284 surveys."),
            ]
        )
        return real_build_agent(checkpointer=saver, model=scripted)

    async def fake_get_checkpointer():
        return saver

    def fake_context(orguser_arg, session=None):
        return RunContext(
            org_id=orguser_arg.org.id,
            org_slug=orguser_arg.org.slug,
            dialect="postgres",
            allowed_schemas=["prod"],
            warehouse=FakeWarehouse(rows=[{"n": 1284}]),
        )

    async def fake_title(question, answer, model=None):
        return "Survey counts"

    from ddpui.core.chat_with_data import runner as runner_module
    from ddpui.core.chat_with_data.calls.router import FAIL_OPEN

    async def fail_open_route(question, model=None, history=None):
        return FAIL_OPEN

    monkeypatch.setattr(runner_module, "route_question", fail_open_route)

    fake_redis = FakeRedis()
    monkeypatch.setattr(consumer_module, "build_agent", fake_build_agent)
    monkeypatch.setattr(consumer_module, "get_checkpointer", fake_get_checkpointer)
    monkeypatch.setattr(consumer_module, "build_run_context", fake_context)
    monkeypatch.setattr(consumer_module, "generate_session_title", fake_title)
    monkeypatch.setattr(
        consumer_module.RedisClient, "get_instance", staticmethod(lambda: fake_redis)
    )
    return session


def test_full_turn_streams_events_and_updates_title(orguser, scripted_turn):
    session = scripted_turn

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(orguser), orgslug=orguser.org.slug
        )
        connected, _ = await communicator.connect()
        assert connected

        await communicator.send_json_to({"action": "send_message", "message": "how many?"})

        events = []
        while True:
            event = await communicator.receive_json_from(timeout=10)
            events.append(event)
            if event["type"] == "title_updated":
                break

        types = [e["type"] for e in events]
        assert "tool_start" in types and "tool_end" in types
        assert "message_complete" in types
        complete = events[types.index("message_complete")]
        assert complete["message"] == "1,284 surveys."
        assert complete["result_table"]["rows"] == [["1284"]]
        assert events[-1] == {"type": "title_updated", "title": "Survey counts"}

        await communicator.disconnect()

    run(scenario())
    session.refresh_from_db()
    assert session.title == "Survey counts"


def test_unsupported_action_yields_error_event(orguser, scripted_turn):
    session = scripted_turn

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(orguser), orgslug=orguser.org.slug
        )
        await communicator.connect()
        await communicator.send_json_to({"action": "nonsense"})
        event = await communicator.receive_json_from(timeout=5)
        assert event["type"] == "error"
        await communicator.disconnect()

    run(scenario())


def test_second_message_rejected_while_turn_in_flight(orguser, scripted_turn):
    from ddpui.websockets import chat_with_data_consumer as consumer_module

    session = scripted_turn
    # simulate an in-flight turn by holding the session's turn lock
    redis = consumer_module.RedisClient.get_instance()
    redis.set(f"chat_with_data:turn_lock:{session.id}", "1")

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(orguser), orgslug=orguser.org.slug
        )
        await communicator.connect()
        await communicator.send_json_to({"action": "send_message", "message": "how many?"})
        event = await communicator.receive_json_from(timeout=5)
        assert event["type"] == "error"
        assert "previous question" in event["message"]
        await communicator.disconnect()

    run(scenario())


def test_rate_limited_user_gets_error_event(orguser, scripted_turn):
    from ddpui.websockets import chat_with_data_consumer as consumer_module

    session = scripted_turn
    # user already at the per-minute message cap
    redis = consumer_module.RedisClient.get_instance()
    redis.store[f"chat_with_data:rate:{orguser.id}"] = consumer_module.RATE_LIMIT_PER_MINUTE

    async def scenario():
        communicator = make_communicator(
            session_id=session.id, token=token_for(orguser), orgslug=orguser.org.slug
        )
        await communicator.connect()
        await communicator.send_json_to({"action": "send_message", "message": "how many?"})
        event = await communicator.receive_json_from(timeout=5)
        assert event["type"] == "error"
        assert "too quickly" in event["message"]
        await communicator.disconnect()

    run(scenario())


def test_connect_without_token_is_closed(seed_db):
    async def scenario():
        communicator = make_communicator(session_id=1)
        connected, _ = await communicator.connect()
        assert connected  # accepted, then closed with an app code
        close = await communicator.receive_output()
        assert close["type"] == "websocket.close"
        assert close["code"] == WebsocketCloseCodes.NO_TOKEN
        await communicator.disconnect()

    run(scenario())
