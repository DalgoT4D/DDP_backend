"""WebSocket consumer for Chat with Data — the first async consumer in ddpui.

Async is required here: the agent turn streams tokens via astream, which must
yield on the event loop (the sync consumers block a worker thread per message,
which cannot stream). Auth mirrors BaseConsumer's cookie-JWT flow, with ORM
touches wrapped for async.

Per-message protocol (in):  {"action": "send_message", "message": "<question>"}
Events (out): see ddpui/core/ai/chat/turn_runner.py, plus
              {"type": "title_updated", "title": str}.
"""

import json
from http.cookies import SimpleCookie
from urllib.parse import parse_qs

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer
from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import AccessToken

from ddpui.core.ai.chat import sessions as service
from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.agent.checkpointer import get_checkpointer
from ddpui.core.ai.agent.context_builder import ChatWithDataNotReady, build_run_context
from ddpui.core.ai.chat.turn_runner import run_turn
from ddpui.core.ai.scopes.base import ScopeUnavailable
from ddpui.core.ai.llm_calls.session_title import generate_session_title
from ddpui.auth import orguser_has_permission
from ddpui.models.chat_with_data import ChatWithDataSession
from ddpui.models.org_user import OrgUser
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.redis_client import RedisClient
from ddpui.websockets.schemas import WebsocketCloseCodes

logger = CustomLogger("ddpui")

REQUIRED_PERMISSION = "can_use_chat_with_data"

# One user may send at most this many messages per minute (across sessions)
RATE_LIMIT_PER_MINUTE = 10
RATE_LIMIT_WINDOW_S = 60

# A crashed consumer's turn lock must not wedge the session forever
TURN_LOCK_TTL_S = 180

DEFAULT_SESSION_TITLE = "New chat"


class ChatWithDataConsumer(AsyncWebsocketConsumer):
    """One connection per chat session; runs agent turns and streams events."""

    async def connect(self):
        self.orguser: OrgUser | None = None
        self.session: ChatWithDataSession | None = None

        await self.accept()

        token = self._get_cookie("access_token")
        if not token:
            # webapp_v1's query-string token fallback is deliberately NOT
            # supported here — this feature ships on webapp_v2 (cookie) only
            logger.info("chat_with_data ws: no access_token cookie")
            await self.close(code=WebsocketCloseCodes.NO_TOKEN)
            return

        orgslug = self._query_param("orgslug")
        if not await self._authenticate(token, orgslug):
            await self.close(code=WebsocketCloseCodes.INVALID_TOKEN)
            return

        if not await self._authorize():
            await self.close(code=WebsocketCloseCodes.FORBIDDEN)
            return

        session_id = self.scope["url_route"]["kwargs"]["session_id"]
        try:
            self.session = await service.aget_session(self.orguser, session_id)
        except service.SessionNotFound:
            logger.info(f"chat_with_data ws: session {session_id} not found for user")
            await self.close(code=WebsocketCloseCodes.FORBIDDEN)
            return

    async def receive(self, text_data=None, bytes_data=None):
        try:
            payload = json.loads(text_data or "{}")
        except json.JSONDecodeError:
            await self._send_event({"type": "error", "message": "Invalid message format"})
            return

        if payload.get("action") != "send_message" or not str(payload.get("message", "")).strip():
            await self._send_event({"type": "error", "message": "Unsupported action"})
            return

        question = str(payload["message"]).strip()

        if not self._check_rate_limit():
            await self._send_event(
                {
                    "type": "error",
                    "message": "You're sending messages too quickly — give it a few seconds.",
                }
            )
            return

        if not self._acquire_turn_lock():
            await self._send_event(
                {"type": "error", "message": "I'm still working on your previous question."}
            )
            return

        try:
            await self._run_turn(question)
        finally:
            self._release_turn_lock()

    async def _run_turn(self, question: str):
        try:
            # session carries the scope; re-resolved every turn so dashboard
            # edits are picked up and a deleted dashboard errors politely
            context = await database_sync_to_async(build_run_context)(
                self.orguser, session=self.session
            )
        except (ChatWithDataNotReady, ScopeUnavailable) as err:
            await self._send_event({"type": "error", "message": str(err)})
            return

        checkpointer = await get_checkpointer()
        agent = build_agent(checkpointer=checkpointer)

        final_answer = ""
        async for event in run_turn(
            agent=agent,
            session=self.session,
            orguser=self.orguser,
            question=question,
            context=context,
        ):
            if event["type"] == "message_complete":
                final_answer = event.get("message", "")
            await self._send_event(event)

        if final_answer and self.session.title == DEFAULT_SESSION_TITLE:
            title = await generate_session_title(question, final_answer)
            if title:
                self.session.title = title
                await database_sync_to_async(self.session.save)(
                    update_fields=["title", "updated_at"]
                )
                await self._send_event({"type": "title_updated", "title": title})

    # ── auth helpers ────────────────────────────────────────────────────────

    async def _authenticate(self, token: str, orgslug: str | None) -> bool:
        try:
            payload = AccessToken(token).payload
        except Exception:  # pylint: disable=broad-except
            logger.info("chat_with_data ws: invalid/expired token")
            return False

        user_id = payload.get("user_id")
        if not user_id:
            return False

        self.orguser = await self._load_orguser(user_id, orgslug)
        return self.orguser is not None

    @database_sync_to_async
    def _load_orguser(self, user_id: int, orgslug: str | None) -> OrgUser | None:
        user = User.objects.filter(id=user_id).first()
        if user is None:
            return None
        queryset = OrgUser.objects.filter(user=user).select_related("org", "new_role", "user")
        if orgslug:
            queryset = queryset.filter(org__slug=orgslug)
        return queryset.first()

    @database_sync_to_async
    def _authorize(self) -> bool:
        """Permission + feature flag + AI consent + warehouse, in one DB hop."""
        if not orguser_has_permission(self.orguser, REQUIRED_PERMISSION):
            logger.info("chat_with_data ws: missing permission")
            return False
        status = service.get_status(self.orguser)
        if not status.enabled:
            logger.info(f"chat_with_data ws: not enabled ({status.reason})")
        return status.enabled

    # ── plumbing ────────────────────────────────────────────────────────────

    async def _send_event(self, event: dict):
        await self.send(text_data=json.dumps(event))

    def _get_cookie(self, name: str) -> str | None:
        for header_name, header_value in self.scope.get("headers", []):
            if header_name == b"cookie":
                cookie = SimpleCookie(header_value.decode())
                if name in cookie:
                    return cookie[name].value
        return None

    def _query_param(self, name: str) -> str | None:
        query = parse_qs(self.scope.get("query_string", b"").decode())
        return query.get(name, [None])[0]

    def _rate_key(self) -> str:
        return f"chat_with_data:rate:{self.orguser.id}"

    def _lock_key(self) -> str:
        return f"chat_with_data:turn_lock:{self.session.id}"

    def _check_rate_limit(self) -> bool:
        redis = RedisClient.get_instance()
        key = self._rate_key()
        count = redis.incr(key)
        if count == 1:
            redis.expire(key, RATE_LIMIT_WINDOW_S)
        return count <= RATE_LIMIT_PER_MINUTE

    def _acquire_turn_lock(self) -> bool:
        redis = RedisClient.get_instance()
        return bool(redis.set(self._lock_key(), "1", nx=True, ex=TURN_LOCK_TTL_S))

    def _release_turn_lock(self):
        RedisClient.get_instance().delete(self._lock_key())
