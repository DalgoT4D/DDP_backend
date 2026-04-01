import json
from urllib.parse import parse_qs

from django.utils import timezone

from ddpui.core.dashboard_chat.sessions.session_service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message_with_status,
    execute_dashboard_chat_turn,
    get_or_create_dashboard_chat_session,
    serialize_dashboard_chat_message,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardChatMessage, DashboardChatSession
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.role_based_access import RolePermission
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.feature_flags import get_all_feature_flags_for_org
from ddpui.websockets import BaseConsumer
from ddpui.websockets.schemas import WebsocketResponse, WebsocketResponseStatus

logger = CustomLogger("ddpui")


class DashboardChatConsumer(BaseConsumer):
    """Authenticated websocket for dashboard-level chat."""

    def connect(self):
        query_string = parse_qs(self.scope["query_string"].decode())
        token = query_string.get("token", [None])[0]
        orgslug = query_string.get("orgslug", [None])[0]

        if not self.authenticate_user(token, orgslug):
            self.close()
            return

        dashboard_id = self.scope.get("url_route", {}).get("kwargs", {}).get("dashboard_id")
        self.dashboard = Dashboard.objects.filter(id=dashboard_id, org=self.orguser.org).first()
        if self.dashboard is None or not self._has_permission("can_view_dashboards"):
            self.close()
            return

        self.accept()

    def websocket_receive(self, message):
        """Handle incoming dashboard chat websocket actions."""
        try:
            payload = json.loads(message["text"])
        except (KeyError, ValueError):
            self._send_error("Invalid websocket payload")
            return

        if payload.get("action") != "send_message":
            self._send_error("Unsupported websocket action")
            return

        raw_message = str(payload.get("message") or "").strip()
        if not raw_message:
            self._send_error("Message is required")
            return

        try:
            self._assert_chat_available()
        except Exception as error:
            self._send_error(str(error))
            return

        try:
            session = get_or_create_dashboard_chat_session(
                orguser=self.orguser,
                dashboard=self.dashboard,
                session_id=payload.get("session_id"),
            )
        except DashboardChatSessionError as error:
            self._send_error(str(error))
            return

        user_message = create_dashboard_chat_user_message_with_status(
            session=session,
            content=raw_message,
            client_message_id=payload.get("client_message_id"),
        ).message

        self._send_progress(session, user_message)

        try:
            assistant_message = execute_dashboard_chat_turn(
                str(session.session_id), user_message.id
            )
        except Exception:
            logger.exception("dashboard chat turn failed for session=%s", session.session_id)
            self._send_error("Something went wrong while generating the response")
            return

        self._send_assistant_message(session, assistant_message)

    # -------------------------------------------------------------------------
    # Response helpers
    # -------------------------------------------------------------------------

    def _send_progress(self, session: DashboardChatSession, user_message: DashboardChatMessage):
        self.respond(
            WebsocketResponse(
                status=WebsocketResponseStatus.SUCCESS,
                message="",
                data={
                    "event_type": "progress",
                    "session_id": str(session.session_id),
                    "message_id": str(user_message.id),
                    "dashboard_id": self.dashboard.id,
                    "occurred_at": timezone.now().isoformat(),
                },
            )
        )

    def _send_assistant_message(self, session: DashboardChatSession, message: DashboardChatMessage):
        self.respond(
            WebsocketResponse(
                status=WebsocketResponseStatus.SUCCESS,
                message="",
                data={
                    "event_type": "assistant_message",
                    "session_id": str(session.session_id),
                    "message_id": str(message.id),
                    "dashboard_id": self.dashboard.id,
                    "occurred_at": timezone.now().isoformat(),
                    **serialize_dashboard_chat_message(message),
                },
            )
        )

    def _send_error(self, message: str):
        self.respond(
            WebsocketResponse(
                status=WebsocketResponseStatus.ERROR,
                message=message,
                data={},
            )
        )

    # -------------------------------------------------------------------------
    # Guards
    # -------------------------------------------------------------------------

    def _assert_chat_available(self) -> None:
        """Raise Exception if the org is not ready for dashboard chat."""
        feature_enabled = get_all_feature_flags_for_org(self.orguser.org).get(
            "AI_DASHBOARD_CHAT", False
        )
        if not feature_enabled:
            raise Exception("Chat with dashboards is not enabled for this organization")

        org_preferences = OrgPreferences.objects.filter(org=self.orguser.org).first()
        if org_preferences is None or not org_preferences.ai_data_sharing_enabled:
            raise Exception("Chat with dashboards is not enabled for this organization")

        if self.orguser.org.dbt is None:
            raise Exception("Chat with dashboards is not available because dbt is not configured")

        if self.orguser.org.dbt.vector_last_ingested_at is None:
            raise Exception("Chat with dashboards is still being prepared for this organization")

    def _has_permission(self, permission_slug: str) -> bool:
        """Check the authenticated orguser's role permission directly."""
        return RolePermission.objects.filter(
            role=self.orguser.new_role,
            permission__slug=permission_slug,
        ).exists()
