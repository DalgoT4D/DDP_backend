import json
from urllib.parse import parse_qs

from asgiref.sync import async_to_sync

from ddpui.celeryworkers.tasks import execute_dashboard_chat_turn
from ddpui.core.dashboard_chat.events import (
    build_dashboard_chat_event,
    dashboard_chat_group_name,
)
from ddpui.core.dashboard_chat.sessions.service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message_with_status,
    find_dashboard_chat_assistant_reply,
    get_or_create_dashboard_chat_session,
    serialize_dashboard_chat_message,
)
from ddpui.models.dashboard import Dashboard
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
        self.joined_session_groups = set()

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
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Invalid websocket payload",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        if payload.get("action") != "send_message":
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Unsupported websocket action",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        raw_message = str(payload.get("message") or "").strip()
        if not raw_message:
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Message is required",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        available, unavailable_message = self._chat_available()
        if not available:
            self.respond(
                WebsocketResponse(
                    data={},
                    message=unavailable_message,
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        try:
            session = get_or_create_dashboard_chat_session(
                orguser=self.orguser,
                dashboard=self.dashboard,
                session_id=payload.get("session_id"),
            )
        except DashboardChatSessionError as error:
            self.respond(
                WebsocketResponse(
                    data={},
                    message=str(error),
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        user_message_result = create_dashboard_chat_user_message_with_status(
            session=session,
            content=raw_message,
            client_message_id=payload.get("client_message_id"),
        )
        user_message = user_message_result.message
        self._subscribe_to_session(str(session.session_id))

        if not user_message_result.created:
            assistant_message = find_dashboard_chat_assistant_reply(
                session=session,
                user_message=user_message,
            )
            if assistant_message is not None:
                self.respond(
                    WebsocketResponse(
                        data=build_dashboard_chat_event(
                            event_type="assistant_message",
                            session_id=str(session.session_id),
                            dashboard_id=self.dashboard.id,
                            message_id=str(assistant_message.id),
                            data=serialize_dashboard_chat_message(assistant_message),
                        ),
                        message="",
                        status=WebsocketResponseStatus.SUCCESS,
                    )
                )
            return

        self.respond(
            WebsocketResponse(
                data=build_dashboard_chat_event(
                    event_type="progress",
                    session_id=str(session.session_id),
                    dashboard_id=self.dashboard.id,
                    message_id=str(user_message.id),
                    data={"label": "thinking"},
                ),
                message="",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

        try:
            result = execute_dashboard_chat_turn(str(session.session_id), user_message.id)
        except Exception:
            logger.exception(
                "dashboard chat turn failed inline for session=%s",
                session.session_id,
            )
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Something went wrong while generating the response",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        assistant_message = result.get("assistant_message")
        if result["status"] in {"completed", "skipped_existing_reply"} and assistant_message is not None:
            self.respond(
                WebsocketResponse(
                    data=build_dashboard_chat_event(
                        event_type="assistant_message",
                        session_id=str(session.session_id),
                        dashboard_id=self.dashboard.id,
                        message_id=str(assistant_message.id),
                        data=serialize_dashboard_chat_message(assistant_message),
                    ),
                    message="",
                    status=WebsocketResponseStatus.SUCCESS,
                )
            )
            return

        if result["status"] == "skipped_missing_session":
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Chat session could not be found",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        if result["status"] == "skipped_missing_message":
            self.respond(
                WebsocketResponse(
                    data={},
                    message="Chat message could not be found",
                    status=WebsocketResponseStatus.ERROR,
                )
            )
            return

        self.respond(
            WebsocketResponse(
                data={},
                message="Something went wrong while generating the response",
                status=WebsocketResponseStatus.ERROR,
            )
        )

    def websocket_disconnect(self, message):
        """Remove the socket from any joined session groups on disconnect."""
        if getattr(self, "channel_layer", None) is None:
            return
        for group_name in getattr(self, "joined_session_groups", set()):
            async_to_sync(self.channel_layer.group_discard)(group_name, self.channel_name)

    def dashboard_chat_event(self, event):
        """Forward dashboard chat events from the channel layer to the browser."""
        self.respond(
            WebsocketResponse(
                data=event["event"],
                message="",
                status=WebsocketResponseStatus.SUCCESS,
            )
        )

    def _subscribe_to_session(self, session_id: str) -> None:
        """Join the session-scoped channel-layer group if not already subscribed."""
        group_name = dashboard_chat_group_name(session_id)
        if group_name in self.joined_session_groups:
            return
        async_to_sync(self.channel_layer.group_add)(group_name, self.channel_name)
        self.joined_session_groups.add(group_name)

    def _chat_available(self) -> tuple[bool, str]:
        """Return whether the current org is ready for dashboard chat."""
        feature_enabled = get_all_feature_flags_for_org(self.orguser.org).get(
            "AI_DASHBOARD_CHAT", False
        )
        if not feature_enabled:
            return False, "Chat with dashboards is not enabled for this organization"

        org_preferences = OrgPreferences.objects.filter(org=self.orguser.org).first()
        if org_preferences is None or not org_preferences.ai_data_sharing_enabled:
            return False, "Chat with dashboards is not enabled for this organization"

        if self.orguser.org.dbt is None:
            return False, "Chat with dashboards is not available because dbt is not configured"

        if self.orguser.org.dbt.vector_last_ingested_at is None:
            return False, "Chat with dashboards is still being prepared for this organization"

        return True, ""

    def _has_permission(self, permission_slug: str) -> bool:
        """Check the authenticated orguser's role permission directly."""
        return RolePermission.objects.filter(
            role=self.orguser.new_role,
            permission__slug=permission_slug,
        ).exists()
