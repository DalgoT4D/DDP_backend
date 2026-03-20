import json
from urllib.parse import parse_qs

from asgiref.sync import async_to_sync

from ddpui.core.dashboard_chat.events import (
    build_dashboard_chat_event,
    dashboard_chat_group_name,
    publish_dashboard_chat_event,
)
from ddpui.core.dashboard_chat.session_service import (
    DashboardChatSessionError,
    create_dashboard_chat_user_message,
    get_or_create_dashboard_chat_session,
)
from ddpui.celeryworkers.tasks import run_dashboard_chat_turn
from ddpui.models.dashboard import Dashboard
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.role_based_access import RolePermission
from ddpui.utils.feature_flags import get_all_feature_flags_for_org
from ddpui.websockets import BaseConsumer

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
            self._respond_error("Invalid websocket payload")
            return

        if payload.get("action") != "send_message":
            self._respond_error("Unsupported websocket action")
            return

        raw_message = str(payload.get("message") or "").strip()
        if not raw_message:
            self._respond_error("Message is required")
            return

        available, unavailable_message = self._chat_available()
        if not available:
            self._respond_error(unavailable_message)
            return

        try:
            session = get_or_create_dashboard_chat_session(
                orguser=self.orguser,
                dashboard=self.dashboard,
                session_id=payload.get("session_id"),
            )
        except DashboardChatSessionError as error:
            self._respond_error(str(error))
            return

        user_message = create_dashboard_chat_user_message(
            session=session,
            content=raw_message,
            client_message_id=payload.get("client_message_id"),
        )
        self._subscribe_to_session(str(session.session_id))
        publish_dashboard_chat_event(
            str(session.session_id),
            build_dashboard_chat_event(
                event_type="progress",
                session_id=str(session.session_id),
                dashboard_id=self.dashboard.id,
                message_id=str(user_message.id),
                data={"label": "thinking"},
            ),
        )
        run_dashboard_chat_turn.delay(str(session.session_id), user_message.id)

    def websocket_disconnect(self, message):
        """Remove the socket from any joined session groups on disconnect."""
        if getattr(self, "channel_layer", None) is None:
            return
        for group_name in getattr(self, "joined_session_groups", set()):
            async_to_sync(self.channel_layer.group_discard)(group_name, self.channel_name)

    def dashboard_chat_event(self, event):
        """Forward dashboard chat events from the channel layer to the browser."""
        self.send(text_data=event["event"])

    def _subscribe_to_session(self, session_id: str) -> None:
        """Join the session-scoped channel-layer group if not already subscribed."""
        group_name = dashboard_chat_group_name(session_id)
        if group_name in self.joined_session_groups:
            return
        async_to_sync(self.channel_layer.group_add)(group_name, self.channel_name)
        self.joined_session_groups.add(group_name)

    def _respond_error(self, message: str) -> None:
        """Send one direct websocket error event."""
        self.send(
            text_data=json.dumps(
                build_dashboard_chat_event(
                    event_type="error",
                    dashboard_id=self.dashboard.id if getattr(self, "dashboard", None) else None,
                    data={"message": message},
                )
            )
        )

    def _chat_available(self) -> tuple[bool, str]:
        """Return whether the current org is ready for dashboard chat."""
        feature_enabled = get_all_feature_flags_for_org(self.orguser.org).get("AI_DASHBOARD_CHAT", False)
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
