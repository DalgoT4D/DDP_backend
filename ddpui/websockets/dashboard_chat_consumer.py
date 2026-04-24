import json
from urllib.parse import parse_qs

from asgiref.sync import async_to_sync
from django.utils import timezone

from ddpui.core.dashboard_chat.contracts.event_contracts import DashboardChatProgressStage
from ddpui.core.dashboard_chat.sessions.session_service import (
    DashboardChatSessionError,
    DASHBOARD_CHAT_SESSION_GROUP_PREFIX,
    create_dashboard_chat_user_message_with_status,
    get_or_create_dashboard_chat_session,
    publish_dashboard_chat_assistant_message,
    publish_dashboard_chat_cancelled,
    publish_dashboard_chat_progress,
    start_dashboard_chat_turn_background,
    update_dashboard_chat_turn,
)
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import (
    DashboardChatSession,
    DashboardChatTurn,
    DashboardChatTurnStatus,
)
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
        self.active_session_group = None
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

        action = payload.get("action")
        if action == "send_message":
            self._handle_send_message(payload)
            return
        if action == "cancel_message":
            self._handle_cancel_message(payload)
            return
        if action != "send_message":
            self._send_error("Unsupported websocket action")
            return

    def disconnect(self, code):
        if self.active_session_group is not None:
            async_to_sync(self.channel_layer.group_discard)(
                self.active_session_group,
                self.channel_name,
            )
        super().disconnect(code)

    def dashboard_chat_event(self, event):
        self.respond(
            WebsocketResponse(
                status=event["status"],
                message=event["message"],
                data=event["data"],
            )
        )

    def _handle_send_message(self, payload: dict) -> None:
        """Persist one user message, start one background turn, and stream progress via channels."""

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

        self._subscribe_to_session(session)

        user_message_result = create_dashboard_chat_user_message_with_status(
            session=session,
            content=raw_message,
            client_message_id=payload.get("client_message_id"),
        )
        turn, turn_created = DashboardChatTurn.objects.get_or_create(
            session=session,
            user_message=user_message_result.message,
            defaults={"status": DashboardChatTurnStatus.QUEUED},
        )
        turn = DashboardChatTurn.objects.select_related("assistant_message").get(id=turn.id)
        if not user_message_result.created and not turn_created:
            self._handle_existing_turn(session, turn)
            return
        publish_dashboard_chat_progress(
            session=session,
            turn=turn,
            label="Understanding question",
            stage=DashboardChatProgressStage.UNDERSTANDING_QUESTION,
            message_id=user_message_result.message.id,
        )
        update_dashboard_chat_turn(
            turn.id,
            progress_label="Understanding question",
        )
        start_dashboard_chat_turn_background(turn.id)

    def _handle_cancel_message(self, payload: dict) -> None:
        """Mark one queued/running dashboard-chat turn as cancelled or cancel-requested."""

        session_id = payload.get("session_id")
        if not session_id:
            self._send_error("session_id is required to cancel a message")
            return
        try:
            session = get_or_create_dashboard_chat_session(
                orguser=self.orguser,
                dashboard=self.dashboard,
                session_id=session_id,
            )
        except DashboardChatSessionError as error:
            self._send_error(str(error))
            return

        turn_id = payload.get("turn_id")
        turn_query = DashboardChatTurn.objects.filter(session=session)
        if turn_id is not None:
            try:
                turn_query = turn_query.filter(id=int(turn_id))
            except (TypeError, ValueError):
                self._send_error("turn_id must be an integer")
                return
        turn = turn_query.order_by("-created_at").first()
        if turn is None:
            self._send_error("No active chat turn found for this session")
            return

        if turn.status == DashboardChatTurnStatus.QUEUED:
            cancelled_turn = update_dashboard_chat_turn(
                turn.id,
                status=DashboardChatTurnStatus.CANCELLED,
                progress_label="Generation stopped",
                cancel_requested_at=timezone.now(),
                completed_at=timezone.now(),
            )
            publish_dashboard_chat_cancelled(session=session, turn=cancelled_turn)
            return

        if turn.status != DashboardChatTurnStatus.RUNNING:
            self._send_error("This chat turn can no longer be cancelled")
            return

        updated_turn = update_dashboard_chat_turn(
            turn.id,
            status=DashboardChatTurnStatus.CANCEL_REQUESTED,
            progress_label="Stopping...",
            cancel_requested_at=timezone.now(),
        )
        publish_dashboard_chat_progress(
            session=session,
            turn=updated_turn,
            label="Stopping...",
            stage=DashboardChatProgressStage.CANCELLING,
        )

    def _handle_existing_turn(
        self,
        session: DashboardChatSession,
        turn: DashboardChatTurn,
    ) -> None:
        """Reuse the existing turn state for duplicate client-message retries."""

        if (
            turn.status == DashboardChatTurnStatus.COMPLETED
            and turn.assistant_message is not None
        ):
            publish_dashboard_chat_assistant_message(
                session=session,
                turn=turn,
                message=turn.assistant_message,
            )
            return

        if turn.status in {
            DashboardChatTurnStatus.QUEUED,
            DashboardChatTurnStatus.RUNNING,
            DashboardChatTurnStatus.CANCEL_REQUESTED,
        }:
            publish_dashboard_chat_progress(
                session=session,
                turn=turn,
                label=turn.progress_label or "Understanding question",
                stage=None,
                message_id=turn.user_message_id,
            )
            return

        if turn.status == DashboardChatTurnStatus.CANCELLED:
            publish_dashboard_chat_cancelled(session=session, turn=turn)
            return

        self._send_error("This chat turn can no longer be retried")

    # -------------------------------------------------------------------------
    # Response helpers
    # -------------------------------------------------------------------------

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

    def _subscribe_to_session(self, session: DashboardChatSession) -> None:
        session_group = f"{DASHBOARD_CHAT_SESSION_GROUP_PREFIX}{session.session_id}"
        if self.active_session_group == session_group:
            return
        if self.active_session_group is not None:
            async_to_sync(self.channel_layer.group_discard)(
                self.active_session_group,
                self.channel_name,
            )
        async_to_sync(self.channel_layer.group_add)(session_group, self.channel_name)
        self.active_session_group = session_group
