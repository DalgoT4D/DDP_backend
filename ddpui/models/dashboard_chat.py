import uuid
from enum import Enum

from django.db import models
from django.utils import timezone

from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class DashboardChatMessageRole(str, Enum):
    """Supported chat message roles for dashboard chat."""

    USER = "user"
    ASSISTANT = "assistant"

    @classmethod
    def choices(cls):
        return [(key.value, key.name) for key in cls]


class DashboardChatTurnStatus(models.TextChoices):
    """Lifecycle states for one dashboard-chat turn."""

    QUEUED = "queued", "Queued"
    RUNNING = "running", "Running"
    CANCEL_REQUESTED = "cancel_requested", "Cancel Requested"
    CANCELLED = "cancelled", "Cancelled"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"


class DashboardChatMessageFeedback(models.TextChoices):
    """Locked-in user feedback for one assistant answer."""

    THUMBS_UP = "thumbs_up", "Thumbs Up"
    THUMBS_DOWN = "thumbs_down", "Thumbs Down"


class DashboardChatPromptTemplateKey(models.TextChoices):
    """Runtime-editable prompt templates used by the dashboard chat LLM client."""

    INTENT_CLASSIFICATION = (
        "intent_classification",
        "Intent Classification",
    )
    NEW_QUERY_SYSTEM = (
        "new_query_system",
        "New Query System",
    )
    FOLLOW_UP_SYSTEM = (
        "follow_up_system",
        "Follow-up System",
    )
    FINAL_ANSWER_COMPOSITION = (
        "final_answer_composition",
        "Final Answer Composition",
    )
    SMALL_TALK_CAPABILITIES = (
        "small_talk_capabilities",
        "Small Talk Capabilities",
    )


class DashboardChatPromptTemplate(models.Model):
    """Database-backed prompt template for dashboard chat LLM calls."""

    key = models.CharField(
        max_length=64,
        unique=True,
        choices=DashboardChatPromptTemplateKey.choices,
    )
    prompt = models.TextField()
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["key"]


class OrgAIContext(models.Model):
    """Organization-level markdown context used by dashboard chat."""

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="ai_context")
    markdown = models.TextField(blank=True, default="")
    updated_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="org_ai_context_updates",
    )
    updated_at = models.DateTimeField(null=True, blank=True)


class DashboardAIContext(models.Model):
    """Dashboard-level markdown context used by dashboard chat."""

    dashboard = models.OneToOneField(
        Dashboard,
        on_delete=models.CASCADE,
        related_name="ai_context",
    )
    markdown = models.TextField(blank=True, default="")
    updated_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="dashboard_ai_context_updates",
    )
    updated_at = models.DateTimeField(null=True, blank=True)


class DashboardChatSession(models.Model):
    """Groups dashboard chat messages under one org/dashboard conversation."""

    session_id = models.UUIDField(editable=False, unique=True, default=uuid.uuid4)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    orguser = models.ForeignKey(OrgUser, null=True, on_delete=models.SET_NULL)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.SET_NULL, null=True)
    vector_collection_name = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dashboard_chat_session"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(
                fields=["org", "dashboard", "created_at"],
                name="dchat_sess_org_dash_idx",
            ),
        ]


class DashboardChatMessage(models.Model):
    """One user or assistant message within a dashboard chat session."""

    session = models.ForeignKey(
        DashboardChatSession,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    sequence_number = models.PositiveIntegerField()
    role = models.CharField(max_length=20, choices=DashboardChatMessageRole.choices())
    content = models.TextField(blank=True, default="")
    client_message_id = models.CharField(max_length=100, null=True, blank=True)
    payload = models.JSONField(null=True, blank=True)
    feedback = models.CharField(
        max_length=16,
        choices=DashboardChatMessageFeedback.choices,
        null=True,
        blank=True,
    )
    response_latency_ms = models.PositiveIntegerField(null=True, blank=True)
    timing_breakdown = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["sequence_number"]
        constraints = [
            models.UniqueConstraint(
                fields=["session", "sequence_number"],
                name="dchat_message_session_seq_unique",
            ),
            models.UniqueConstraint(
                fields=["session", "client_message_id"],
                condition=models.Q(client_message_id__isnull=False),
                name="dchat_message_session_client_msg_unique",
            ),
        ]


class DashboardChatTurn(models.Model):
    """Runtime state for one queued/running/completed dashboard-chat turn."""

    session = models.ForeignKey(
        DashboardChatSession,
        on_delete=models.CASCADE,
        related_name="turns",
    )
    user_message = models.OneToOneField(
        DashboardChatMessage,
        on_delete=models.CASCADE,
        related_name="turn",
    )
    assistant_message = models.OneToOneField(
        DashboardChatMessage,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="assistant_turn",
    )
    status = models.CharField(
        max_length=32,
        choices=DashboardChatTurnStatus.choices,
        default=DashboardChatTurnStatus.QUEUED,
    )
    progress_label = models.CharField(max_length=255, blank=True, default="")
    error_message = models.TextField(blank=True, default="")
    cancel_requested_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dashboard_chat_turn"
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["session", "status"], name="dchat_turn_session_status_idx"),
            models.Index(fields=["created_at"], name="dchat_turn_created_idx"),
        ]
