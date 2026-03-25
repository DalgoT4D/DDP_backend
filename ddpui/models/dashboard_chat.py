import uuid
from enum import Enum

from django.core.cache import cache
from django.db import models
from django.utils import timezone

from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.core.dashboard_chat.prompt_cache import build_dashboard_chat_prompt_cache_key


class DashboardChatMessageRole(str, Enum):
    """Supported chat message roles for dashboard chat."""

    USER = "user"
    ASSISTANT = "assistant"

    @classmethod
    def choices(cls):
        return [(key.value, key.name) for key in cls]


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

    def save(self, *args, **kwargs):
        """Persist the prompt template and invalidate its runtime cache entry."""
        super().save(*args, **kwargs)
        cache.delete(build_dashboard_chat_prompt_cache_key(self.key))

    def delete(self, *args, **kwargs):
        """Delete the prompt template and invalidate its runtime cache entry."""
        cache_key = build_dashboard_chat_prompt_cache_key(self.key)
        super().delete(*args, **kwargs)
        cache.delete(cache_key)


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
