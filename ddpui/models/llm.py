"""
All models related to ai/llm feature of Dalgo will go here
"""

from enum import Enum
from django.db import models
from django.utils import timezone
import uuid

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class LlmSessionStatus(str, Enum):
    """all possible statuses of a task progress"""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class DashboardChatSessionStatus(str, Enum):
    """Lifecycle states for dashboard chat sessions."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class LogsSummarizationType(str, Enum):
    """enum for log summarization types"""

    DEPLOYMENT = "deployment"
    AIRBYTE_SYNC = "airbyte_sync"


class LlmAssistantType(str, Enum):
    """enum for llm assistant types"""

    LOG_SUMMARIZATION = "log_summarization"
    LONG_TEXT_SUMMARIZATION = "long_text_summarization"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class AssistantPrompt(models.Model):
    """System prompts for various assistant/services"""

    prompt = models.TextField(null=False)
    type = models.CharField(null=False, choices=LlmAssistantType.choices(), max_length=100)


class LlmSession(models.Model):
    """Save response(s)/activities from llm service for a particular session"""

    request_uuid = models.UUIDField(editable=False, unique=True, default=uuid.uuid4)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    orguser = models.ForeignKey(OrgUser, null=True, on_delete=models.SET_NULL)
    flow_run_id = models.CharField(max_length=200, null=True)
    task_id = models.CharField(max_length=200, null=True)
    airbyte_job_id = models.IntegerField(null=True)
    assistant_prompt = models.TextField(null=True)
    user_prompts = models.JSONField(default=list, null=True)
    session_id = models.CharField(max_length=200, null=True)
    session_type = models.CharField(
        default=LlmAssistantType.LOG_SUMMARIZATION,
        choices=LlmAssistantType.choices(),
        max_length=100,
    )
    session_name = models.CharField(max_length=500, null=True)
    session_status = models.CharField(max_length=200, null=True)
    response = models.JSONField(
        null=True
    )  # one request might have multiple summaries; we store all of them as a json
    response_meta = models.JSONField(null=True)
    request_meta = models.JSONField(null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    feedback = models.TextField(null=True)
    updated_by = models.ForeignKey(
        OrgUser, null=True, on_delete=models.SET_NULL, related_name="updated_by"
    )


class UserPrompt(models.Model):
    """System defined user prompts for various assistant/services"""

    prompt = models.TextField(null=False)
    type = models.CharField(
        default=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
        choices=LlmAssistantType.choices(),
        max_length=100,
    )
    label = models.CharField(max_length=200, null=True)


class DashboardChatSession(models.Model):
    """Persist multi-turn dashboard chat state independently of legacy LLM sessions."""

    session_id = models.UUIDField(editable=False, unique=True, default=uuid.uuid4)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    orguser = models.ForeignKey(OrgUser, null=True, on_delete=models.SET_NULL)
    dashboard = models.ForeignKey("ddpui.Dashboard", on_delete=models.SET_NULL, null=True)
    selected_chart = models.ForeignKey(
        "ddpui.Chart", on_delete=models.SET_NULL, null=True, blank=True
    )
    status = models.CharField(
        max_length=50,
        choices=DashboardChatSessionStatus.choices(),
        default=DashboardChatSessionStatus.QUEUED,
    )
    messages = models.JSONField(default=list)
    latest_response = models.JSONField(null=True, blank=True)
    request_meta = models.JSONField(null=True, blank=True)
    response_meta = models.JSONField(null=True, blank=True)
    feedback = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
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
