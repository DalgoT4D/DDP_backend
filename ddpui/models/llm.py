"""
All models related to ai/llm feature of Dalgo will go here
"""

from enum import Enum
from django.db import models
import uuid

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


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
    type = models.CharField(
        null=False, choices=LlmAssistantType.choices(), max_length=100
    )


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
    session_status = models.CharField(max_length=200, null=True)
    response = models.JSONField(
        null=True
    )  # one request might have multiple summaries; we store all of them as a json
    response_meta = models.JSONField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
