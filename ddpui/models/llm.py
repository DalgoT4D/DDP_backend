"""
All models related to ai/llm feature of Dalgo will go here
"""

from enum import Enum
from django.db import models


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
