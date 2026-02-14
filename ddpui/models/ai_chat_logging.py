"""
AI Chat Logging and Metering models for tracking AI usage and conversations
"""

from django.db import models
from django.utils import timezone
from django.contrib.auth.models import User
from ddpui.models.org import Org


class AIChatLog(models.Model):
    """
    Logs AI chat conversations when ai_logging_acknowledged is enabled.
    Stores both user prompt and AI response in a single row for easy tracking.
    """

    org = models.ForeignKey(
        Org, on_delete=models.CASCADE, help_text="Organization this chat belongs to"
    )
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, help_text="User who initiated the chat"
    )

    # Chat context
    dashboard_id = models.IntegerField(
        help_text="Dashboard ID if chat is dashboard-specific", null=True, blank=True
    )
    chart_id = models.CharField(
        max_length=200, help_text="Chart ID if chat is chart-specific", null=True, blank=True
    )
    session_id = models.CharField(max_length=100, help_text="Chat session identifier")

    # Request-Response pair
    user_prompt = models.TextField(help_text="The user's original question or prompt")
    ai_response = models.TextField(help_text="The AI's complete response to the user prompt")

    # Metadata
    request_timestamp = models.DateTimeField(help_text="When the user sent the request")
    response_timestamp = models.DateTimeField(
        default=timezone.now, help_text="When the AI response was completed"
    )

    class Meta:
        db_table = "ai_chat_log"
        verbose_name = "AI Chat Log"
        verbose_name_plural = "AI Chat Logs"
        indexes = [
            models.Index(fields=["org", "response_timestamp"]),
            models.Index(fields=["user", "response_timestamp"]),
            models.Index(fields=["session_id"]),
        ]

    def __str__(self) -> str:
        return f"AIChatLog[{self.org.slug}|{self.user.email}|{self.session_id}|{self.response_timestamp}]"


class AIChatMetering(models.Model):
    """
    Meters AI chat usage for billing and monitoring purposes.
    Always logged regardless of user logging preferences.
    """

    org = models.ForeignKey(
        Org, on_delete=models.CASCADE, help_text="Organization this usage belongs to"
    )
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, help_text="User who initiated the request"
    )

    # Request context
    dashboard_id = models.IntegerField(
        help_text="Dashboard ID if request is dashboard-specific", null=True, blank=True
    )
    chart_id = models.CharField(
        max_length=200, help_text="Chart ID if request is chart-specific", null=True, blank=True
    )
    session_id = models.CharField(max_length=100, help_text="Chat session identifier")

    # Usage metrics
    model_used = models.CharField(
        max_length=100, help_text="AI model used (e.g., 'openai-gpt-4', 'anthropic-claude')"
    )
    prompt_tokens = models.IntegerField(help_text="Number of tokens in the prompt", default=0)
    completion_tokens = models.IntegerField(
        help_text="Number of tokens in the completion", default=0
    )
    total_tokens = models.IntegerField(help_text="Total tokens used", default=0)

    # Performance metrics
    response_time_ms = models.IntegerField(help_text="Response time in milliseconds")

    # Request details
    include_data = models.BooleanField(
        help_text="Whether data was included in the request", default=False
    )
    max_rows = models.IntegerField(
        help_text="Maximum rows included if data sharing enabled", null=True, blank=True
    )

    # Timestamps
    timestamp = models.DateTimeField(default=timezone.now, help_text="When this request was made")

    class Meta:
        db_table = "ai_chat_metering"
        verbose_name = "AI Chat Metering"
        verbose_name_plural = "AI Chat Metering"
        indexes = [
            models.Index(fields=["org", "timestamp"]),
            models.Index(fields=["user", "timestamp"]),
            models.Index(fields=["model_used", "timestamp"]),
        ]

    def __str__(self) -> str:
        return f"AIChatMetering[{self.org.slug}|{self.user.email}|{self.model_used}|{self.total_tokens}|{self.timestamp}]"
