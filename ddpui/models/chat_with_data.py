"""Models for Chat with Data sessions, audit, and per-org configuration.

Message content is NOT stored here — the LangGraph Postgres checkpointer is the
single source of truth for conversation content, keyed by thread_id.
"""

import uuid

from django.db import models
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class ChatWithDataSession(models.Model):
    """One chat conversation, owned by one user in one org."""

    id = models.BigAutoField(primary_key=True)
    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="chat_sessions")
    orguser = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="chat_sessions")
    title = models.CharField(max_length=255, default="New chat")
    thread_id = models.UUIDField(unique=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(null=True, blank=True)

    def soft_delete(self):
        self.deleted_at = timezone.now()
        self.save(update_fields=["deleted_at"])


class ChatWithDataTurnAudit(models.Model):
    """One row per user question: what was asked, what SQL ran, what it cost."""

    id = models.BigAutoField(primary_key=True)
    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="chat_turn_audits")
    orguser = models.ForeignKey(OrgUser, null=True, on_delete=models.SET_NULL)
    session = models.ForeignKey(
        ChatWithDataSession, on_delete=models.CASCADE, related_name="turn_audits"
    )
    request_uuid = models.UUIDField(default=uuid.uuid4, editable=False)
    user_message = models.TextField()
    # [{sql, status, row_count, duration_ms, error}] — one entry per execute_sql call
    sql_queries = models.JSONField(default=list)
    tools_called = models.JSONField(default=list)
    input_tokens = models.IntegerField(default=0)
    output_tokens = models.IntegerField(default=0)
    latency_ms = models.IntegerField(null=True)
    status = models.CharField(max_length=20, default="completed")  # completed|failed|aborted
    # router output: {intent, complexity, entities, clarification}
    intent = models.JSONField(null=True, blank=True)
    # post-execution validator output: {verdict, assumptions, caveat}
    validation = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)


class ChatWithDataOrgConfig(models.Model):
    """Per-org knobs, admin-managed (Django admin only in v1).

    allowed_schemas=NULL means "derive": the org's dbt output schema, falling
    back to all raw (non-system) schemas.
    """

    org = models.OneToOneField(Org, on_delete=models.CASCADE, related_name="chat_with_data_config")
    allowed_schemas = models.JSONField(null=True, blank=True)
    max_result_rows = models.IntegerField(default=100)
    query_timeout_s = models.IntegerField(default=30)
