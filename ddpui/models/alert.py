"""Alert and AlertLog models for Dalgo platform."""

from django.db import models

from ddpui.models.metric import Metric, KPI
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser


class AlertType(models.TextChoices):
    METRIC_THRESHOLD = "metric_threshold", "Metric threshold"
    KPI_RAG = "kpi_rag", "KPI RAG"
    STANDALONE = "standalone", "Standalone"


# Condition shape (lives inside Alert.condition JSON; enforced at the API
# boundary by a discriminated Pydantic Union):
#   metric_threshold / standalone: {"operator": "lt"|"gt"|"eq", "value": float}
#   kpi_rag:                        {"rag_states": ["red"|"amber"|"green", ...]}


class Alert(models.Model):
    """A saved alert rule.

    All three alert types (metric_threshold, kpi_rag, standalone) share this
    table. `alert_type` discriminates which source fields are populated.
    """

    id = models.BigAutoField(primary_key=True)
    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="alerts")

    # Identity
    name = models.CharField(max_length=255)
    alert_type = models.CharField(max_length=20, choices=AlertType.choices)

    # Source (exactly one populated based on alert_type).
    # CASCADE — deleting the underlying Metric/KPI silently deletes its alerts
    # (and transitively their AlertLog rows). Per plan §8 open question 8.
    metric = models.ForeignKey(
        Metric,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="alerts",
    )
    kpi = models.ForeignKey(
        KPI,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="alerts",
    )

    # Standalone source — populated only for STANDALONE alerts.
    # Shape (validated at API boundary by StandaloneConfig Pydantic model):
    #   {
    #     "schema_name": str,
    #     "table_name": str,
    #     "column": str | null,             # null for COUNT(*)
    #     "aggregation": str | null,        # sum/avg/count/min/max/count_distinct
    #     "column_expression": str | null,  # Calculated mode
    #     "filters": [{"column": str, "operator": str, "value": Any}, ...]
    #   }
    standalone_config = models.JSONField(null=True, blank=True)

    # Condition — JSON shape depends on alert_type (see module docstring).
    condition = models.JSONField()

    # Schedule — UTC cron expression; frontend converts user's local-clock-time
    # + frequency + day to a cron at submit.
    schedule_cron = models.CharField(max_length=100)

    # Delivery configuration
    delivery_channels = models.JSONField(default=list)  # ["email"] or ["email", "slack"]
    slack_webhook_url = models.TextField(null=True, blank=True)
    # Single Mustache template — same body rendered for both email and Slack
    message_template = models.TextField()

    # Recipients — JSON list. Each entry:
    #   {"type": "orguser", "orguser_id": int}
    #   {"type": "external", "email": str}
    # Replaced wholesale on edit.
    recipients = models.JSONField(default=list)

    # State
    is_active = models.BooleanField(default=True)
    last_evaluated_at = models.DateTimeField(null=True, blank=True)  # UTC

    # Audit
    created_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE, related_name="alerts_created")
    last_modified_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="alerts_modified",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at"]
        constraints = [
            models.UniqueConstraint(fields=["org", "name"], name="unique_alert_name_per_org"),
        ]
        indexes = [
            models.Index(fields=["org", "is_active"]),
            models.Index(fields=["is_active", "last_evaluated_at"]),  # dispatcher query
        ]

    def __str__(self):
        return f"{self.name} ({self.alert_type})"


class AlertLog(models.Model):
    """One row per evaluation (whether or not it fired). Immutable history.

    Deliveries are stored as a JSON column on the row — there is no separate
    alert_delivery table. The Alert log modal always shows deliveries grouped
    under their evaluation.

    Summary delivery status (success / partial / failed / not_fired) is NOT
    stored — derive from `fired` + `deliveries` if/when the UI needs it.
    """

    id = models.BigAutoField(primary_key=True)
    alert = models.ForeignKey(Alert, on_delete=models.CASCADE, related_name="logs")

    scheduled_for = models.DateTimeField()  # UTC; the cron tick this evaluation is for
    evaluated_at = models.DateTimeField()  # UTC; when we actually ran the query
    value = models.FloatField(null=True, blank=True)  # null if query returned no rows
    fired = models.BooleanField()

    # Frozen audit copy of the alert at evaluation time. Shape:
    #   {
    #     "name": str,
    #     "alert_type": "metric_threshold" | "kpi_rag" | "standalone",
    #     "metric_id": int | null,
    #     "kpi_id": int | null,
    #     "condition": {...},
    #     "recipients": [...],
    #     "message_template": str
    #   }
    alert_snapshot = models.JSONField()
    sql_executed = models.TextField()
    # Rendered body — always populated (even on non-fires), so users can see
    # "what would have been sent if this had fired."
    message = models.TextField()

    # Deliveries as JSON. Each entry:
    #   {
    #     "channel": "email" | "slack",
    #     "target":  "recipient@example.com" | "slack:webhook",
    #     "status":  "sent" | "failed",
    #     "error_reason": str | null,
    #     "http_status": int | null,
    #     "sent_at": str (ISO 8601)
    #   }
    # Empty list when fired=False.
    deliveries = models.JSONField(default=list)

    class Meta:
        ordering = ["-evaluated_at"]
        indexes = [
            models.Index(fields=["alert", "-evaluated_at"]),  # log modal pagination
            models.Index(fields=["alert", "-evaluated_at", "fired"]),  # Firing tab
        ]

    def __str__(self):
        return f"AlertLog({self.alert_id}, {self.evaluated_at}, fired={self.fired})"
