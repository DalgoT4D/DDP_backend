"""Alert models for Dalgo platform"""

from dataclasses import dataclass, field, asdict
from typing import Literal, Optional

from django.db import models
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.metrics import KPI, Metric


AlertType = Literal["threshold", "rag", "standalone"]
RecipientKind = Literal["email", "user"]


ALERT_TYPE_CHOICES = [
    ("threshold", "Metric Threshold"),
    ("rag", "KPI RAG"),
    ("standalone", "Standalone SQL"),
]


@dataclass
class AlertRecipient:
    """Typed recipient — stored inside Alert.recipients.

    `type="email"` → ref is a free-form email string (external stakeholders).
    `type="user"`  → ref is an OrgUser ID, resolved to that user's email at
    send time so we pick up address changes.
    """

    type: RecipientKind
    ref: str

    def to_dict(self) -> dict:
        return {"type": self.type, "ref": self.ref}

    @classmethod
    def from_any(cls, raw) -> "AlertRecipient":
        """Accepts either the typed dict or a legacy plain email string."""
        if isinstance(raw, str):
            return cls(type="email", ref=raw)
        if isinstance(raw, dict) and "type" in raw and "ref" in raw:
            return cls(type=raw["type"], ref=str(raw["ref"]))
        raise ValueError(f"Invalid recipient payload: {raw!r}")


@dataclass
class AlertFilterConfig:
    """Typed filter condition — stored inside query_config.filters"""

    column: str
    operator: str  # =, !=, >, <, >=, <=, contains, not contains, is true, is false
    value: str


@dataclass
class AlertQueryConfig:
    """
    Typed structure for Alert.query_config JSONField.
    This is the single source of truth for what the JSON blob contains.
    Always use Alert.get_query_config() / Alert.set_query_config() to read/write.
    """

    schema_name: str
    table_name: str
    aggregation: str  # SUM, AVG, COUNT, MIN, MAX
    condition_operator: str  # >, <, >=, <=, =, !=
    condition_value: float
    filters: list[AlertFilterConfig] = field(default_factory=list)
    filter_connector: str = "AND"  # AND or OR
    measure_column: Optional[str] = None  # null for COUNT(*)
    group_by_column: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to dict for JSONField storage"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AlertQueryConfig":
        """Deserialize from JSONField dict"""
        filters = [AlertFilterConfig(**f) for f in data.get("filters", [])]
        return cls(
            schema_name=data["schema_name"],
            table_name=data["table_name"],
            filters=filters,
            filter_connector=data.get("filter_connector", "AND"),
            aggregation=data["aggregation"],
            measure_column=data.get("measure_column"),
            group_by_column=data.get("group_by_column"),
            condition_operator=data["condition_operator"],
            condition_value=data["condition_value"],
        )


class Alert(models.Model):
    """Alert configuration"""

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.CASCADE,
        db_column="created_by",
        related_name="alerts_created",
    )
    # Which of the three variants this alert is. Set at create time.
    alert_type = models.CharField(
        max_length=20, choices=ALERT_TYPE_CHOICES, default="standalone"
    )

    # KPI RAG alerts populate `kpi` + `metric_rag_level`.
    # Metric-threshold alerts populate `metric` + query_config.condition_*.
    # Standalone alerts populate neither — query_config drives the SQL directly.
    kpi = models.ForeignKey(
        KPI,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="alerts",
    )
    metric = models.ForeignKey(
        Metric,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="alerts",
    )
    metric_rag_level = models.CharField(max_length=10, null=True, blank=True)

    # Typed via AlertQueryConfig dataclass — see get/set methods below
    query_config = models.JSONField()

    # Which pipelines trigger evaluation. Empty list = infer from the Metric's
    # source tables (for threshold/rag) or "all transform pipelines" fallback
    # (for standalone). Non-empty = explicit deployment_id list.
    pipeline_triggers = models.JSONField(default=list)

    # Notification cooldown in days. None = "notify only on state change"
    # (default). N = "re-notify every N days while still firing." Evaluations
    # still happen on every pipeline run; cooldown only gates the outgoing
    # notification.
    notification_cooldown_days = models.IntegerField(null=True, blank=True)

    # Delivery
    recipients = models.JSONField(default=list)
    message = models.TextField()
    group_message = models.TextField(default="")

    # Status
    is_active = models.BooleanField(default=True)

    # Timestamps
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "alert"
        ordering = ["-updated_at"]

    def __str__(self):
        return f"{self.name} ({self.id})"

    def get_query_config(self) -> AlertQueryConfig:
        """Deserialize query_config JSON into typed dataclass"""
        return AlertQueryConfig.from_dict(self.query_config)

    def set_query_config(self, config: AlertQueryConfig):
        """Serialize typed dataclass into query_config JSON"""
        self.query_config = config.to_dict()

    def get_recipients(self) -> list[AlertRecipient]:
        """Deserialize recipients into typed list, tolerating legacy strings."""
        return [AlertRecipient.from_any(r) for r in (self.recipients or [])]


class AlertEvaluation(models.Model):
    """Log of each alert evaluation — fully self-contained with config + query snapshots"""

    id = models.BigAutoField(primary_key=True)
    alert = models.ForeignKey(Alert, on_delete=models.CASCADE, related_name="evaluations")

    # Full snapshot of alert config at evaluation time
    query_config = models.JSONField()
    query_executed = models.TextField()
    recipients = models.JSONField(default=list)
    message = models.TextField(default="")

    # Result
    fired = models.BooleanField()
    rows_returned = models.IntegerField(default=0)
    result_preview = models.JSONField(default=list)
    rendered_message = models.TextField(default="")
    trigger_flow_run_id = models.TextField(null=True, blank=True)
    # True iff the evaluation's notification actually went out. False when
    # the alert fired but the cooldown suppressed the send.
    notification_sent = models.BooleanField(default=False)
    # When was the underlying pipeline last updated? Surfaces as "data freshness"
    # in the fired-alert detail view (Goalkeep ask).
    last_pipeline_update = models.DateTimeField(null=True, blank=True)

    # Error tracking
    error_message = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "alert_evaluation"
        ordering = ["-created_at"]

    def __str__(self):
        status = "FIRED" if self.fired else "OK"
        return f"Alert {self.alert_id} - {status} at {self.created_at}"

    def get_query_config(self) -> AlertQueryConfig:
        """Deserialize the config snapshot into typed dataclass"""
        return AlertQueryConfig.from_dict(self.query_config)
