"""Alert schemas for request/response validation"""

from typing import Optional, List, Any, Literal
from datetime import datetime
from ninja import Schema, Field


# --- Nested Schemas ---


class AlertFilterSchema(Schema):
    """Single filter condition"""

    column: str
    operator: str  # =, !=, >, <, >=, <=, contains, not contains, is true, is false
    value: str  # always string, cast at query time


class AlertQueryConfigSchema(Schema):
    """The full query configuration blob — validated before storage"""

    schema_name: str
    table_name: str
    filters: List[AlertFilterSchema] = []
    filter_connector: str = Field("AND", pattern="^(AND|OR)$")
    aggregation: str = Field(..., pattern="^(SUM|AVG|COUNT|MIN|MAX|FORMULA)$")
    measure_column: Optional[str] = None
    group_by_column: Optional[str] = None
    condition_operator: str = Field(..., pattern="^(>|<|>=|<=|=|!=)$")
    condition_value: float


MetricRagLevel = Literal["red", "amber", "green"]
AlertType = Literal["threshold", "rag", "standalone"]


class AlertRecipientSchema(Schema):
    """Typed recipient payload (email or Dalgo user reference)."""

    type: Literal["email", "user"]
    ref: str  # email string, or user_id as string


# --- Request Schemas ---


class AlertCreate(Schema):
    """Schema for creating an alert.

    alert_type is authoritative:
      - threshold: requires metric_id + query_config.condition_*
      - rag:       requires kpi_id + metric_rag_level
      - standalone: free-form query_config, no metric/kpi link
    """

    name: str = Field(..., min_length=1, max_length=255)
    alert_type: AlertType
    kpi_id: Optional[int] = None
    metric_id: Optional[int] = None
    metric_rag_level: Optional[MetricRagLevel] = None
    query_config: AlertQueryConfigSchema
    # Typed recipients; for backwards compatibility the API also accepts flat
    # email strings on create and normalizes them to {type: "email", ref: …}.
    recipients: List[AlertRecipientSchema | str] = Field(..., min_length=1)
    # Empty list = infer (current default). Non-empty = explicit deployment IDs.
    pipeline_triggers: List[str] = []
    # Null = "notify only on state change" (default). N = re-notify every N days.
    notification_cooldown_days: Optional[int] = Field(None, ge=0, le=365)
    message: str = ""
    group_message: str = ""


class AlertUpdate(Schema):
    """Schema for updating an alert (all fields optional)"""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    alert_type: Optional[AlertType] = None
    kpi_id: Optional[int] = None
    metric_id: Optional[int] = None
    metric_rag_level: Optional[MetricRagLevel] = None
    query_config: Optional[AlertQueryConfigSchema] = None
    recipients: Optional[List[AlertRecipientSchema | str]] = None
    pipeline_triggers: Optional[List[str]] = None
    notification_cooldown_days: Optional[int] = Field(None, ge=0, le=365)
    message: Optional[str] = None
    group_message: Optional[str] = None
    is_active: Optional[bool] = None


class AlertTestRequest(Schema):
    """For the Test Alert button — query config only with pagination"""

    alert_type: Optional[AlertType] = None
    kpi_id: Optional[int] = None
    metric_id: Optional[int] = None
    metric_rag_level: Optional[MetricRagLevel] = None
    query_config: AlertQueryConfigSchema
    message: str = ""
    group_message: str = ""
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


# --- Response Schemas ---


class AlertResponse(Schema):
    """Schema for alert response"""

    id: int
    name: str
    alert_type: AlertType
    kpi_id: Optional[int] = None
    kpi_name: Optional[str] = None
    metric_id: Optional[int] = None
    metric_name: Optional[str] = None
    metric_rag_level: Optional[MetricRagLevel] = None
    query_config: AlertQueryConfigSchema
    recipients: List[AlertRecipientSchema]
    pipeline_triggers: List[str] = []
    notification_cooldown_days: Optional[int] = None
    message: str
    group_message: str = ""
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_evaluated_at: Optional[datetime] = None
    last_fired_at: Optional[datetime] = None
    fire_streak: int = 0

    @classmethod
    def from_model(
        cls,
        alert,
        last_evaluated_at=None,
        last_fired_at=None,
        fire_streak=0,
    ):
        return cls(
            id=alert.id,
            name=alert.name,
            alert_type=alert.alert_type,
            kpi_id=alert.kpi_id,
            kpi_name=alert.kpi.name if alert.kpi_id else None,
            metric_id=alert.metric_id,
            metric_name=alert.metric.name if alert.metric_id else None,
            metric_rag_level=alert.metric_rag_level,
            query_config=AlertQueryConfigSchema(**alert.query_config),
            recipients=[r.to_dict() for r in alert.get_recipients()],
            pipeline_triggers=alert.pipeline_triggers or [],
            notification_cooldown_days=alert.notification_cooldown_days,
            message=alert.message,
            group_message=alert.group_message,
            is_active=alert.is_active,
            created_at=alert.created_at,
            updated_at=alert.updated_at,
            last_evaluated_at=last_evaluated_at,
            last_fired_at=last_fired_at,
            fire_streak=fire_streak,
        )


class AlertEvaluationResponse(Schema):
    """Schema for alert evaluation response"""

    id: int
    query_config: AlertQueryConfigSchema
    query_executed: str
    recipients: List[AlertRecipientSchema]
    num_recipients: int
    message: str
    fired: bool
    rows_returned: int
    result_preview: List[dict[str, Any]]
    rendered_message: str
    trigger_flow_run_id: Optional[str]
    # True iff a notification actually went out on this evaluation.
    notification_sent: bool = False
    # Provenance — when did the underlying pipeline last complete?
    last_pipeline_update: Optional[datetime] = None
    error_message: Optional[str]
    created_at: datetime

    @classmethod
    def from_model(cls, evaluation):
        from ddpui.models.alert import AlertRecipient

        typed_recipients = [AlertRecipient.from_any(r).to_dict() for r in (evaluation.recipients or [])]
        return cls(
            id=evaluation.id,
            query_config=AlertQueryConfigSchema(**evaluation.query_config),
            query_executed=evaluation.query_executed,
            recipients=typed_recipients,
            num_recipients=len(typed_recipients),
            message=evaluation.message,
            fired=evaluation.fired,
            rows_returned=evaluation.rows_returned,
            result_preview=evaluation.result_preview or [],
            rendered_message=evaluation.rendered_message,
            trigger_flow_run_id=evaluation.trigger_flow_run_id,
            notification_sent=evaluation.notification_sent,
            last_pipeline_update=evaluation.last_pipeline_update,
            error_message=evaluation.error_message,
            created_at=evaluation.created_at,
        )


class AlertTestResponse(Schema):
    """Schema for test alert response"""

    would_fire: bool
    total_rows: int
    results: List[dict[str, Any]]
    page: int
    page_size: int
    query_executed: str
    rendered_message: str


class TriggeredAlertEventResponse(Schema):
    """Recent fired alert event across the org."""

    id: int
    alert_id: int
    alert_name: str
    alert_type: AlertType
    kpi_id: Optional[int] = None
    kpi_name: Optional[str] = None
    metric_id: Optional[int] = None
    metric_name: Optional[str] = None
    metric_rag_level: Optional[MetricRagLevel] = None
    rows_returned: int
    num_recipients: int
    notification_sent: bool = False
    last_pipeline_update: Optional[datetime] = None
    rendered_message: str
    result_preview: List[dict[str, Any]]
    trigger_flow_run_id: Optional[str]
    created_at: datetime

    @classmethod
    def from_model(cls, evaluation):
        alert = evaluation.alert
        return cls(
            id=evaluation.id,
            alert_id=alert.id,
            alert_name=alert.name,
            alert_type=alert.alert_type,
            kpi_id=alert.kpi_id,
            kpi_name=alert.kpi.name if alert.kpi_id else None,
            metric_id=alert.metric_id,
            metric_name=alert.metric.name if alert.metric_id else None,
            metric_rag_level=alert.metric_rag_level,
            rows_returned=evaluation.rows_returned,
            num_recipients=len(evaluation.recipients) if evaluation.recipients else 0,
            notification_sent=evaluation.notification_sent,
            last_pipeline_update=evaluation.last_pipeline_update,
            rendered_message=evaluation.rendered_message,
            result_preview=evaluation.result_preview or [],
            trigger_flow_run_id=evaluation.trigger_flow_run_id,
            created_at=evaluation.created_at,
        )
