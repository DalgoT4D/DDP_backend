"""Alert schemas for request/response validation"""

from typing import Optional, List, Any
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
    aggregation: str = Field(..., pattern="^(SUM|AVG|COUNT|MIN|MAX)$")
    measure_column: Optional[str] = None
    group_by_column: Optional[str] = None
    condition_operator: str = Field(..., pattern="^(>|<|>=|<=|=|!=)$")
    condition_value: float


class AlertMessagePlaceholderSchema(Schema):
    """Additional aggregated value used in alert messages."""

    key: str = Field(..., pattern="^[A-Za-z_][A-Za-z0-9_]*$")
    aggregation: str = Field(..., pattern="^(SUM|AVG|COUNT|MIN|MAX)$")
    column: Optional[str] = None


# --- Request Schemas ---


class AlertCreate(Schema):
    """Schema for creating an alert"""

    name: str = Field(..., min_length=1, max_length=255)
    metric_id: Optional[int] = None
    query_config: AlertQueryConfigSchema
    recipients: List[str] = Field(..., min_length=1)  # at least one email required
    message: str = ""
    group_message: str = ""
    message_placeholders: List[AlertMessagePlaceholderSchema] = []


class AlertUpdate(Schema):
    """Schema for updating an alert (all fields optional)"""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    metric_id: Optional[int] = None
    query_config: Optional[AlertQueryConfigSchema] = None
    recipients: Optional[List[str]] = None
    message: Optional[str] = None
    group_message: Optional[str] = None
    message_placeholders: Optional[List[AlertMessagePlaceholderSchema]] = None
    is_active: Optional[bool] = None


class AlertTestRequest(Schema):
    """For the Test Alert button — query config only with pagination"""

    metric_id: Optional[int] = None
    query_config: AlertQueryConfigSchema
    message: str = ""
    group_message: str = ""
    message_placeholders: List[AlertMessagePlaceholderSchema] = []
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


# --- Response Schemas ---


class AlertResponse(Schema):
    """Schema for alert response"""

    id: int
    name: str
    metric_id: Optional[int] = None
    metric_name: Optional[str] = None
    query_config: AlertQueryConfigSchema
    recipients: List[str]
    message: str
    group_message: str = ""
    message_placeholders: List[AlertMessagePlaceholderSchema] = []
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
            metric_id=alert.metric_id,
            metric_name=alert.metric.name if alert.metric_id else None,
            query_config=AlertQueryConfigSchema(**alert.query_config),
            recipients=alert.recipients,
            message=alert.message,
            group_message=alert.group_message,
            message_placeholders=[
                AlertMessagePlaceholderSchema(**placeholder)
                for placeholder in (alert.message_placeholders or [])
            ],
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
    recipients: List[str]
    num_recipients: int
    message: str
    fired: bool
    rows_returned: int
    result_preview: List[dict[str, Any]]
    rendered_message: str
    trigger_flow_run_id: Optional[str]
    error_message: Optional[str]
    created_at: datetime

    @classmethod
    def from_model(cls, evaluation):
        return cls(
            id=evaluation.id,
            query_config=AlertQueryConfigSchema(**evaluation.query_config),
            query_executed=evaluation.query_executed,
            recipients=evaluation.recipients,
            num_recipients=len(evaluation.recipients) if evaluation.recipients else 0,
            message=evaluation.message,
            fired=evaluation.fired,
            rows_returned=evaluation.rows_returned,
            result_preview=evaluation.result_preview or [],
            rendered_message=evaluation.rendered_message,
            trigger_flow_run_id=evaluation.trigger_flow_run_id,
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
    metric_id: Optional[int] = None
    metric_name: Optional[str] = None
    rows_returned: int
    num_recipients: int
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
            metric_id=alert.metric_id,
            metric_name=alert.metric.name if alert.metric_id else None,
            rows_returned=evaluation.rows_returned,
            num_recipients=len(evaluation.recipients) if evaluation.recipients else 0,
            rendered_message=evaluation.rendered_message,
            result_preview=evaluation.result_preview or [],
            trigger_flow_run_id=evaluation.trigger_flow_run_id,
            created_at=evaluation.created_at,
        )
