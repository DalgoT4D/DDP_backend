"""Alert schemas for request/response validation"""

from typing import Optional, List
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


# --- Request Schemas ---


class AlertCreate(Schema):
    """Schema for creating an alert"""

    name: str = Field(..., min_length=1, max_length=255)
    query_config: AlertQueryConfigSchema
    cron: str = Field(..., min_length=1)
    recipients: List[str] = Field(..., min_length=1)  # at least one email required
    message: str = Field(..., min_length=1)


class AlertUpdate(Schema):
    """Schema for updating an alert (all fields optional)"""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    query_config: Optional[AlertQueryConfigSchema] = None
    cron: Optional[str] = None
    recipients: Optional[List[str]] = None
    message: Optional[str] = None
    is_active: Optional[bool] = None


class AlertTestRequest(Schema):
    """For the Test Alert button — query config only with pagination"""

    query_config: AlertQueryConfigSchema
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


# --- Response Schemas ---


class AlertResponse(Schema):
    """Schema for alert response"""

    id: int
    name: str
    query_config: dict
    cron: str
    recipients: list
    message: str
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
            query_config=alert.query_config,
            cron=alert.cron,
            recipients=alert.recipients,
            message=alert.message,
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
    query_config: dict
    query_executed: str
    cron: str
    recipients: list
    num_recipients: int
    message: str
    fired: bool
    rows_returned: int
    error_message: Optional[str]
    created_at: datetime

    @classmethod
    def from_model(cls, evaluation):
        return cls(
            id=evaluation.id,
            query_config=evaluation.query_config,
            query_executed=evaluation.query_executed,
            cron=evaluation.cron,
            recipients=evaluation.recipients,
            num_recipients=len(evaluation.recipients) if evaluation.recipients else 0,
            message=evaluation.message,
            fired=evaluation.fired,
            rows_returned=evaluation.rows_returned,
            error_message=evaluation.error_message,
            created_at=evaluation.created_at,
        )


class AlertTestResponse(Schema):
    """Schema for test alert response"""

    would_fire: bool
    total_rows: int
    results: list
    page: int
    page_size: int
    query_executed: str
