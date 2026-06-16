"""Schemas for Alert API endpoints."""

from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from ninja import Schema


# ── Recipients ─────────────────────────────────────────────────────────────


class RecipientIn(Schema):
    type: Literal["orguser", "external"]
    orguser_id: Optional[int] = None
    email: Optional[str] = None


class RecipientOut(Schema):
    type: str
    orguser_id: Optional[int] = None
    orguser_name: Optional[str] = None
    email: Optional[str] = None


# ── Standalone source ──────────────────────────────────────────────────────


class FilterClause(Schema):
    column: str
    operator: str
    value: Any


class StandaloneConfig(Schema):
    schema_name: str
    table_name: str
    column: Optional[str] = None  # null for COUNT(*)
    aggregation: Optional[str] = None  # sum/avg/count/min/max/count_distinct (Simple)
    column_expression: Optional[str] = None  # Calculated mode (mutually exclusive)
    filters: List[FilterClause] = []


# ── Condition (discriminated Union by shape) ────────────────────────────────


class ThresholdCondition(Schema):
    operator: Literal["lt", "gt", "eq"]
    value: float


class RagCondition(Schema):
    rag_states: List[Literal["red", "amber", "green"]]


Condition = Union[ThresholdCondition, RagCondition]


# ── Create / Update payloads ───────────────────────────────────────────────


class AlertCreate(Schema):
    name: str
    alert_type: Literal["metric_threshold", "kpi_rag", "standalone"]
    metric_id: Optional[int] = None
    kpi_id: Optional[int] = None
    standalone_config: Optional[StandaloneConfig] = None
    condition: Condition

    schedule_cron: str  # validated by croniter at the service layer

    delivery_channels: List[Literal["email", "slack"]]
    slack_webhook_url: Optional[str] = None
    message_template: str  # single template — same body for email + Slack

    recipients: List[RecipientIn]


class AlertUpdate(Schema):
    name: Optional[str] = None
    # Source — only the field matching the alert's existing alert_type is honored;
    # alert_type itself is immutable. Switching alert types requires delete + recreate.
    metric_id: Optional[int] = None
    kpi_id: Optional[int] = None
    standalone_config: Optional[StandaloneConfig] = None
    condition: Optional[Condition] = None
    schedule_cron: Optional[str] = None
    delivery_channels: Optional[List[str]] = None
    slack_webhook_url: Optional[str] = None
    message_template: Optional[str] = None
    recipients: Optional[List[RecipientIn]] = None
    is_active: Optional[bool] = None


class AlertToggle(Schema):
    is_active: bool


# ── Response shapes ─────────────────────────────────────────────────────────


def mask_webhook_url(url: Optional[str]) -> Optional[str]:
    """Return a masked version of the Slack webhook URL safe for client display.

    Keeps the host part so the user can tell which provider; everything after
    `/services/` is masked.
    """
    if not url:
        return None
    if "/services/" in url:
        head, _ = url.split("/services/", 1)
        return f"{head}/services/****"
    return "****"


class AlertResponse(Schema):
    id: int
    name: str
    alert_type: str
    metric_id: Optional[int] = None
    metric_name: Optional[str] = None
    kpi_id: Optional[int] = None
    kpi_name: Optional[str] = None
    standalone_config: Optional[StandaloneConfig] = None
    condition: Condition
    schedule_cron: str
    delivery_channels: List[str]
    slack_webhook_url_masked: Optional[str] = None
    message_template: str
    is_active: bool
    last_evaluated_at: Optional[datetime] = None
    recipients: List[RecipientOut]
    created_at: datetime
    updated_at: datetime


class KpiRagContext(Schema):
    """KPI snapshot for an alert listing row — drives numeric band boundaries
    shown inside the RAG-chip tooltips, without dragging the full KPIResponse
    (and its nested MetricResponse) into the listing payload."""

    target_value: Optional[float] = None
    direction: str  # "increase" | "decrease"
    green_threshold_pct: float
    amber_threshold_pct: float


class AlertListItem(Schema):
    """Lightweight row for /alerts listing tabs."""

    id: int
    name: str
    alert_type: str
    source_kind: str  # "metric" | "kpi" | "dataset"
    source_id: Optional[int] = None
    source_name: Optional[str] = None
    condition_pretty: str  # e.g. "value < 50" / "RAG = red"
    # KPI-RAG alerts: the rag_states subset (e.g. ["red", "amber"]). Null for non-KPI types.
    # Lets the frontend render colored RAG chips instead of plain text.
    rag_states: Optional[List[str]] = None
    # KPI snapshot — only set for KPI alerts. Drives the numeric band boundaries
    # shown inside the RAG-chip tooltips.
    kpi_rag_context: Optional[KpiRagContext] = None
    schedule_frequency: str  # derived: "daily" | "weekly" | "monthly" | "cron"
    schedule_cron: str  # raw UTC cron expression — frontend renders it human-readable
    is_active: bool
    last_fire_at: Optional[datetime] = None
    fire_streak: int = 0


class AlertListResponse(Schema):
    data: List[AlertListItem]
    total: int
    page: int
    page_size: int
    total_pages: int


# ── Test (dry-run) endpoint ────────────────────────────────────────────────


class AlertTestRequest(Schema):
    """Inputs for wizard Step 3 dry-run. Same shape as AlertCreate (no recipients required)."""

    name: Optional[str] = None  # only used to render alert_name in the preview
    alert_type: str
    metric_id: Optional[int] = None
    kpi_id: Optional[int] = None
    standalone_config: Optional[StandaloneConfig] = None
    condition: Condition
    delivery_channels: List[str]
    message_template: str


class AlertTestResponse(Schema):
    would_fire: bool
    current_value: Optional[float] = None
    sql_executed: str
    message: str
    error: Optional[str] = None


# ── Slack webhook test ──────────────────────────────────────────────────────


class SlackTestRequest(Schema):
    webhook_url: str


class SlackTestResponse(Schema):
    success: bool
    http_status: int
    response_body: str


# ── Alert log (per-evaluation history) ──────────────────────────────────────


class LogDeliveryOut(Schema):
    channel: str
    target: str
    status: str
    error_reason: Optional[str] = None
    http_status: Optional[int] = None
    sent_at: datetime


class AlertLogOut(Schema):
    id: int
    scheduled_for: datetime
    evaluated_at: datetime
    value: Optional[float] = None
    fired: bool
    # KPI-RAG only — band the value landed in at evaluation time. Null for non-KPI types.
    rag_status: Optional[Literal["red", "amber", "green"]] = None
    condition_pretty: str
    sql_executed: str
    message: str
    deliveries: List[LogDeliveryOut]


class LogListResponse(Schema):
    data: List[AlertLogOut]
    total: int
    page: int
    page_size: int
    total_pages: int
