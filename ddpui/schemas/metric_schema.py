"""Schemas for Metric and KPI API endpoints"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema


# ── Metric Schemas ──────────────────────────────────────────────────────────


class MetricPayload(Schema):
    """Schema for creating or updating a metric"""

    name: str
    description: Optional[str] = None
    schema_name: str
    table_name: str
    # Simple path (mutually exclusive with column_expression)
    column: Optional[str] = None
    aggregation: Optional[str] = None  # sum/avg/count/min/max/count_distinct
    # Expression path (mutually exclusive with column + aggregation)
    column_expression: Optional[str] = None


class MetricResponse(Schema):
    """Serializes DB Metric model -> API response"""

    id: int
    name: str
    description: Optional[str]
    schema_name: str
    table_name: str
    column: Optional[str]
    aggregation: Optional[str]
    column_expression: Optional[str]
    created_by: str  # creator's email
    created_at: datetime
    updated_at: datetime


class MetricListResponse(Schema):
    """Paginated metric list response"""

    data: List[MetricResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class MetricPreviewResponse(Schema):
    """Response for metric value preview"""

    value: Optional[float] = None
    error: Optional[str] = None


class MetricConsumersResponse(Schema):
    """Response for metric consumers (charts, KPIs, alerts referencing this metric).

    Alerts are CASCADE on Metric delete — included so the UI can warn the user
    what will be deleted alongside; they do not contribute to delete-blocking.
    """

    charts: List[dict]  # [{id, title, chart_type}]
    kpis: List[dict]  # [{id, name}]
    alerts: List[dict] = []  # [{id, name, alert_type}]


class MetricValidateResponse(Schema):
    """Response for metric definition validation"""

    valid: bool
    error: Optional[str] = None
