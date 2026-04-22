"""Schemas for Metric and KPI API endpoints"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema


# ── Metric Schemas ──────────────────────────────────────────────────────────


class MetricCreate(Schema):
    """Schema for creating a metric"""

    name: str
    description: Optional[str] = None
    schema_name: str
    table_name: str
    # Simple path (mutually exclusive with column_expression)
    column: Optional[str] = None
    aggregation: Optional[str] = None  # sum/avg/count/min/max/count_distinct
    # Expression path (mutually exclusive with column + aggregation)
    column_expression: Optional[str] = None


class MetricUpdate(Schema):
    """Schema for updating a metric"""

    name: Optional[str] = None
    description: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column: Optional[str] = None
    aggregation: Optional[str] = None
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
    """Response for metric consumers (charts and KPIs referencing this metric)"""

    charts: List[dict]  # [{id, title, chart_type}]
    kpis: List[dict]  # [{id, name}]
