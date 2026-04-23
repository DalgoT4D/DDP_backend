"""Schemas for KPI API endpoints"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema
from ddpui.schemas.metric_schema import MetricResponse


# ── KPI Schemas ─────────────────────────────────────────────────────────────


class KPICreate(Schema):
    metric_id: int
    name: Optional[str] = None  # defaults to metric name
    target_value: Optional[float] = None
    direction: str  # "increase" or "decrease"
    green_threshold_pct: float = 100.0
    amber_threshold_pct: float = 80.0
    time_grain: str  # daily/weekly/monthly/quarterly/yearly
    time_dimension_column: Optional[str] = None
    trend_periods: int = 12
    metric_type_tag: Optional[str] = None
    program_tags: List[str] = []


class KPIUpdate(Schema):
    metric_id: Optional[int] = None
    name: Optional[str] = None
    target_value: Optional[float] = None
    direction: Optional[str] = None
    green_threshold_pct: Optional[float] = None
    amber_threshold_pct: Optional[float] = None
    time_grain: Optional[str] = None
    time_dimension_column: Optional[str] = None
    trend_periods: Optional[int] = None
    metric_type_tag: Optional[str] = None
    program_tags: Optional[List[str]] = None
    display_order: Optional[int] = None


class KPIResponse(Schema):
    id: int
    name: str
    metric: MetricResponse
    target_value: Optional[float]
    direction: str
    green_threshold_pct: float
    amber_threshold_pct: float
    time_grain: str
    time_dimension_column: Optional[str]
    trend_periods: int
    metric_type_tag: Optional[str]
    program_tags: List[str]
    display_order: int
    created_at: datetime
    updated_at: datetime


class KPIListResponse(Schema):
    data: List[KPIResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
