"""Pydantic schemas for the My Metrics API"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema


# ── Request schemas ──────────────────────────────────────────────────────────


class MetricCreate(Schema):
    name: str
    schema_name: str
    table_name: str
    column: str
    aggregation: str  # sum, avg, count, min, max, count_distinct

    # Time (optional)
    time_column: Optional[str] = None
    time_grain: str = "month"  # month, quarter, year

    # Target & RAG
    direction: str = "increase"  # "increase" or "decrease"
    target_value: Optional[float] = None
    amber_threshold_pct: float = 80
    green_threshold_pct: float = 100

    # Tags
    program_tag: str = ""
    metric_type_tag: str = ""  # Input, Output, Outcome, Impact

    # Trend
    trend_periods: int = 12
    display_order: int = 0


class MetricUpdate(Schema):
    name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column: Optional[str] = None
    aggregation: Optional[str] = None

    time_column: Optional[str] = None
    time_grain: Optional[str] = None

    direction: Optional[str] = None
    target_value: Optional[float] = None
    amber_threshold_pct: Optional[float] = None
    green_threshold_pct: Optional[float] = None

    program_tag: Optional[str] = None
    metric_type_tag: Optional[str] = None

    trend_periods: Optional[int] = None
    display_order: Optional[int] = None


class AnnotationCreate(Schema):
    period_key: str  # "2025-03", "2025-Q1", "2025"
    rationale: str = ""
    quote_text: str = ""
    quote_attribution: str = ""


class MetricDataRequest(Schema):
    """Request body for bulk metric data fetch"""

    metric_ids: List[int]


class LatestAnnotationsRequest(Schema):
    """Request body for bulk latest-annotation fetch"""

    metric_ids: List[int]


# ── Response schemas ─────────────────────────────────────────────────────────


class MetricResponse(Schema):
    id: int
    name: str
    schema_name: str
    table_name: str
    column: str
    aggregation: str

    time_column: Optional[str]
    time_grain: str

    direction: str
    target_value: Optional[float]
    amber_threshold_pct: float
    green_threshold_pct: float

    program_tag: str
    metric_type_tag: str

    trend_periods: int
    display_order: int

    created_at: datetime
    updated_at: datetime


class TrendPoint(Schema):
    period: str  # "2024-04", "2024-Q2", "2024"
    value: Optional[float]


class MetricDataPoint(Schema):
    """Live data for a single metric: current value + trend + RAG"""

    metric_id: int
    current_value: Optional[float]
    rag_status: str  # "green", "amber", "red", "grey"
    achievement_pct: Optional[float]
    trend: List[TrendPoint]
    error: Optional[str] = None  # non-null when warehouse query failed


class AnnotationResponse(Schema):
    id: int
    period_key: str
    rationale: str
    quote_text: str
    quote_attribution: str
    created_at: datetime
    updated_at: datetime


class LatestAnnotationEntry(Schema):
    """Latest annotation for a single metric (used in bulk response)"""

    metric_id: int
    id: int
    period_key: str
    rationale: str
    quote_text: str
    quote_attribution: str
    created_at: datetime
    updated_at: datetime


# ── Metric Entries (timeline) ───────────────────────────────────────────────


class EntryCreate(Schema):
    """Request body for creating a timeline entry"""

    entry_type: str  # "comment" or "quote"
    period_key: str  # "2026-03", "2026-Q1", "2026"
    content: str
    attribution: str = ""  # only relevant for quotes


class EntryResponse(Schema):
    """A single timeline entry with its metric snapshot"""

    id: int
    entry_type: str
    period_key: str
    content: str
    attribution: str
    snapshot_value: Optional[float]
    snapshot_rag: str
    snapshot_achievement_pct: Optional[float]
    created_by_name: str
    created_at: datetime
