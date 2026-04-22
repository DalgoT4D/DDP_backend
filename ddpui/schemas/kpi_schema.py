"""Pydantic schemas for the KPI + KPIEntry API (Batch 1)."""

from datetime import datetime
from typing import Optional, List, Literal

from ninja import Schema, Field

from ddpui.schemas.metric_schema import MetricCreate, MetricResponse, TrendPoint


RAGStatus = Literal["green", "amber", "red", "grey"]


# ── Requests ─────────────────────────────────────────────────────────────────


class KPICreate(Schema):
    # Either link to an existing metric …
    metric_id: Optional[int] = None
    # … or define one inline (the creation form embeds MetricCreate)
    inline_metric: Optional[MetricCreate] = None

    target_value: Optional[float] = None
    direction: Literal["increase", "decrease"] = "increase"
    amber_threshold_pct: float = 80
    green_threshold_pct: float = 100

    # Grain + periods default to the Metric's `default_time_grain` / 12 if omitted.
    trend_grain: Optional[Literal["month", "quarter", "year"]] = None
    trend_periods: Optional[int] = None

    metric_type_tag: str = ""
    program_tag: str = ""
    tags: List[str] = []
    display_order: int = 0


class KPIUpdate(Schema):
    target_value: Optional[float] = None
    direction: Optional[Literal["increase", "decrease"]] = None
    amber_threshold_pct: Optional[float] = None
    green_threshold_pct: Optional[float] = None

    trend_grain: Optional[Literal["month", "quarter", "year"]] = None
    trend_periods: Optional[int] = None

    metric_type_tag: Optional[str] = None
    program_tag: Optional[str] = None
    tags: Optional[List[str]] = None
    display_order: Optional[int] = None


class KPIDataRequest(Schema):
    kpi_ids: List[int]


class KPIEntryCreate(Schema):
    entry_type: Literal["comment", "quote"]
    period_key: str = Field(..., min_length=1, max_length=20)
    content: str = Field(..., min_length=1)
    attribution: str = ""


class LatestEntriesRequest(Schema):
    kpi_ids: List[int]


# ── Responses ────────────────────────────────────────────────────────────────


class KPIResponse(Schema):
    id: int
    metric: MetricResponse

    target_value: Optional[float]
    direction: Literal["increase", "decrease"]
    amber_threshold_pct: float
    green_threshold_pct: float

    trend_grain: Literal["month", "quarter", "year"]
    trend_periods: int

    metric_type_tag: str
    program_tag: str
    tags: List[str]
    display_order: int

    created_at: datetime
    updated_at: datetime


class KPIDataPoint(Schema):
    kpi_id: int
    current_value: Optional[float]
    rag_status: RAGStatus
    achievement_pct: Optional[float]
    trend: List[TrendPoint]
    # Period-over-period change (last two completed periods). Spec § 7.
    period_over_period_delta: Optional[float] = None
    period_over_period_pct: Optional[float] = None
    # "last_pipeline_update" — surfaced as ISO timestamp string; populated by the API layer.
    last_pipeline_update: Optional[datetime] = None
    error: Optional[str] = None


class KPIEntryResponse(Schema):
    id: int
    entry_type: Literal["comment", "quote"]
    period_key: str
    content: str
    attribution: str
    snapshot_value: Optional[float]
    snapshot_rag: str
    snapshot_achievement_pct: Optional[float]
    created_at: datetime
    created_by_name: str = ""


class LatestEntryResponse(Schema):
    kpi_id: int
    entry: KPIEntryResponse
