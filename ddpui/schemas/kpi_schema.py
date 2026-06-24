"""Schemas for KPI API endpoints"""

from datetime import datetime
from typing import Optional, List

from ninja import Schema
from pydantic import ConfigDict
from ddpui.schemas.metric_schema import MetricResponse
from ddpui.schemas.chart_schemas.customizations import NumberChartCustomizations


# ── KPI Schemas ─────────────────────────────────────────────────────────────


class KPIExtraConfig(Schema):
    """Typed container for ``KPI.extra_config``.

    ``extra_config`` itself is always a dict at DB + API layer (never null).
    ``customizations`` inside is optional — clients MUST check before reading.
    """

    model_config = ConfigDict(extra="forbid")

    customizations: Optional[NumberChartCustomizations] = None


class KPICreate(Schema):
    metric_id: int
    name: Optional[str] = None  # defaults to metric name
    target_value: Optional[float] = None
    direction: str  # "increase" or "decrease"
    green_threshold_pct: float = 100.0
    amber_threshold_pct: float = 80.0
    time_grain: str  # daily/weekly/monthly/quarterly/yearly
    time_dimension_column: Optional[str] = None
    metric_type_tag: Optional[str] = None
    program_tags: List[str] = []
    extra_config: KPIExtraConfig  # required; clients always send {} or full payload


class KPIUpdate(Schema):
    metric_id: Optional[int] = None
    name: Optional[str] = None
    target_value: Optional[float] = None
    direction: Optional[str] = None
    green_threshold_pct: Optional[float] = None
    amber_threshold_pct: Optional[float] = None
    time_grain: Optional[str] = None
    time_dimension_column: Optional[str] = None
    metric_type_tag: Optional[str] = None
    program_tags: Optional[List[str]] = None
    display_order: Optional[int] = None
    extra_config: KPIExtraConfig  # required on every update


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
    metric_type_tag: Optional[str]
    program_tags: List[str]
    display_order: int
    extra_config: KPIExtraConfig  # always present — DB column has default=dict, null=False
    created_by: str  # creator's email
    created_at: datetime
    updated_at: datetime


class KPIListResponse(Schema):
    data: List[KPIResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


# ── Annotation Schemas ─────────────────────────────────────────────────


class AnnotationEntryCreate(Schema):
    note_type: str
    period_key: str
    period_date: Optional[str] = None
    content: str
    snapshot_value: Optional[float] = None
    snapshot_pop_change: Optional[float] = None


class AnnotationEntryUpdate(Schema):
    note_type: Optional[str] = None
    period_key: Optional[str] = None
    period_date: Optional[str] = None
    content: Optional[str] = None
    snapshot_value: Optional[float] = None
    snapshot_pop_change: Optional[float] = None


class AnnotationEntryResponse(Schema):
    id: int
    note_type: str
    period_key: str
    period_date: Optional[str]
    content: str
    snapshot_value: Optional[float]
    snapshot_pop_change: Optional[float]
    created_by_email: str
    last_modified_by_email: str
    created_at: datetime
    updated_at: datetime
