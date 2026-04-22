"""Pydantic schemas for the Metric primitive API (Batch 1)."""

from datetime import datetime
from typing import Optional, List, Literal, Any

from ninja import Schema, Field


# ── Nested building blocks ───────────────────────────────────────────────────


class MetricTermSchema(Schema):
    """Simple-mode term — one (agg, column) pair, referenced by ``id`` in the formula."""

    id: str = Field(..., pattern=r"^t\d+$")
    agg: Literal["sum", "avg", "count", "min", "max", "count_distinct"]
    column: str


class MetricFilterSchema(Schema):
    """Baked-in filter — always applied when the metric is evaluated."""

    column: str
    operator: str  # =, !=, >, <, >=, <=, contains, not contains
    value: str


# ── Requests ─────────────────────────────────────────────────────────────────


class MetricCreate(Schema):
    name: str = Field(..., min_length=1, max_length=255)
    description: str = ""
    tags: List[str] = []

    schema_name: str
    table_name: str
    time_column: Optional[str] = None
    default_time_grain: Literal["month", "quarter", "year"] = "month"

    creation_mode: Literal["simple", "sql"] = "simple"
    simple_terms: List[MetricTermSchema] = []
    simple_formula: str = ""
    sql_expression: str = ""
    filters: List[MetricFilterSchema] = []


class MetricUpdate(Schema):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    tags: Optional[List[str]] = None

    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    time_column: Optional[str] = None
    default_time_grain: Optional[Literal["month", "quarter", "year"]] = None

    creation_mode: Optional[Literal["simple", "sql"]] = None
    simple_terms: Optional[List[MetricTermSchema]] = None
    simple_formula: Optional[str] = None
    sql_expression: Optional[str] = None
    filters: Optional[List[MetricFilterSchema]] = None


class MetricDataRequest(Schema):
    metric_ids: List[int]
    # Whether to include a trend alongside the current value (library preview).
    include_trend: bool = False


class ValidateSqlRequest(Schema):
    schema_name: str
    table_name: str
    sql_expression: str
    filters: List[MetricFilterSchema] = []


# ── Responses ────────────────────────────────────────────────────────────────


class MetricResponse(Schema):
    id: int
    name: str
    description: str
    tags: List[str]

    schema_name: str
    table_name: str
    time_column: Optional[str]
    default_time_grain: Literal["month", "quarter", "year"]

    creation_mode: Literal["simple", "sql"]
    simple_terms: List[MetricTermSchema]
    simple_formula: str
    sql_expression: str
    filters: List[MetricFilterSchema]

    created_at: datetime
    updated_at: datetime


class MetricReferencesResponse(Schema):
    """Blast-radius summary — what breaks if this Metric changes?"""

    metric_id: int
    kpi_count: int
    alert_count: int
    # chart_count: reserved for the chart-builder rewire in Batch 6
    chart_count: int = 0
    kpi_ids: List[int] = []
    alert_ids: List[int] = []


class MetricDetailResponse(Schema):
    """Single-metric detail with references attached."""

    metric: MetricResponse
    references: MetricReferencesResponse


class TrendPoint(Schema):
    period: str
    value: Optional[float]


class MetricDataPoint(Schema):
    metric_id: int
    current_value: Optional[float]
    trend: List[TrendPoint] = []
    error: Optional[str] = None


class ValidateSqlResponse(Schema):
    ok: bool
    value: Optional[float] = None
    error: Optional[str] = None
    query_executed: Optional[str] = None
