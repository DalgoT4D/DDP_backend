"""Chart `extra_config` payloads: per-chart-type configs, shared sub-schemas, and dispatch."""

from typing import Any, List, Literal, Optional, Union

from ninja import Schema
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ddpui.schemas.chart_schemas.customizations import (
    BarChartCustomizations,
    LineChartCustomizations,
    MapChartCustomizations,
    NumberChartCustomizations,
    PieChartCustomizations,
    TableChartCustomizations,
)

# ── Shared sub-schemas (metrics, filters, sort, pagination, table dim) ──────


ChartMetricAggregation = Literal["sum", "avg", "count", "min", "max", "count_distinct"]


class ChartMetric(Schema):
    """One metric on `extra_config.metrics`.

    Valid shapes: (1) `column_expression` set → raw SQL; (2) `aggregation="count"`
    → column may be None; (3) otherwise both aggregation + column required.
    """

    column: Optional[str] = None
    aggregation: Optional[ChartMetricAggregation] = None
    alias: Optional[str] = None
    column_expression: Optional[str] = None
    saved_metric_id: Optional[int] = None

    @model_validator(mode="after")
    def check_aggregation_column_pair(self) -> "ChartMetric":
        if self.column_expression:
            return self
        if self.aggregation is None:
            raise ValueError(
                "metric requires either `aggregation` (with optional `column`) or `column_expression`"
            )
        if self.aggregation != "count" and not self.column:
            raise ValueError(f"metric with aggregation='{self.aggregation}' requires `column`")
        return self


FilterOperator = Literal[
    "equals",
    "not_equals",
    "greater_than",
    "less_than",
    "greater_than_equal",
    "less_than_equal",
    "like",
    "like_case_insensitive",
    "contains",
    "not_contains",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
]


class ChartFilter(BaseModel):
    """One filter clause on `extra_config.filters`."""

    model_config = ConfigDict(extra="allow")

    column: str
    operator: FilterOperator
    value: Optional[Any] = None


class ChartSort(BaseModel):
    """One sort clause on `extra_config.sort`."""

    model_config = ConfigDict(extra="allow")

    column: str
    direction: Literal["asc", "desc"]


class ChartPagination(BaseModel):
    """Persisted pagination config on `extra_config.pagination`."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    page_size: int = 50


class TableChartDimension(BaseModel):
    """One entry on `TableChartConfig.dimensions` — the rich shape with drill-down."""

    model_config = ConfigDict(extra="allow")

    column: str
    enable_drill_down: bool = False


# ── Per-chart-type extra_config payloads ────────────────────────────────────


class _ChartConfigBase(BaseModel):
    """Fields every chart_type accepts on `extra_config`."""

    model_config = ConfigDict(extra="allow")

    filters: Optional[List[ChartFilter]] = None
    sort: Optional[List[ChartSort]] = None
    pagination: Optional[ChartPagination] = None
    customizations: Optional[dict] = None


class BarChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='bar'."""

    dimension_column: str
    extra_dimension_column: Optional[str] = None
    metrics: List[ChartMetric] = Field(..., min_length=1)
    customizations: Optional[BarChartCustomizations] = None


class LineChartConfig(BarChartConfig):
    """extra_config payload for chart_type='line'."""

    customizations: Optional[LineChartCustomizations] = None


class PieChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='pie'. Exactly one metric."""

    dimension_column: str
    metrics: List[ChartMetric] = Field(..., min_length=1, max_length=1)
    customizations: Optional[PieChartCustomizations] = None


class NumberChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='number'. One metric, no dimension."""

    metrics: List[ChartMetric] = Field(..., min_length=1, max_length=1)
    customizations: Optional[NumberChartCustomizations] = None


class MapChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='map'.

    `value_column` is Optional because COUNT(*) maps don't carry a measured column.
    """

    geographic_column: str
    value_column: Optional[str] = None
    selected_geojson_id: int
    customizations: Optional[MapChartCustomizations] = None


class TableChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='table'.

    Raw-data (populate `table_columns`) or aggregated (populate `dimensions`
    + optional metrics). The render path reads `dimension_columns`; the
    after-validator backfills it from `dimensions` for modern callers.
    """

    table_columns: Optional[List[str]] = None

    dimensions: Optional[List[TableChartDimension]] = None
    dimension_columns: Optional[List[str]] = None
    metrics: Optional[List[ChartMetric]] = None

    aggregate_function: Optional[str] = None
    customizations: Optional[TableChartCustomizations] = None

    @model_validator(mode="after")
    def derive_dimension_columns(self) -> "TableChartConfig":
        if self.dimensions and self.dimension_columns is None:
            self.dimension_columns = [d.column for d in self.dimensions if d.column]
        return self


ChartExtraConfig = Union[
    BarChartConfig,
    LineChartConfig,
    PieChartConfig,
    NumberChartConfig,
    MapChartConfig,
    TableChartConfig,
]

_CHART_CONFIG_BY_TYPE: dict[str, type] = {
    "bar": BarChartConfig,
    "line": LineChartConfig,
    "pie": PieChartConfig,
    "number": NumberChartConfig,
    "map": MapChartConfig,
    "table": TableChartConfig,
}
