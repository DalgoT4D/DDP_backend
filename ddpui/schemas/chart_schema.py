from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from ninja import Schema
from pydantic import BaseModel, ConfigDict, Field, model_validator


# The aggregations the chart pipeline supports. None is allowed too — it's
# valid when the metric carries a `column_expression` (raw SQL) instead.
ChartMetricAggregation = Literal["sum", "avg", "count", "min", "max", "count_distinct"]


class ChartMetric(Schema):
    """Schema for individual chart metric.

    Three valid shapes:
    1. `column_expression` set → raw SQL; `aggregation` / `column` ignored.
    2. `aggregation == "count"` → `column` may be None (COUNT(*)).
    3. Otherwise → both `aggregation` and `column` required.

    Rule (3) is enforced by the model_validator below; without it Pydantic
    would happily accept aggregation="sum" with column=None, which the SQL
    builder later rejects with a much worse error.
    """

    column: Optional[str] = None
    aggregation: Optional[ChartMetricAggregation] = None
    alias: Optional[str] = None
    column_expression: Optional[str] = None

    @model_validator(mode="after")
    def check_aggregation_column_pair(self) -> "ChartMetric":
        # Expression metrics bypass the aggregation/column requirement.
        if self.column_expression:
            return self
        if self.aggregation is None:
            raise ValueError(
                "metric requires either `aggregation` (with optional `column`) or `column_expression`"
            )
        # COUNT can stand alone (COUNT(*)); every other aggregation needs a column.
        if self.aggregation != "count" and not self.column:
            raise ValueError(f"metric with aggregation='{self.aggregation}' requires `column`")
        return self


# ── Shared filter / sort / pagination sub-schemas ──────────────────────────
# These are declared at the top of `extra_config` (not nested in
# `customizations`) for every chart_type. The UI always sends them as arrays
# / objects, even when empty.


# All operators the UI's filter editor exposes. Used by the query builder
# when translating filters into WHERE clauses.
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
    """A single filter clause on `extra_config.filters`.

    `value` is omitted for null-check operators (`is_null` / `is_not_null`);
    for `in` / `not_in` it is a list; otherwise a scalar. We don't enforce
    that here — the query builder coerces per operator.
    """

    model_config = ConfigDict(extra="allow")

    column: str
    operator: FilterOperator
    value: Optional[Any] = None


class ChartSort(BaseModel):
    """A single sort clause on `extra_config.sort`."""

    model_config = ConfigDict(extra="allow")

    column: str
    direction: Literal["asc", "desc"]


# Per-chart-type customizations — typed where the chart pipeline actually
# constrains values. `_ChartConfigBase.customizations` is plain dict for
# chart_types we don't constrain (bar/line/pie/table).


NumberFormat = Literal[
    "default",
    "percentage",
    "currency",
    "indian",
    "international",
    "european",
    "adaptive_international",
    "adaptive_indian",
]


class NumberChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='number'."""

    model_config = ConfigDict(extra="allow")

    numberFormat: Optional[NumberFormat] = None
    decimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)


MapColorScheme = Literal["Blues", "Reds", "Greens", "Purples", "Oranges", "Greys"]


class MapChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='map'."""

    model_config = ConfigDict(extra="allow")

    colorScheme: Optional[MapColorScheme] = None


class ChartPagination(BaseModel):
    """Pagination on `extra_config.pagination` — the persisted chart-level
    config (whether to paginate, page size). This is distinct from the
    request-level `offset` / `limit` on `ChartDataPayload`, which the data
    endpoint uses per query.
    """

    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    page_size: int = 50


class TableChartDimension(BaseModel):
    """A dimension entry on `TableChartConfig.dimensions`.

    This is the "rich" dimension shape introduced for the drill-down feature.
    The UI persists it alongside two older fields:

    - `TableChartConfig.dimension_columns: List[str]` — a flat list of the
      column names, written for backward compatibility. **This is the field
      the backend render path currently reads** (`build_chart_data_payload`
      and `normalize_dimensions` in `charts_service.py`).
    - `TableChartConfig.dimension_column: Optional[str]` — a scalar carried
      over from the universal chart payload shape. Not read for tables.

    `enable_drill_down` is currently only consumed for a log line in the
    create endpoint; the drill-down feature itself is wired up at the
    frontend. Future render-path migration could read `dimensions` directly.
    """

    model_config = ConfigDict(extra="allow")

    column: str
    enable_drill_down: bool = False


# ── Per-chart-type extra_config payloads ────────────────────────────────────
# These document the contract per chart_type for the LLM and for new
# callers. Required fields are enforced; everything else is Optional but
# declared so it shows up in OpenAPI / the agent's tool schema. Any UI field
# we missed still passes through via `extra="allow"`.


class _ChartConfigBase(BaseModel):
    """Fields every chart_type accepts on `extra_config`.

    Subclasses add per-type required fields on top.
    """

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


class LineChartConfig(BarChartConfig):
    """extra_config payload for chart_type='line'. Same shape as bar."""


class PieChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='pie'. Exactly one metric."""

    dimension_column: str
    metrics: List[ChartMetric] = Field(..., min_length=1, max_length=1)


class NumberChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='number'. One metric, no dimension."""

    metrics: List[ChartMetric] = Field(..., min_length=1, max_length=1)
    # Override the base's plain-dict customizations with a typed model that
    # enforces numberFormat / decimalPlaces.
    customizations: Optional[NumberChartCustomizations] = None


class MapChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='map'. Requires `geographic_column`
    and `selected_geojson_id`. `value_column` is Optional because COUNT(*)
    metrics don't carry a measured column — the deleted ChartValidator only
    enforced geographic_column + selected_geojson_id, and the UI omits
    value_column for count-based maps.
    """

    geographic_column: str
    value_column: Optional[str] = None
    selected_geojson_id: int
    customizations: Optional[MapChartCustomizations] = None


class TableChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='table'.

    Table charts come in two flavours and the render path enforces nothing
    on either, so all fields are Optional:

    - Raw-data table → populate `table_columns` with the columns to display.
    - Aggregated table → populate `dimensions` (+ optional `metrics`).

    `dimensions` is the canonical shape (per-row drill-down flags).
    `dimension_columns: List[str]` is the flat-list mirror the backend
    render path actually reads (`build_chart_data_payload` →
    `normalize_dimensions`). If the caller supplies `dimensions` without
    `dimension_columns`, the validator below backfills the mirror so the
    backend still sees a value. When the render path eventually reads
    `dimensions` directly, the derived field can be retired.

    The UI also writes a scalar `dimension_column` on every chart save
    (universal-payload leftover). The backend ignores it for tables; we
    let it pass through via `extra="allow"` rather than declaring it,
    keeping the LLM / OpenAPI contract focused on the fields that matter.
    """

    # Raw-data table shape
    table_columns: Optional[List[str]] = None

    # Aggregated table shape
    dimensions: Optional[List[TableChartDimension]] = None
    dimension_columns: Optional[List[str]] = None
    metrics: Optional[List[ChartMetric]] = None

    aggregate_function: Optional[str] = None

    @model_validator(mode="after")
    def derive_dimension_columns(self) -> "TableChartConfig":
        """Mirror `dimensions[].column` onto `dimension_columns` if the caller
        didn't already supply it. The backend render path reads
        `dimension_columns` today; this keeps that path working when a
        modern caller only sends the rich `dimensions` shape."""
        if self.dimensions and not self.dimension_columns:
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


class ChartCreate(Schema):
    """Schema for creating a chart.

    `extra_config` is typed per-chart-type — the actual sub-schema is chosen
    by the after-validator below (e.g. bar → BarChartConfig). Invalid
    combinations (bar without dimension_column, map without
    geographic_column, pie with > 1 metric, metric with aggregation='sum'
    and no column, etc.) are rejected here rather than deeper in the SQL
    builder.

    Each sub-schema declares the fields the render path requires; all other
    UI-sent fields pass through via `extra="allow"` so existing payloads
    are not lossy.

    Implementation note: `extra_config` is declared as `Any` rather than
    `Union[BarChartConfig, …]` because Pydantic's smart Union resolution
    would always pick `TableChartConfig` (no required fields → loosest
    match) for every payload, defeating per-type validation. We dispatch
    explicitly in `coerce_extra_config_to_typed` keyed on `chart_type`.
    """

    title: str
    description: Optional[str] = None
    chart_type: Literal["bar", "line", "pie", "number", "map", "table"]
    schema_name: str
    table_name: str
    extra_config: Any

    @model_validator(mode="after")
    def coerce_extra_config_to_typed(self) -> "ChartCreate":
        """Replace the raw `extra_config` dict with the typed sub-schema
        instance for `chart_type`. Raises ValidationError with field-level
        details on failure (e.g. metrics without dimension_column for bar).

        Rejects non-dict `extra_config` (None, list, string, int) — the field
        is annotated `Any` to bypass Union resolution, so the dict-shape
        check has to happen here rather than at the field level.
        """
        if not isinstance(self.extra_config, dict):
            raise ValueError("extra_config must be an object")
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None:
            # model_dump() on the result preserves extras because each
            # sub-schema sets extra="allow".
            self.extra_config = sub_schema(**self.extra_config)
        return self


class ChartUpdate(Schema):
    """Schema for updating a chart.

    All fields are Optional — callers can send any subset (e.g. just a title
    change). When both `chart_type` and `extra_config` are present the same
    per-type validation as `ChartCreate` is applied. When only `extra_config`
    is sent without `chart_type` we cannot infer which sub-schema to apply
    (the existing chart_type lives in the DB, not on the payload), so the
    dict passes through unchecked. In practice the chart-builder UI always
    sends both (see `app/charts/[id]/edit/page.tsx`), so the typed path is
    the live path.
    """

    title: Optional[str] = None
    description: Optional[str] = None
    chart_type: Optional[Literal["bar", "line", "pie", "number", "map", "table"]] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    extra_config: Optional[Any] = None

    @model_validator(mode="after")
    def coerce_extra_config_to_typed(self) -> "ChartUpdate":
        """Per-type validation runs only when both chart_type and extra_config
        are sent. Otherwise the dict passes through untouched.

        `extra_config` is annotated `Any` (to bypass Union resolution), so we
        enforce the dict-shape here. None remains valid — it means the caller
        is doing a partial update that doesn't touch extra_config.
        """
        if self.extra_config is not None and not isinstance(self.extra_config, dict):
            raise ValueError("extra_config must be an object")
        if self.chart_type is None or self.extra_config is None:
            return self
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None:
            self.extra_config = sub_schema(**self.extra_config)
        return self


class ChartResponse(Schema):
    """Schema for chart response"""

    id: int
    title: str
    description: Optional[str] = None
    chart_type: str
    schema_name: str
    table_name: str
    extra_config: dict  # Contains all column configuration and customizations
    # Note: render_config removed - charts fetch fresh config via /data endpoint
    created_at: datetime
    updated_at: datetime


class ChartConfig(Schema):
    """Chart configuration used to build a ChartDataPayload.

    Works for both Chart model instances and frozen report configs.
    """

    chart_type: str
    schema_name: str
    table_name: str
    title: Optional[str] = None
    extra_config: Optional[dict] = None


class ChartDataPayload(Schema):
    """Schema for chart data request"""

    chart_type: str
    schema_name: str
    table_name: str

    # For raw data
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None

    # For aggregated data
    dimension_col: Optional[
        str
    ] = None  # later we need to still merge dimension and extra dimension into dimensions list
    extra_dimension: Optional[str] = None
    dimensions: Optional[List[str]] = None  # Multiple dimensions for table charts

    # Multiple metrics for bar/line charts (optional for table charts)
    metrics: Optional[List[ChartMetric]] = None

    # Map-specific fields
    geographic_column: Optional[str] = None
    value_column: Optional[str] = None
    selected_geojson_id: Optional[int] = None

    # Customizations
    customizations: Optional[dict] = None

    # Extra config for filters and other settings
    extra_config: Optional[dict] = None

    # Dashboard filters
    dashboard_filters: Optional[list[dict]] = None

    # Pagination
    offset: int = 0
    limit: int = 100


class ChartDataResponse(Schema):
    """Schema for chart data response"""

    data: dict
    echarts_config: dict


class DataPreviewResponse(Schema):
    """Schema for data preview response"""

    columns: List
    column_types: dict
    data: List[dict]
    page: Optional[int] = 0
    page_size: Optional[int] = 100
    total_rows: Optional[int] = 0


class ExecuteChartQuery(Schema):
    chart_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    dimension_col: Optional[str] = None
    extra_dimension: Optional[str] = None
    dimensions: Optional[List[str]] = None  # Multiple dimensions for table charts

    # Metrics for aggregated charts
    metrics: Optional[List[ChartMetric]] = None


class TransformDataForChart(Schema):
    """Schema for transforming data for chart visualization"""

    chart_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    dimension_col: Optional[str] = None
    extra_dimension: Optional[str] = None
    dimensions: Optional[List[str]] = None  # Multiple dimensions for table charts

    # Metrics for aggregated charts
    metrics: Optional[List[ChartMetric]] = None

    # Map-specific fields
    geographic_column: Optional[str] = None  # Column containing region names
    value_column: Optional[str] = None  # Column with values to visualize
    selected_geojson_id: Optional[int] = None

    customizations: Optional[dict] = None

    # Time grain for formatting axis labels
    time_grain: Optional[str] = None


class GeoJSONListResponse(Schema):
    """Schema for GeoJSON list response"""

    id: int
    name: str
    display_name: str
    is_default: bool
    layer_name: str
    properties_key: str


class GeoJSONDetailResponse(Schema):
    """Schema for GeoJSON detail response"""

    id: int
    name: str
    display_name: str
    geojson_data: dict
    properties_key: str


class GeoJSONUpload(Schema):
    """Schema for uploading custom GeoJSON"""

    region_id: int
    name: str
    description: Optional[str] = None
    properties_key: str
    geojson_data: dict
