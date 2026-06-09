from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from ninja import Schema
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChartMetric(Schema):
    """Schema for individual chart metric"""

    column: Optional[str] = None  # Column name, null for COUNT(*) operations
    aggregation: Optional[str] = None  # SUM, COUNT, AVG, MAX, MIN, etc.
    alias: Optional[str] = None  # Display name for the metric
    # Expression path: raw SQL expression (e.g. "SUM(col_a) / COUNT(DISTINCT id)")
    # Mutually exclusive with column + aggregation
    column_expression: Optional[str] = None


# ── Shared filter / sort / pagination sub-schemas ──────────────────────────
# These are declared at the top of `extra_config` (not nested in
# `customizations`) for every chart_type. The UI always sends them as arrays
# / objects, even when empty.


# All operators the UI's filter editor exposes. Used by chart_validator and by
# the query builder when translating filters into WHERE clauses.
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
    """A dimension entry on `TableChartConfig.dimensions`."""

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


class MapChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='map'. Requires geographic + value cols."""

    geographic_column: str
    value_column: str
    selected_geojson_id: int


class TableChartConfig(_ChartConfigBase):
    """extra_config payload for chart_type='table'.

    Table charts come in two flavours and the chart_validator enforces
    nothing on either, so all fields are declared Optional. They are
    declared (rather than left as untyped extras) so the LLM / OpenAPI
    schema see the contract:

    - Raw-data table → populate `table_columns` with the columns to display.
    - Aggregated table → populate `dimensions` + `metrics`.

    Any combination is accepted at the schema layer; additional UI fields
    pass through via `extra="allow"`.
    """

    # Raw-data table shape
    table_columns: Optional[List[str]] = None

    # Aggregated table shape
    dimensions: Optional[List[TableChartDimension]] = None
    dimension_columns: Optional[List[str]] = None
    metrics: Optional[List[ChartMetric]] = None

    # Scalar dimension + aggregate the UI also sends on table-chart saves
    dimension_column: Optional[str] = None
    aggregate_function: Optional[str] = None


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
    geographic_column, pie with > 1 metric, etc.) are rejected at the schema
    layer rather than deeper in the chart_validator.

    Each sub-schema declares only the fields the downstream chart_validator
    actually requires; all other UI-sent fields pass through via
    `extra="allow"` so existing payloads are not lossy.

    Implementation note: `extra_config` is declared as `dict` rather than as
    a `Union[...]` so the per-chart-type dispatch is explicit (Pydantic's
    smart Union resolution would always pick TableChartConfig, which has no
    required fields, as the loose match for every payload).
    """

    title: str
    description: Optional[str] = None
    chart_type: Literal["bar", "line", "pie", "number", "map", "table"]
    schema_name: str
    table_name: str
    # Typed as Any at the field level — the actual runtime type is one of the
    # per-chart-type config models (BarChartConfig, MapChartConfig, etc.) set
    # by the validator below. Declaring it as a `Union[...]` here would let
    # Pydantic's smart Union resolution always pick TableChartConfig (no
    # required fields → loosest match), which defeats per-type validation.
    extra_config: Any

    @model_validator(mode="after")
    def coerce_extra_config_to_typed(self) -> "ChartCreate":
        """Validate the raw `extra_config` dict against the per-chart-type sub-schema.

        Raises ValidationError with field-level details if the user passed
        e.g. metrics without dimension_column for a bar chart.
        """
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None and isinstance(self.extra_config, dict):
            # .model_dump() on the result will preserve extras because each
            # sub-schema sets extra="allow".
            self.extra_config = sub_schema(**self.extra_config)
        return self


class ChartUpdate(Schema):
    """Schema for updating a chart.

    All fields are Optional — callers can send any subset (e.g. just a title
    change). When both `chart_type` and `extra_config` are present, the same
    per-type validation as `ChartCreate` is applied. When only `extra_config`
    is sent without `chart_type`, we cannot infer which sub-schema to apply
    (the existing chart_type lives in the DB, not on the payload), so the
    dict passes through and downstream `ChartValidator.validate_for_update`
    catches structural errors. In practice the chart-builder UI always sends
    both, so the typed path is the live path.
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
        are sent. Otherwise the dict passes through untouched."""
        if self.chart_type is None or self.extra_config is None:
            return self
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None and isinstance(self.extra_config, dict):
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
    dimension_col: Optional[str] = (
        None  # later we need to still merge dimension and extra dimension into dimensions list
    )
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
