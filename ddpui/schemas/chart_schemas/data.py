"""Schemas for chart data fetch, transform, and preview endpoints."""

from typing import Any, Dict, List, Optional

from ninja import Field, Schema

from ddpui.schemas.chart_schemas.config import ChartMetric


class ChartDataPayload(Schema):
    """Schema for chart data request."""

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

    metrics: Optional[List[ChartMetric]] = None

    # Map-specific fields
    geographic_column: Optional[str] = None
    value_column: Optional[str] = None
    selected_geojson_id: Optional[int] = None

    # Pivot table fields
    row_dimensions: Optional[List[str]] = None
    column_dimensions: Optional[List[str]] = None  # multiple column dimensions (pivot axes)
    show_row_subtotals: bool = False
    show_column_subtotals: bool = False
    # Independent grand totals (Excel model).
    show_row_grand_total: bool = False  # rightmost "Total" column (each row across cols)
    show_column_grand_total: bool = False  # bottom "Total" row (each col across rows)

    customizations: Optional[dict] = None
    extra_config: Optional[dict] = None
    dashboard_filters: Optional[list[dict]] = None

    offset: int = 0
    limit: int = 100


class ChartDataResponse(Schema):
    """Schema for chart data response."""

    data: dict
    echarts_config: dict


class MapDataOverlayPayload(Schema):
    """Schema for map data overlay requests (data layered onto a separately-fetched GeoJSON)."""

    schema_name: str
    table_name: str
    geographic_column: str
    value_column: str
    metrics: List[ChartMetric]
    filters: Dict[str, Any] = Field(default_factory=dict)  # Drill-down filters (key-value pairs)
    dashboard_filters: Optional[dict[str, Any]] = Field(
        default_factory=dict
    )  # Dashboard-level filters (dictionary of filter objects)
    extra_config: Optional[Dict[str, Any]] = Field(
        default_factory=dict
    )  # Additional configuration including chart-level filters, pagination, sorting, etc.


class DataPreviewResponse(Schema):
    """Schema for data preview response."""

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
    dimensions: Optional[List[str]] = None
    metrics: Optional[List[ChartMetric]] = None


class TransformDataForChart(Schema):
    """Schema for transforming data for chart visualization."""

    chart_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    dimension_col: Optional[str] = None
    extra_dimension: Optional[str] = None
    dimensions: Optional[List[str]] = None

    metrics: Optional[List[ChartMetric]] = None

    # Map-specific fields
    geographic_column: Optional[str] = None
    value_column: Optional[str] = None
    selected_geojson_id: Optional[int] = None

    customizations: Optional[dict] = None

    # Time grain for formatting axis labels
    time_grain: Optional[str] = None
