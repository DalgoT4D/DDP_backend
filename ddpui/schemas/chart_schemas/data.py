"""Schemas for chart data fetch, transform, and preview endpoints."""

from typing import Dict, List, Optional

from ninja import Schema

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
    column_time_grains: Optional[
        Dict[str, str]
    ] = None  # {column_name: grain} e.g. {"enrollment_date": "month"}
    show_row_subtotals: bool = False
    show_column_subtotals: bool = False
    show_grand_total: bool = True

    customizations: Optional[dict] = None
    extra_config: Optional[dict] = None
    dashboard_filters: Optional[list[dict]] = None

    offset: int = 0
    limit: int = 100


class ChartDataResponse(Schema):
    """Schema for chart data response."""

    data: dict
    echarts_config: dict


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
