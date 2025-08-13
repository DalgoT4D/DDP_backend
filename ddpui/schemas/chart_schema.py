from datetime import datetime
from typing import Optional, List

from ninja import Schema


class ChartCreate(Schema):
    """Schema for creating a chart"""

    title: str
    description: Optional[str] = None
    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str

    # All column configuration and customizations in config
    extra_config: dict


class ChartUpdate(Schema):
    """Schema for updating a chart"""

    title: Optional[str] = None
    description: Optional[str] = None
    chart_type: Optional[str] = None
    computation_type: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    extra_config: Optional[dict] = None


class ChartResponse(Schema):
    """Schema for chart response"""

    id: int
    title: str
    description: Optional[str]
    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str
    extra_config: dict  # Contains all column configuration and customizations
    # Note: render_config removed - charts fetch fresh config via /data endpoint
    created_at: datetime
    updated_at: datetime


class ChartDataPayload(Schema):
    """Schema for chart data request"""

    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str

    # For raw data
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None

    # For aggregated data
    dimension_col: Optional[str] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    extra_dimension: Optional[str] = None

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
    total_rows: int
    page: int
    page_size: int


class ExecuteChartQuery(Schema):
    computation_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    dimension_col: Optional[str] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    extra_dimension: Optional[str] = None


class TransformDataForChart(Schema):
    """Schema for transforming data for chart visualization"""

    chart_type: str
    computation_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    dimension_col: Optional[str] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    extra_dimension: Optional[str] = None
    customizations: Optional[dict] = None
