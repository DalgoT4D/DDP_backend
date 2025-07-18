from ninja import Schema
from typing import Optional, Dict, Any, List, Union
from datetime import datetime


class ChartConfigSchema(Schema):
    """Chart configuration schema"""

    chartType: str
    computation_type: str
    xAxis: Optional[str] = None
    yAxis: Optional[str] = None
    dimensions: Optional[Union[str, List[str]]] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    aggregate_col_alias: Optional[str] = None
    dimension_col: Optional[str] = None


class ChartCreateSchema(Schema):
    """Schema for creating a new chart"""

    title: str
    description: Optional[str] = None
    chart_type: str
    schema_name: str
    table: str
    config: ChartConfigSchema
    is_public: Optional[bool] = False


class ChartUpdateSchema(Schema):
    """Schema for updating a chart"""

    title: Optional[str] = None
    description: Optional[str] = None
    chart_type: Optional[str] = None
    schema_name: Optional[str] = None
    table: Optional[str] = None
    config: Optional[ChartConfigSchema] = None
    is_public: Optional[bool] = None


class ChartUserSchema(Schema):
    """User schema for chart responses"""

    id: int
    email: str
    first_name: str
    last_name: str


class ChartCreatedBySchema(Schema):
    """Created by schema for chart responses"""

    id: int
    user: ChartUserSchema


class ChartResponseDataSchema(Schema):
    """Chart data schema for responses"""

    id: int
    title: str
    description: Optional[str]
    chart_type: str
    schema_name: str
    table: str
    config: Dict[str, Any]
    is_public: bool
    is_favorite: bool
    created_at: str
    updated_at: str
    created_by: Optional[ChartCreatedBySchema]


class ChartResponseSchema(Schema):
    """Schema for chart API responses"""

    success: bool
    data: Optional[ChartResponseDataSchema] = None
    message: str
    error: Optional[str] = None


class ChartListResponseSchema(Schema):
    """Schema for chart list responses"""

    success: bool
    data: List[ChartResponseDataSchema]
    message: str
    error: Optional[str] = None


class ChartGenerateSchema(Schema):
    """Schema for generating chart data"""

    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str
    xaxis: Optional[str] = None
    yaxis: Optional[str] = None
    dimensions: Optional[Union[str, List[str]]] = None
    aggregate_col: Optional[str] = None
    aggregate_func: Optional[str] = None
    aggregate_col_alias: Optional[str] = None
    dimension_col: Optional[str] = None
    offset: Optional[int] = 0
    limit: Optional[int] = 100


class ChartDataSchema(Schema):
    """Schema for chart data items"""

    x: Any
    y: Any
    name: str
    value: Any


class ChartMetadataSchema(Schema):
    """Schema for chart metadata"""

    chart_type: str
    computation_type: str
    schema_name: str
    table_name: str
    record_count: int
    generated_at: str
    cached: Optional[bool] = False
    cached_at: Optional[str] = None


class ChartGenerateDataSchema(Schema):
    """Schema for generated chart data"""

    chart_config: Dict[str, Any]
    raw_data: Dict[str, Any]
    metadata: ChartMetadataSchema


class ChartGenerateResponseSchema(Schema):
    """Schema for chart generation responses"""

    success: bool
    data: Optional[ChartGenerateDataSchema] = None
    message: str
    error: Optional[str] = None


class ColumnSchema(Schema):
    """Schema for table columns"""

    name: str
    data_type: str


class DatabaseObjectResponseSchema(Schema):
    """Schema for database objects (schemas, tables, columns)"""

    success: bool
    data: Union[List[str], List[ColumnSchema]]
    message: str
    error: Optional[str] = None
