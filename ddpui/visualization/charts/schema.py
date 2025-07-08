from typing import Dict, Any, List, Optional
from enum import Enum
from ninja import Schema


################## Enums ##################


class AggregateFunction(Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"


class SupportedChartingLibrary(Enum):
    APACHE_ECHARTS = "apache_echarts"


class SupportedChartType(Enum):
    BAR = "bar"


###### api request and response schemas ######


class RawChartQueryRequest(Schema):
    chart_type: str
    schema_name: str
    table_name: str
    xaxis_col: str
    yaxis_col: str
    offset: int
    limit: int = 10


class ChartCreateRequest(Schema):
    title: str
    description: Optional[str] = None
    chart_type: str
    schema_name: str
    table: str
    config: Dict[str, Any]


class ChartUpdateRequest(Schema):
    title: Optional[str] = None
    description: Optional[str] = None
    chart_type: Optional[str] = None
    schema_name: Optional[str] = None
    table: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class ChartResponse(Schema):
    id: int
    title: str
    description: Optional[str]
    chart_type: str
    schema_name: str
    table: str
    config: Dict[str, Any]
    created_at: str
    updated_at: str


class ChartQueryResponse(Schema):
    data: List[Dict[str, Any]]


class ChartDataRequest(Schema):
    xaxis_col: str
    yaxis_col: str
    offset: int = 0
    limit: int = 10


class GenerateChartConfigRequest(Schema):
    charting_library: str
    chart_type: str
    schema_name: str
    table_name: str
    title: str
    description: Optional[str] = None
    xaxis: str
    yaxis: str
    offset: int = 0
    limit: int = 10
    computation_type: str
    dimension_col: str
    aggregate_func: str
    aggregate_col: str


##########################################


class SeriesData(Schema):
    name: str
    data: list[Any]
