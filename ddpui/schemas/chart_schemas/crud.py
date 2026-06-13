"""Create/Read/Update schemas for charts."""

from datetime import datetime
from typing import Any, Literal, Optional

from ninja import Schema
from pydantic import model_validator

from ddpui.schemas.chart_schemas.config import _CHART_CONFIG_BY_TYPE


class ChartCreate(Schema):
    """Schema for creating a chart.

    `extra_config` is typed per chart_type via the after-validator below.
    Declared as `Any` (not Union) because Pydantic's smart Union resolution
    picks the loosest matching variant; we dispatch explicitly on `chart_type`.
    """

    title: str
    description: Optional[str] = None
    chart_type: Literal["bar", "line", "pie", "number", "map", "table"]
    schema_name: str
    table_name: str
    extra_config: Any

    @model_validator(mode="after")
    def coerce_extra_config_to_typed(self) -> "ChartCreate":
        if not isinstance(self.extra_config, dict):
            raise ValueError("extra_config must be an object")
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None:
            self.extra_config = sub_schema(**self.extra_config)
        return self


class ChartUpdate(Schema):
    """Schema for updating a chart. Partial — any subset is valid."""

    title: Optional[str] = None
    description: Optional[str] = None
    chart_type: Optional[Literal["bar", "line", "pie", "number", "map", "table"]] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    extra_config: Optional[Any] = None

    @model_validator(mode="after")
    def coerce_extra_config_to_typed(self) -> "ChartUpdate":
        if self.extra_config is not None and not isinstance(self.extra_config, dict):
            raise ValueError("extra_config must be an object")
        if self.chart_type is None or self.extra_config is None:
            return self
        sub_schema = _CHART_CONFIG_BY_TYPE.get(self.chart_type)
        if sub_schema is not None:
            self.extra_config = sub_schema(**self.extra_config)
        return self


class ChartResponse(Schema):
    """Schema for chart response."""

    id: int
    title: str
    description: Optional[str] = None
    chart_type: str
    schema_name: str
    table_name: str
    created_by: str
    extra_config: dict
    created_at: datetime
    updated_at: datetime


class ChartConfig(Schema):
    """Chart config used to build a ChartDataPayload.

    Works for Chart model instances and frozen report configs.
    """

    chart_type: str
    schema_name: str
    table_name: str
    title: Optional[str] = None
    extra_config: Optional[dict] = None
