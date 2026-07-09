"""Typed `extra_config.customizations` payloads per chart_type."""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


NumberFormat = Literal[
    "default",
    "percentage",
    "indian",
    "international",
    "european",
    "adaptive_international",
    "adaptive_indian",
]

DateFormat = Literal[
    "default",
    "iso_datetime",
    "dd_mm_yyyy",
    "mm_dd_yyyy",
    "yyyy_mm_dd",
    "dd_mm_yyyy_time",
    "time_only",
]

LegendDisplay = Literal["paginated", "all"]
CardinalLegendPosition = Literal["top", "bottom", "left", "right"]
MapLegendPosition = Literal["top-left", "top-right", "bottom-left", "bottom-right"]
AxisLabelRotation = Literal["horizontal", "45", "vertical"]
MapColorScheme = Literal["Blues", "Reds", "Greens", "Purples", "Oranges", "Greys"]


class ColumnNumberFormat(BaseModel):
    """Per-column numeric formatting on `TableChartCustomizations.columnFormatting`."""

    model_config = ConfigDict(extra="allow")

    numberFormat: Optional[NumberFormat] = None
    decimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)


class ColumnDateFormat(BaseModel):
    """Per-column date formatting on `TableChartCustomizations.dateColumnFormatting`."""

    model_config = ConfigDict(extra="allow")

    dateFormat: Optional[DateFormat] = None


class BarChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='bar'."""

    model_config = ConfigDict(extra="allow")

    orientation: Optional[Literal["vertical", "horizontal"]] = None
    stacked: Optional[bool] = None
    showTooltip: Optional[bool] = None
    showLegend: Optional[bool] = None
    legendDisplay: Optional[LegendDisplay] = None
    legendPosition: Optional[CardinalLegendPosition] = None
    showDataLabels: Optional[bool] = None
    dataLabelPosition: Optional[Literal["top", "inside", "insideBottom"]] = None
    xAxisTitle: Optional[str] = None
    yAxisTitle: Optional[str] = None
    xAxisLabelRotation: Optional[AxisLabelRotation] = None
    yAxisLabelRotation: Optional[AxisLabelRotation] = None
    xAxisNumberFormat: Optional[NumberFormat] = None
    xAxisDecimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)
    xAxisDateFormat: Optional[DateFormat] = None
    yAxisNumberFormat: Optional[NumberFormat] = None
    yAxisDecimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)


class LineChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='line'."""

    model_config = ConfigDict(extra="allow")

    lineStyle: Optional[Literal["smooth", "straight"]] = None
    showDataPoints: Optional[bool] = None
    showTooltip: Optional[bool] = None
    showLegend: Optional[bool] = None
    legendDisplay: Optional[LegendDisplay] = None
    legendPosition: Optional[CardinalLegendPosition] = None
    showDataLabels: Optional[bool] = None
    dataLabelPosition: Optional[CardinalLegendPosition] = None
    xAxisTitle: Optional[str] = None
    yAxisTitle: Optional[str] = None
    xAxisLabelRotation: Optional[AxisLabelRotation] = None
    yAxisLabelRotation: Optional[AxisLabelRotation] = None
    xAxisNumberFormat: Optional[NumberFormat] = None
    xAxisDecimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)
    xAxisDateFormat: Optional[DateFormat] = None
    yAxisNumberFormat: Optional[NumberFormat] = None
    yAxisDecimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)


class PieChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='pie'."""

    model_config = ConfigDict(extra="allow")

    chartStyle: Optional[Literal["donut", "pie"]] = None
    showLegend: Optional[bool] = None
    legendDisplay: Optional[LegendDisplay] = None
    legendPosition: Optional[CardinalLegendPosition] = None
    showTooltip: Optional[bool] = None
    showDataLabels: Optional[bool] = None
    labelFormat: Optional[Literal["percentage", "value", "name_percentage", "name_value"]] = None
    dataLabelPosition: Optional[Literal["outside", "inside"]] = None
    # null = "show all"; otherwise typically 3/5/10
    maxSlices: Optional[int] = None
    numberFormat: Optional[NumberFormat] = None
    decimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)
    dateFormat: Optional[DateFormat] = None


class NumberChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='number'."""

    model_config = ConfigDict(extra="allow")

    numberSize: Optional[Literal["small", "medium", "large"]] = None
    subtitle: Optional[str] = None
    numberFormat: Optional[NumberFormat] = None
    decimalPlaces: Optional[int] = Field(default=None, ge=0, le=10)
    numberPrefix: Optional[str] = None
    numberSuffix: Optional[str] = None


class MapChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='map'."""

    model_config = ConfigDict(extra="allow")

    colorScheme: Optional[MapColorScheme] = None
    showTooltip: Optional[bool] = None
    showLegend: Optional[bool] = None
    select: Optional[bool] = None
    nullValueLabel: Optional[str] = None
    legendPosition: Optional[MapLegendPosition] = None
    showLabels: Optional[bool] = None
    title: Optional[str] = None


class TableChartCustomizations(BaseModel):
    """`extra_config.customizations` for chart_type='table'.

    Sparse: only columns the user has configured appear. UI cleans up entries
    when columns are removed from the query.
    """

    model_config = ConfigDict(extra="allow")

    columnFormatting: Optional[dict[str, ColumnNumberFormat]] = None
    dateColumnFormatting: Optional[dict[str, ColumnDateFormat]] = None
