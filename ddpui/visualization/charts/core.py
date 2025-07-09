from typing import Any
from sqlalchemy.sql.functions import func

from ddpui.visualization.charts.schema import (
    GenerateChartConfigRequest,
    SupportedChartingLibrary,
    SupportedChartType,
    SeriesData,
    AggregateFunction,
    ComputationType,
)
from ninja.errors import HttpError
from sqlalchemy.sql.expression import column

from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def generate_echarts_bar_chart_config(xaxis: SeriesData, yseries: list[SeriesData], title: str):
    """
    Generate Apache ECharts bar chart configuration.
    """
    # Color palette for different series
    color_palette = [
        "#5470c6",
        "#91cc75",
        "#fac858",
        "#ee6666",
        "#73c0de",
        "#3ba272",
        "#fc8452",
        "#9a60b4",
        "#ea7ccc",
        "#d4a76a",
    ]

    yaxis_series_list: list[dict] = []

    for i, series in enumerate(yseries):
        color = color_palette[i % len(color_palette)]  # Cycle through colors
        series_config = {
            "name": series.name.title(),
            "data": series.data,
            "type": "bar",
            "itemStyle": {"color": color},
            "emphasis": {"itemStyle": {"color": color, "shadowBlur": 10}},
        }
        yaxis_series_list.append(series_config)

    xaxis_series: dict = {
        "name": xaxis.name.title(),
        "type": "category",
        "data": xaxis.data,
        "nameLocation": "middle",
        "nameGap": 30,
    }

    config = {
        "title": {"text": title, "left": "center", "top": "10%"},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "xAxis": xaxis_series,
        "yAxis": {
            "type": "value",
            "nameLocation": "middle",
            "nameGap": 50,
            "nameRotate": 90,
        },
        "legend": {},
        "series": yaxis_series_list,
    }

    return config


def generate_echarts_line_chart_config(payload: GenerateChartConfigRequest):
    """
    Generate Apache ECharts line chart configuration.
    """
    # Hardcoded sample data - will be replaced with warehouse data later
    sample_categories = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    sample_values = [150, 230, 224, 218, 135, 147]

    x_axis_name = (
        payload.xaxis.split("_")[0].title()
        if payload.xaxis and "_" in payload.xaxis
        else payload.xaxis.title()
        if payload.xaxis
        else "X-Axis"
    )
    y_axis_name = (
        payload.yaxis.split("_")[0].title()
        if payload.yaxis and "_" in payload.yaxis
        else payload.yaxis.title()
        if payload.yaxis
        else "Y-Axis"
    )

    config = {
        "title": {
            "text": payload.title,
            "left": "center",
        },
        "tooltip": {"trigger": "axis"},
        "grid": {"left": "3%", "right": "4%", "bottom": "3%", "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": sample_categories,
            "name": x_axis_name,
            "nameLocation": "middle",
            "nameGap": 30,
        },
        "yAxis": {
            "type": "value",
            "name": y_axis_name,
            "nameLocation": "middle",
            "nameGap": 50,
            "nameRotate": 90,
        },
        "series": [
            {
                "name": y_axis_name,
                "type": "line",
                "data": sample_values,
                "smooth": True,
                "lineStyle": {"color": "#5470c6", "width": 2},
                "itemStyle": {"color": "#5470c6"},
            }
        ],
    }

    return config


def generate_echarts_pie_chart_config(payload: GenerateChartConfigRequest):
    """
    Generate Apache ECharts pie chart configuration.
    """
    # Hardcoded sample data - will be replaced with warehouse data later
    sample_data = [
        {"name": "Category A", "value": 2292707},
        {"name": "Category B", "value": 771258},
        {"name": "Category C", "value": 606387},
        {"name": "Category D", "value": 450123},
    ]

    config = {
        "title": {
            "text": payload.title,
            "left": "center",
        },
        "tooltip": {"trigger": "item", "formatter": "{a} <br/>{b}: {c} ({d}%)"},
        "legend": {"orient": "vertical", "left": "left"},
        "series": [
            {
                "name": payload.yaxis or "Data",
                "type": "pie",
                "radius": "50%",
                "data": sample_data,
                "emphasis": {
                    "itemStyle": {
                        "shadowBlur": 10,
                        "shadowOffsetX": 0,
                        "shadowColor": "rgba(0, 0, 0, 0.5)",
                    }
                },
            }
        ],
    }

    return config


def fetch_chart_data_from_warehouse(
    org_warehouse: OrgWarehouse,
    payload: GenerateChartConfigRequest,
) -> list[dict[str, Any]]:
    """
    Fetch chart data from warehouse using SQLAlchemy query builder.
    Returns data in the format required for chart configuration generation.
    """
    try:
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
        wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

        do_groupby = payload.computation_type == ComputationType.AGGREGATE.value

        # Use AggQueryBuilder to build the query
        builder = AggQueryBuilder()
        if payload.dimension_col:
            builder.add_column(column(payload.dimension_col))

        if payload.xaxis:
            builder.add_column(column(payload.xaxis))

        if payload.yaxis:
            builder.add_column(column(payload.yaxis))

        if payload.aggregate_func:
            # Map aggregate function enum to SQLAlchemy func
            aggregate_func_map = {
                AggregateFunction.COUNT.value: lambda col: func.count(),  # COUNT(*) doesn't need a column
                AggregateFunction.SUM.value: func.sum,
                AggregateFunction.AVG.value: func.avg,
                AggregateFunction.MIN.value: func.min,
                AggregateFunction.MAX.value: func.max,
                AggregateFunction.COUNT_DISTINCT.value: lambda col: func.count(func.distinct(col)),
            }

            if payload.aggregate_func in aggregate_func_map:
                agg_func = aggregate_func_map[payload.aggregate_func]
                if payload.aggregate_func == AggregateFunction.COUNT.value:
                    builder.add_column(agg_func(None).label(payload.aggregate_col_alias))
                elif payload.aggregate_col:
                    builder.add_column(
                        agg_func(column(payload.aggregate_col)).label(payload.aggregate_col_alias)
                    )
                else:
                    raise ValueError(
                        f"Aggregate function {payload.aggregate_func} requires aggregate_col to be specified"
                    )
            else:
                raise ValueError(f"Unsupported aggregate function: {payload.aggregate_func}")

        if do_groupby:
            groupby_cols = []
            if payload.dimension_col:
                groupby_cols.append(payload.dimension_col)

            if payload.xaxis:
                groupby_cols.append(payload.xaxis)

            if groupby_cols:  # Only group by if there are columns to group by
                builder.group_cols_by(*groupby_cols)

        builder.fetch_from(payload.table_name, payload.schema_name)
        builder.offset_rows(payload.offset)
        builder.limit_rows(payload.limit)
        stmt = builder.build()

        stmt = stmt.compile(bind=wclient.engine, compile_kwargs={"literal_binds": True})
        sql = str(stmt)

        logger.info(f"Generated SQL query: {sql}")

        rows = wclient.execute(sql)

        return rows

    except Exception as e:
        logger.error(f"Failed to fetch chart data from warehouse: {e}")
        return []


def transform_rows_to_echarts_bar_series_data(
    rows: list[dict[str, Any]], payload: GenerateChartConfigRequest
) -> tuple[SeriesData, list[SeriesData]]:
    """
    Transform warehouse query rows into SeriesData format required by ECharts bar chart config generator.
    Handles both raw and aggregated computation types.
    """
    if not rows:
        return SeriesData(name=payload.xaxis or "Empty", data=[]), [
            SeriesData(name=payload.yaxis or "Empty", data=[])
        ]

    if payload.computation_type == ComputationType.RAW.value:
        # For raw charts: direct x-axis and y-axis mapping
        xaxis_data = (
            [
                str(row[payload.xaxis]) if row[payload.xaxis] is not None else "Unknown"
                for row in rows
            ]
            if payload.xaxis
            else []
        )

        # Handle empty yaxis for partial configs
        if payload.yaxis:
            yaxis_data = [
                int(row[payload.yaxis]) if row[payload.yaxis] is not None else 0 for row in rows
            ]
            yaxis_series = [SeriesData(name=payload.yaxis, data=yaxis_data)]
        else:
            # Empty yaxis data for partial config
            yaxis_series = [SeriesData(name="Empty", data=[])]

        xaxis_series = SeriesData(name=payload.xaxis or "Empty", data=xaxis_data)

        return xaxis_series, yaxis_series

    else:
        if payload.xaxis:
            unique_xaxis_values = sorted(
                set(row[payload.xaxis] for row in rows), key=lambda x: (x is None, x)
            )
            xaxis_data_formatted = [
                str(x) if x is not None else "Null" for x in unique_xaxis_values
            ]
            xaxis_series = SeriesData(name=payload.xaxis, data=xaxis_data_formatted)
        else:
            xaxis_series = SeriesData(name="Empty", data=[])

        unique_dimensions = []
        if payload.dimension_col:
            unique_dimensions = sorted(
                set(row[payload.dimension_col] for row in rows), key=lambda x: (x is None, x)
            )

        yaxis_series = []

        for dimension_value in unique_dimensions:
            series_data = []

            for unique_xaxis_val in unique_xaxis_values:
                matching_row = None
                if payload.dimension_col and payload.xaxis:
                    # Both dimension_col and xaxis are available
                    matching_row = next(
                        (
                            row
                            for row in rows
                            if row[payload.dimension_col] == dimension_value
                            and row[payload.xaxis] == unique_xaxis_val
                        ),
                        None,
                    )
                elif payload.dimension_col:
                    # Only dimension_col is available
                    matching_row = next(
                        (row for row in rows if row[payload.dimension_col] == dimension_value),
                        None,
                    )
                elif payload.xaxis:
                    # Only xaxis is available
                    matching_row = next(
                        (row for row in rows if row[payload.xaxis] == unique_xaxis_val),
                        None,
                    )

                if (
                    matching_row
                    and payload.aggregate_col_alias
                    and matching_row[payload.aggregate_col_alias] is not None
                ):
                    series_data.append(int(matching_row[payload.aggregate_col_alias]))
                else:
                    series_data.append(0)

            yaxis_series.append(
                SeriesData(
                    name=str(dimension_value) if dimension_value is not None else "Null",
                    data=series_data,
                )
            )

        if not yaxis_series:
            # pick the payload.aggregate_col_alias if available
            # value should be picked from rows
            if payload.aggregate_col_alias:
                yaxis_series.append(
                    SeriesData(
                        name=payload.aggregate_col_alias,
                        data=[
                            (
                                int(row[payload.aggregate_col_alias])
                                if row[payload.aggregate_col_alias] is not None
                                else 0
                            )
                            for row in rows
                        ],
                    )
                )

        return xaxis_series, yaxis_series


def generate_chart_config(org_warehouse: OrgWarehouse, payload: GenerateChartConfigRequest):
    """
    Generates the chart configuration based on the provided request payload.
    This function is used to prepare the configuration for rendering charts
    in the frontend.
    """
    supported_libraries = [library.value for library in SupportedChartingLibrary]
    if not payload.charting_library or payload.charting_library.lower() not in supported_libraries:
        raise ValueError(
            f"Unsupported charting library: {payload.charting_library}. Supported libraries: {supported_libraries}"
        )

    supported_chart_types = [chart_type.value for chart_type in SupportedChartType]
    if payload.chart_type not in supported_chart_types:
        raise ValueError(
            f"Unsupported chart type: {payload.chart_type} - Supported types: {supported_chart_types}"
        )

    if payload.chart_type == SupportedChartType.BAR.value:
        # generate the data by running query against the warehouse
        rows = fetch_chart_data_from_warehouse(org_warehouse, payload)

        # # transform that data into the format required to generate chart config
        xaxis, yaxis = transform_rows_to_echarts_bar_series_data(rows, payload)

        # generate the chart config
        config = generate_echarts_bar_chart_config(
            xaxis,
            yaxis,
            payload.title,
        )

    return config
