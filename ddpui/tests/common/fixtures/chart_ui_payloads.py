"""Real UI payloads webapp_v2 sends for chart create — one per chart_type.

Captured from live smoke-test charts (55–60). When the UI changes shape, update
these fixtures and the schema + API tests will catch any backend drift.

Each fixture is a complete `ChartCreate` payload (top-level fields + `extra_config`).
"""

BAR_UI_PAYLOAD = {
    "title": "Bar fixture",
    "chart_type": "bar",
    "schema_name": "staging",
    "table_name": "agg_pipeline_runs",
    "extra_config": {
        "dimension_column": "granularity",
        "extra_dimension_column": None,
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "orientation": "horizontal",
            "stacked": False,
            "showTooltip": True,
            "showLegend": True,
            "legendDisplay": None,
            "legendPosition": None,
            "showDataLabels": True,
            "dataLabelPosition": "top",
            "xAxisTitle": "",
            "yAxisTitle": "",
            "xAxisLabelRotation": "horizontal",
            "yAxisLabelRotation": "horizontal",
            "xAxisNumberFormat": None,
            "xAxisDecimalPlaces": None,
            "xAxisDateFormat": None,
            "yAxisNumberFormat": None,
            "yAxisDecimalPlaces": None,
        },
        "aggregate_function": "count",
    },
}

LINE_UI_PAYLOAD = {
    "title": "Line fixture",
    "chart_type": "line",
    "schema_name": "staging",
    "table_name": "agg_pipeline_runs",
    "extra_config": {
        "dimension_column": "granularity",
        "extra_dimension_column": None,
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "lineStyle": "straight",
            "showDataPoints": False,
            "showTooltip": True,
            "showLegend": True,
            "legendDisplay": "all",
            "legendPosition": None,
            "showDataLabels": True,
            "dataLabelPosition": "top",
            "xAxisTitle": "",
            "yAxisTitle": "",
            "xAxisLabelRotation": "horizontal",
            "yAxisLabelRotation": "horizontal",
            "xAxisNumberFormat": None,
            "xAxisDecimalPlaces": None,
            "xAxisDateFormat": None,
            "yAxisNumberFormat": None,
            "yAxisDecimalPlaces": None,
        },
        "aggregate_function": "count",
    },
}

PIE_UI_PAYLOAD = {
    "title": "Pie fixture",
    "chart_type": "pie",
    "schema_name": "staging",
    "table_name": "agg_pipeline_runs",
    "extra_config": {
        "dimension_column": "granularity",
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "chartStyle": "pie",
            "showLegend": True,
            "legendDisplay": None,
            "legendPosition": "right",
            "showTooltip": True,
            "showDataLabels": True,
            "labelFormat": "name_value",
            "dataLabelPosition": "outside",
            "maxSlices": None,
            "numberFormat": None,
            "decimalPlaces": None,
            "dateFormat": None,
        },
        "aggregate_function": "count",
    },
}

NUMBER_UI_PAYLOAD = {
    "title": "Number fixture",
    "chart_type": "number",
    "schema_name": "staging",
    "table_name": "agg_pipeline_runs",
    "extra_config": {
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "numberSize": "medium",
            "subtitle": "",
            "numberFormat": "adaptive_international",
            "decimalPlaces": 0,
            "numberPrefix": "",
            "numberSuffix": " USD",
        },
        "aggregate_function": "count",
    },
}

MAP_UI_PAYLOAD = {
    "title": "Map fixture",
    "chart_type": "map",
    "schema_name": "staging",
    "table_name": "state_csv2",
    "extra_config": {
        "geographic_column": "state",
        "value_column": None,
        "selected_geojson_id": 32,
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "colorScheme": "Greens",
            "showTooltip": True,
            "showLegend": True,
            "select": None,
            "nullValueLabel": "N/A",
            "legendPosition": "top-right",
            "showLabels": True,
            "title": "",
        },
        "aggregate_function": "count",
    },
}

TABLE_UI_PAYLOAD = {
    "title": "Table fixture",
    "chart_type": "table",
    "schema_name": "staging",
    "table_name": "agg_pipeline_runs",
    "extra_config": {
        "dimensions": [{"column": "granularity", "enable_drill_down": False}],
        "dimension_column": "granularity",
        "dimension_columns": ["granularity"],
        "table_columns": ["date_day", "granularity", "period_start", "period_end", "year", "month"],
        "metrics": [
            {"alias": "Total Count", "column": None, "aggregation": "count"},
        ],
        "filters": None,
        "sort": None,
        "pagination": None,
        "customizations": {
            "columnFormatting": {
                "Total Count": {"numberFormat": "indian", "decimalPlaces": None},
            },
            "dateColumnFormatting": None,
        },
        "aggregate_function": "count",
    },
}

CHART_UI_PAYLOADS = {
    "bar": BAR_UI_PAYLOAD,
    "line": LINE_UI_PAYLOAD,
    "pie": PIE_UI_PAYLOAD,
    "number": NUMBER_UI_PAYLOAD,
    "map": MAP_UI_PAYLOAD,
    "table": TABLE_UI_PAYLOAD,
}
