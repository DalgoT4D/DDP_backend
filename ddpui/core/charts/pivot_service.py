"""
Pivot table service: orchestrates the full pivot data pipeline.

Supports multiple column dimensions via column_dimensions (list).
"""

from ddpui.schemas.chart_schemas import ChartDataPayload
from ddpui.models.org import OrgWarehouse
from ddpui.core.charts.pivot_transform import rotate_to_pivot
from ddpui.core.charts.charts_service import (
    build_chart_query,
    get_warehouse_client,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.charts.pivot_service")


def get_pivot_table_data(
    org_warehouse: OrgWarehouse,
    payload: ChartDataPayload,
) -> dict:
    """
    Full pivot table pipeline:
    1. Build & execute ROLLUP query
    2. Rotate flat rows into pivoted JSON with composite column keys
    """
    warehouse_client = get_warehouse_client(org_warehouse)
    col_dims = payload.column_dimensions or []

    # Compute metric aliases (same logic as build_pivot_table_query)
    # metric_aliases are technical SQL column aliases used for query execution
    # metric_display_names are user-facing display headers
    metric_aliases = []
    metric_display_names = []
    if payload.metrics:
        for metric in payload.metrics:
            if metric.aggregation.lower() == "count" and metric.column is None:
                sql_alias = f"count_all_{metric.alias}" if metric.alias else "count_all"
                display_name = metric.alias or "count_all"
            else:
                sql_alias = metric.alias or f"{metric.aggregation}_{metric.column}"
                display_name = metric.alias or f"{metric.aggregation}_{metric.column}"
            metric_aliases.append(sql_alias)
            metric_display_names.append(display_name)

    # Build & execute ROLLUP query over all rows (rotate_to_pivot handles empty results)
    query_builder = build_chart_query(payload, org_warehouse)
    sql_stmt = query_builder.build()
    compiled_stmt = sql_stmt.compile(
        bind=warehouse_client.engine, compile_kwargs={"literal_binds": True}
    )
    logger.debug(f"Executing pivot SQL: {compiled_stmt}")
    flat_rows = list(warehouse_client.execute(compiled_stmt))

    # Rotate
    return rotate_to_pivot(
        flat_rows=flat_rows,
        row_dim_cols=payload.row_dimensions or [],
        num_col_dims=len(col_dims),
        col_dim_names=col_dims,
        metric_aliases=metric_aliases,
        metric_display_names=metric_display_names,
        show_column_subtotals=payload.show_column_subtotals,
        show_row_subtotals=payload.show_row_subtotals,
    )
