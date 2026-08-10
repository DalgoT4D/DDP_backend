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
    deduplicate_metric_aliases,
    metric_display_name,
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

    # Metric SQL aliases (for reading result columns) and display headers — the alias
    # rule is shared with build_pivot_table_query so producer/consumer can't drift.
    # De-duplicate against dimension labels to stay in sync with the SQL query.
    pivot_dimension_names = list(payload.row_dimensions or []) + [
        f"pivot_col_{i}" for i in range(len(col_dims))
    ]
    metric_aliases = deduplicate_metric_aliases(
        payload.metrics or [], pivot_dimension_names
    )
    metric_display_names = [metric_display_name(m) for m in payload.metrics or []]

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
        show_row_grand_total=payload.show_row_grand_total,
        show_column_grand_total=payload.show_column_grand_total,
    )
