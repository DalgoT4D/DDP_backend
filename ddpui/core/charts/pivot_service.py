"""
Pivot table service: orchestrates cardinality checks, group-level pagination,
and the full pivot data pipeline.

Supports multiple column dimensions via column_dimensions (list) and
column_time_grains (dict mapping column name → grain).
"""

from sqlalchemy import func, select, column
from sqlalchemy.sql.expression import table as sa_table

from ddpui.schemas.chart_schema import ChartDataPayload
from ddpui.models.org import OrgWarehouse
from ddpui.core.charts.pivot_transform import rotate_to_pivot, MAX_PIVOT_COLUMNS
from ddpui.core.charts.charts_service import (
    build_chart_query,
    apply_time_grain,
    get_warehouse_client,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.charts.pivot_service")

PIVOT_DEFAULT_PAGE_SIZE = 50


def check_pivot_cardinality(warehouse_client, payload: ChartDataPayload) -> None:
    """
    For each column dimension, check COUNT(DISTINCT col) and raise ValueError
    if ANY single dimension exceeds MAX_PIVOT_COLUMNS.
    """
    col_dims = payload.column_dimensions or []
    if not col_dims:
        return

    time_grains = payload.column_time_grains or {}
    source = sa_table(payload.table_name, schema=payload.schema_name)

    for col_dim in col_dims:
        col_expr = column(col_dim)
        grain = time_grains.get(col_dim)
        if grain:
            col_expr = apply_time_grain(col_expr, grain, "postgres")

        stmt = select(func.count(func.distinct(col_expr)).label("cnt")).select_from(source)
        results = warehouse_client.execute(stmt)
        cnt = results[0]["cnt"] if results else 0

        if cnt > MAX_PIVOT_COLUMNS:
            raise ValueError(
                f"Column dimension '{col_dim}' has too many unique values "
                f"({cnt} > {MAX_PIVOT_COLUMNS}). Choose a lower-cardinality column."
            )


def count_total_top_level_groups(warehouse_client, payload: ChartDataPayload) -> int:
    """Return COUNT(DISTINCT first_row_dimension) from the source table."""
    if not payload.row_dimensions:
        return 0

    first_dim = payload.row_dimensions[0]
    source = sa_table(payload.table_name, schema=payload.schema_name)
    stmt = select(func.count(func.distinct(column(first_dim))).label("cnt")).select_from(source)

    if payload.dashboard_filters:
        for f in payload.dashboard_filters:
            col_name = f.get("column")
            value = f.get("value")
            if col_name and value is not None:
                if isinstance(value, list):
                    stmt = stmt.where(column(col_name).in_(value))
                else:
                    stmt = stmt.where(column(col_name) == value)

    results = warehouse_client.execute(stmt)
    return results[0]["cnt"] if results else 0


def build_page_groups_subquery(
    payload: ChartDataPayload,
    page: int,
    page_size: int,
):
    """Build a subquery that returns the distinct first-row-dimension values for the requested page.

    Used as: WHERE first_dim IN (this_subquery) — single round-trip instead of two.
    """
    first_dim = payload.row_dimensions[0]
    source = sa_table(payload.table_name, schema=payload.schema_name)
    offset = (page - 1) * page_size

    subq = (
        select(column(first_dim))
        .select_from(source)
        .distinct()
        .order_by(column(first_dim))
        .limit(page_size)
        .offset(offset)
    )

    if payload.dashboard_filters:
        for f in payload.dashboard_filters:
            col_name = f.get("column")
            value = f.get("value")
            if col_name and value is not None:
                if isinstance(value, list):
                    subq = subq.where(column(col_name).in_(value))
                else:
                    subq = subq.where(column(col_name) == value)

    return subq


def get_pivot_table_data(
    org_warehouse: OrgWarehouse,
    payload: ChartDataPayload,
    page: int = 1,
    page_size: int = PIVOT_DEFAULT_PAGE_SIZE,
) -> dict:
    """
    Full pivot table pipeline:
    1. Cardinality check per column dimension
    2. Group-level pagination
    3. Build & execute ROLLUP query
    4. Rotate flat rows into pivoted JSON with composite column keys
    """
    warehouse_client = get_warehouse_client(org_warehouse)
    col_dims = payload.column_dimensions or []
    time_grains = payload.column_time_grains or {}

    # Cardinality guard
    check_pivot_cardinality(warehouse_client, payload)

    # Total groups count (needed for pagination metadata)
    total_groups = count_total_top_level_groups(warehouse_client, payload)

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

    # Empty dataset
    if total_groups == 0:
        return {
            "column_keys": [],
            "column_dimension_names": col_dims,
            "metric_headers": metric_display_names,
            "rows": [],
            "grand_total": None,
            "total_row_groups": 0,
            "page": page,
            "page_size": page_size,
        }

    # Build ROLLUP query with subquery-based pagination
    # WHERE first_dim IN (SELECT DISTINCT first_dim ... LIMIT page_size OFFSET offset)
    first_dim = payload.row_dimensions[0]
    query_builder = build_chart_query(payload, org_warehouse)

    page_subquery = build_page_groups_subquery(payload, page, page_size)
    query_builder.where_clause(column(first_dim).in_(page_subquery))

    # Execute
    sql_stmt = query_builder.build()
    compiled_stmt = sql_stmt.compile(
        bind=warehouse_client.engine, compile_kwargs={"literal_binds": True}
    )
    logger.debug(f"Executing pivot SQL: {compiled_stmt}")
    flat_rows = list(warehouse_client.execute(compiled_stmt))

    # Rotate
    result = rotate_to_pivot(
        flat_rows=flat_rows,
        row_dim_cols=payload.row_dimensions or [],
        num_col_dims=len(col_dims),
        col_dim_names=col_dims,
        metric_aliases=metric_aliases,
        time_grains=time_grains,
        page=page,
        page_size=page_size,
        metric_display_names=metric_display_names,
    )

    result["total_row_groups"] = total_groups
    return result
