"""Build alert SQL using the existing AggQueryBuilder (warehouse-agnostic)"""

from datetime import datetime, timezone, timedelta

from sqlalchemy import column, func, and_, or_, cast, Date

from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.models.alert import (
    AlertQueryConfig,
    AlertFilterConfig,
)


CONDITION_OPERATORS = {
    ">": lambda col, val: col > val,
    "<": lambda col, val: col < val,
    ">=": lambda col, val: col >= val,
    "<=": lambda col, val: col <= val,
    "=": lambda col, val: col == val,
    "!=": lambda col, val: col != val,
}


def build_alert_query_builder(
    config: AlertQueryConfig,
) -> AggQueryBuilder:
    """
    Build an AggQueryBuilder from typed AlertQueryConfig.

    Returns the outer query builder (not compiled). Callers can:
    - Compile it for execution
    - Add limit/offset for pagination
    - Wrap it in another subquery for COUNT(*)

    Query pattern:
      SELECT [group_col,] alert_value
      FROM (
        SELECT [group_col,] AGG(measure_col) AS alert_value
        FROM schema.table
        WHERE <filters>
        GROUP BY group_col
      ) AS cte
      WHERE alert_value <op> <value>
    """
    # --- Inner query: aggregation + filters ---
    inner_qb = AggQueryBuilder()
    inner_qb.fetch_from(config.table_name, config.schema_name)

    if config.group_by_column:
        inner_qb.add_column(column(config.group_by_column))

    inner_qb.add_aggregate_column(
        config.measure_column,
        config.aggregation.lower(),
        "alert_value",
    )

    if config.filters:
        filter_clauses = [_build_filter_clause(f) for f in config.filters]
        if config.filter_connector == "AND":
            inner_qb.where_clause(and_(*filter_clauses))
        else:
            inner_qb.where_clause(or_(*filter_clauses))

    if config.group_by_column:
        inner_qb.group_cols_by(config.group_by_column)

    inner_subquery = inner_qb.subquery(alias="cte")

    # --- Outer query: threshold condition ---
    outer_qb = AggQueryBuilder()
    outer_qb.fetch_from_subquery(inner_subquery)

    if config.group_by_column:
        outer_qb.add_column(column(config.group_by_column))

    outer_qb.add_column(column("alert_value"))

    op_func = CONDITION_OPERATORS.get(config.condition_operator)
    if not op_func:
        raise ValueError(f"Unsupported condition operator: {config.condition_operator}")

    outer_qb.where_clause(op_func(column("alert_value"), config.condition_value))

    return outer_qb


def compile_query(query_builder: AggQueryBuilder, warehouse_client) -> str:
    """Compile an AggQueryBuilder to a SQL string using the warehouse engine"""
    sql_stmt = query_builder.build()
    compiled = sql_stmt.compile(
        bind=warehouse_client.engine,
        compile_kwargs={"literal_binds": True},
    )
    return str(compiled)


def build_count_query_builder(base_qb: AggQueryBuilder) -> AggQueryBuilder:
    """Wrap a query builder in a COUNT(*) query for getting total rows"""
    count_qb = AggQueryBuilder()
    count_qb.fetch_from_subquery(base_qb.subquery(alias="alert_results"))
    count_qb.add_column(func.count().label("cnt"))
    return count_qb


def build_paginated_query_builder(
    base_qb: AggQueryBuilder, page: int, page_size: int
) -> AggQueryBuilder:
    """Add LIMIT/OFFSET to a query builder for pagination"""
    offset = (page - 1) * page_size
    base_qb.limit_rows(page_size)
    base_qb.offset_rows(offset)
    return base_qb


RELATIVE_DATE_TOKENS = {"__today__", "__yesterday__"}


def _resolve_filter_value(val: str):
    """Resolve filter value — handles relative date tokens.
    Resolves to literal date strings (UTC) for cross-warehouse compatibility.
    """
    if val == "__today__":
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if val == "__yesterday__":
        return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    return val


def _is_date_filter(val: str) -> bool:
    """Check if the value is a date token or a date string (YYYY-MM-DD)"""
    if val in RELATIVE_DATE_TOKENS:
        return True
    # Match YYYY-MM-DD pattern
    if len(val) == 10 and val[4] == "-" and val[7] == "-":
        return True
    return False


def _build_filter_clause(filter_config: AlertFilterConfig):
    """Build a single SQLAlchemy filter expression from typed AlertFilterConfig"""
    col = column(filter_config.column)
    op = filter_config.operator
    val = _resolve_filter_value(filter_config.value)

    # Cast column to DATE when comparing with date values
    if _is_date_filter(filter_config.value):
        col = cast(col, Date)

    if op == "=":
        return col == val
    elif op == "!=":
        return col != val
    elif op == ">":
        return col > val
    elif op == "<":
        return col < val
    elif op == ">=":
        return col >= val
    elif op == "<=":
        return col <= val
    elif op == "contains":
        return col.contains(val)
    elif op == "not contains":
        return ~col.contains(val)
    elif op == "is true":
        return col == True  # noqa: E712
    elif op == "is false":
        return col == False  # noqa: E712
    else:
        raise ValueError(f"Unsupported filter operator: {op}")
