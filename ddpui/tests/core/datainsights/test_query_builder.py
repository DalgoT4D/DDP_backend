import os
import re
from datetime import datetime
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from unittest.mock import patch


from ddpui.core.datainsights.query_builder import AggQueryBuilder
from sqlalchemy.sql.expression import (
    table,
    TableClause,
    select,
    Select,
    ColumnClause,
    column,
    asc,
    desc,
)
from sqlalchemy import create_engine


def test_add_column():
    query_builder = AggQueryBuilder()
    col = ColumnClause("column1")
    assert isinstance(query_builder.add_column(col), AggQueryBuilder)
    assert query_builder.column_clauses == [col]


def test_fetch_from():
    query_builder = AggQueryBuilder()
    db_table = "test_table"
    db_schema = "test_schema"
    assert isinstance(query_builder.fetch_from(db_table, db_schema), AggQueryBuilder)
    assert query_builder.select_from.name == db_table
    assert query_builder.select_from.schema == db_schema


def test_fetch_from_subquery():
    query_builder = AggQueryBuilder()
    subquery = select(column("column1")).select_from(table("test_table", schema="test_schema"))
    assert isinstance(query_builder.fetch_from_subquery(subquery), AggQueryBuilder)
    assert query_builder.select_from == subquery


def test_having_clause():
    query_builder = AggQueryBuilder()
    condition = column("column1") > 0
    assert isinstance(query_builder.having_clause(condition), AggQueryBuilder)
    assert query_builder.having_clauses == [condition]


def test_order_by_columns():
    query_builder = AggQueryBuilder()
    cols = [("column1", "asc"), ("column2", "desc")]
    assert isinstance(query_builder.order_cols_by(cols), AggQueryBuilder)
    assert str(query_builder.order_by_clauses[0]) == "column1 ASC"
    assert str(query_builder.order_by_clauses[1]) == "column2 DESC"


def test_where_clause():
    query_builder = AggQueryBuilder()
    condition = column("column1") > 0
    assert isinstance(query_builder.where_clause(condition), AggQueryBuilder)
    assert query_builder.where_clauses == [condition]


def test_limit_rows():
    query_builder = AggQueryBuilder()
    limit = 10
    assert isinstance(query_builder.limit_rows(limit), AggQueryBuilder)
    assert query_builder.limit_records == limit


def test_offset_rows():
    query_builder = AggQueryBuilder()
    offset = 5
    assert isinstance(query_builder.offset_rows(offset), AggQueryBuilder)
    assert query_builder.offset_records == offset


def test_build():
    with patch.object(AggQueryBuilder, "subquery") as MockSubqueryMethod:
        query_builder = AggQueryBuilder()
        query_builder.build()
        MockSubqueryMethod.assert_called_once()


def test_reset():
    query_builder = AggQueryBuilder()
    query_builder.column_clauses = [column("column1")]
    query_builder.select_from = table("test_table", schema="test_schema")
    query_builder.group_by_clauses = [column("column1")]
    query_builder.order_by_clauses = [asc(column("column1"))]
    query_builder.limit_records = 10
    query_builder.offset_records = 5
    query_builder.where_clauses = [column("column1") > 0]
    query_builder.having_clauses = [column("column1") > 0]

    query_builder.reset()

    assert query_builder.column_clauses == []
    assert query_builder.select_from is None
    assert query_builder.group_by_clauses == []
    assert query_builder.order_by_clauses == []
    assert query_builder.limit_records is None
    assert query_builder.offset_records == 0
    assert query_builder.where_clauses == []
    assert query_builder.having_clauses == []


def test_subquery():
    query_builder = AggQueryBuilder()
    query_builder.column_clauses = [column("column1")]
    query_builder.select_from = table("test_table", schema="test_schema")
    query_builder.group_by_clauses = [column("column1")]
    query_builder.order_by_clauses = [asc(column("column1"))]
    query_builder.limit_records = 10
    query_builder.offset_records = 5
    query_builder.where_clauses = [column("column1") > 0]
    query_builder.having_clauses = [column("column1") > 0]

    subquery = query_builder.subquery().compile(compile_kwargs={"literal_binds": True})

    assert re.sub(r"\s+", "", str(subquery)) == re.sub(
        r"\s+",
        "",
        """
            SELECT column1 
            FROM test_schema.test_table 
            WHERE column1 > 0 GROUP BY column1 
            HAVING column1 > 0 ORDER BY column1 ASC
            LIMIT 10 OFFSET 5
        """,
    )


def _normalize(sql: str) -> str:
    """Collapse whitespace for comparison."""
    return re.sub(r"\s+", " ", sql).strip()


def test_add_aggregate_column_boolean_sum_casts_to_integer():
    """SUM on a boolean column must wrap in CAST(... AS INTEGER)."""
    qb = AggQueryBuilder()
    qb.fetch_from("users", "public")
    qb.add_aggregate_column("is_active", "sum", alias="active_sum", column_type="boolean")

    sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
    assert "CAST(is_active AS INTEGER)" in sql
    assert "sum(cast(is_active as integer))" in _normalize(sql).lower()


def test_add_aggregate_column_boolean_avg_casts_to_integer():
    """AVG on a boolean column must wrap in CAST(... AS INTEGER)."""
    qb = AggQueryBuilder()
    qb.fetch_from("users", "public")
    qb.add_aggregate_column("is_active", "avg", alias="active_avg", column_type="bool")

    sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
    assert "CAST(is_active AS INTEGER)" in sql


def test_add_aggregate_column_boolean_count_no_cast():
    """COUNT on a boolean column should NOT cast — COUNT(bool) works fine."""
    qb = AggQueryBuilder()
    qb.fetch_from("users", "public")
    qb.add_aggregate_column("is_active", "count", alias="cnt", column_type="boolean")

    sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
    assert "CAST" not in sql


def test_add_aggregate_column_numeric_sum_no_cast():
    """SUM on a numeric column should NOT cast."""
    qb = AggQueryBuilder()
    qb.fetch_from("orders", "public")
    qb.add_aggregate_column("amount", "sum", alias="total", column_type="numeric")

    sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
    assert "CAST" not in sql


def test_add_aggregate_column_no_column_type_no_cast():
    """When column_type is not provided (backward compat), no cast is applied."""
    qb = AggQueryBuilder()
    qb.fetch_from("orders", "public")
    qb.add_aggregate_column("amount", "sum", alias="total")

    sql = str(qb.build().compile(compile_kwargs={"literal_binds": True}))
    assert "CAST" not in sql
