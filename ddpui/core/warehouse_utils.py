"""Warehouse utility functions

Helper functions for warehouse schema introspection and column type detection.
Shared across filter, report, and other modules.
"""

from sqlalchemy import column

from ddpui.core.charts.charts_service import execute_query
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.models.dashboard import DashboardFilterType


def get_table_columns(warehouse_client, org_warehouse, schema_name, table_name):
    """Query warehouse information_schema for columns in a table.

    Returns list of dicts with keys: column_name, data_type, is_nullable.
    Supports Postgres and BigQuery warehouses.
    Returns empty list for unsupported warehouse types.
    """
    query_builder = AggQueryBuilder()

    if org_warehouse.wtype == "postgres":
        query_builder.add_column(column("column_name"))
        query_builder.add_column(column("data_type"))
        query_builder.add_column(column("is_nullable"))
        query_builder.fetch_from("columns", "information_schema")
        query_builder.where_clause(column("table_schema") == schema_name)
        query_builder.where_clause(column("table_name") == table_name)
        query_builder.order_cols_by([("ordinal_position", "asc")])
    elif org_warehouse.wtype == "bigquery":
        query_builder.add_column(column("column_name"))
        query_builder.add_column(column("data_type"))
        query_builder.add_column(column("is_nullable"))
        query_builder.fetch_from(
            "COLUMNS",
            f"{org_warehouse.bq_location}.{schema_name}.INFORMATION_SCHEMA",
        )
        query_builder.where_clause(column("table_name") == table_name)
        query_builder.order_cols_by([("ordinal_position", "asc")])
    else:
        return []

    return execute_query(warehouse_client, query_builder)


def determine_filter_type_from_column(data_type: str) -> str:
    """Simple filter type determination based on column data type"""
    data_type_lower = data_type.lower()

    # DateTime patterns
    datetime_patterns = ["timestamp", "datetime", "date", "timestamptz", "time"]
    if any(pattern in data_type_lower for pattern in datetime_patterns):
        return DashboardFilterType.DATETIME.value

    # Numerical patterns
    numerical_patterns = [
        "integer",
        "bigint",
        "numeric",
        "decimal",
        "double",
        "real",
        "float",
        "money",
    ]
    if any(pattern in data_type_lower for pattern in numerical_patterns):
        return DashboardFilterType.NUMERICAL.value

    # Default to value filter for text/categorical
    return DashboardFilterType.VALUE.value
