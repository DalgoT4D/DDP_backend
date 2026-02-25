"""Filter API endpoints for dashboard filters"""

from typing import List, Optional, Dict, Any
from django.shortcuts import get_object_or_404
from ninja import Router, Schema
from ninja.errors import HttpError
from sqlalchemy import func, column, distinct, cast, Float, Date
from sqlalchemy.sql.expression import table

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.models.dashboard import DashboardFilterType
from ddpui.core.charts.charts_service import execute_query, get_warehouse_client
from ddpui.core.datainsights.query_builder import AggQueryBuilder
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

filter_router = Router()


class SchemaResponse(Schema):
    name: str
    type: str = "schema"


class TableResponse(Schema):
    name: str
    type: str = "table"


class ColumnResponse(Schema):
    name: str
    type: str
    data_type: str  # Original database data type
    recommended_filter_type: str  # Auto-determined filter type
    nullable: bool = True


class FilterOptionResponse(Schema):
    label: str
    value: str
    count: Optional[int] = None


class NumericalStatsResponse(Schema):
    min_value: float
    max_value: float
    avg_value: float
    distinct_count: int


class FilterPreviewResponse(Schema):
    options: Optional[List[FilterOptionResponse]] = None
    stats: Optional[Dict[str, Any]] = None  # Can be numerical or datetime stats


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


@filter_router.get("/schemas/", response=List[SchemaResponse])
@has_permission(["can_view_warehouse_data"])
def list_schemas(request):
    """List available schemas in the organization's warehouse"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = get_warehouse_client(org_warehouse)

        # Build query using AggQueryBuilder
        query_builder = AggQueryBuilder()

        if org_warehouse.wtype == "postgres":
            # Query information_schema using query builder
            query_builder.add_column(column("schema_name"))
            query_builder.fetch_from("schemata", "information_schema")
            query_builder.where_clause(
                ~column("schema_name").in_(["information_schema", "pg_catalog", "pg_toast"])
            )
            query_builder.order_cols_by([("schema_name", "asc")])

        elif org_warehouse.wtype == "bigquery":
            # For BigQuery, use INFORMATION_SCHEMA
            query_builder.add_column(column("schema_name"))
            query_builder.fetch_from("SCHEMATA", f"{org_warehouse.bq_location}.INFORMATION_SCHEMA")
            query_builder.order_cols_by([("schema_name", "asc")])
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        # Execute query using charts_service function
        results = execute_query(warehouse_client, query_builder)

        schemas = [SchemaResponse(name=row["schema_name"]) for row in results]

        logger.info(f"Found {len(schemas)} schemas for org {orguser.org.id}")
        return schemas

    except Exception as e:
        logger.error(f"Error fetching schemas: {str(e)}")
        raise HttpError(500, "Error fetching schemas")


@filter_router.get("/schemas/{schema_name}/tables/", response=List[TableResponse])
@has_permission(["can_view_warehouse_data"])
def list_tables(request, schema_name: str):
    """List tables in a specific schema"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = get_warehouse_client(org_warehouse)

        # Build query using AggQueryBuilder
        query_builder = AggQueryBuilder()

        if org_warehouse.wtype == "postgres":
            query_builder.add_column(column("table_name"))
            query_builder.fetch_from("tables", "information_schema")
            query_builder.where_clause(column("table_schema") == schema_name)
            query_builder.where_clause(column("table_type") == "BASE TABLE")
            query_builder.order_cols_by([("table_name", "asc")])

        elif org_warehouse.wtype == "bigquery":
            query_builder.add_column(column("table_name"))
            query_builder.fetch_from(
                "TABLES", f"{org_warehouse.bq_location}.{schema_name}.INFORMATION_SCHEMA"
            )
            query_builder.where_clause(column("table_type") == "BASE TABLE")
            query_builder.order_cols_by([("table_name", "asc")])
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        # Execute query using charts_service function
        results = execute_query(warehouse_client, query_builder)

        tables = [TableResponse(name=row["table_name"]) for row in results]

        logger.info(f"Found {len(tables)} tables in schema {schema_name}")
        return tables

    except Exception as e:
        logger.error(f"Error fetching tables for schema {schema_name}: {str(e)}")
        raise HttpError(500, "Error fetching tables")


@filter_router.get(
    "/schemas/{schema_name}/tables/{table_name}/columns/", response=List[ColumnResponse]
)
@has_permission(["can_view_warehouse_data"])
def list_columns(request, schema_name: str, table_name: str):
    """List columns in a specific table with recommended filter types"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = get_warehouse_client(org_warehouse)

        # Build query using AggQueryBuilder
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
                "COLUMNS", f"{org_warehouse.bq_location}.{schema_name}.INFORMATION_SCHEMA"
            )
            query_builder.where_clause(column("table_name") == table_name)
            query_builder.order_cols_by([("ordinal_position", "asc")])
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        # Execute query using charts_service function
        results = execute_query(warehouse_client, query_builder)

        columns = []
        for row in results:
            # Normalize data types
            col_type = row["data_type"].lower()
            if any(t in col_type for t in ["int", "float", "decimal", "numeric", "double"]):
                normalized_type = "number"
            elif any(t in col_type for t in ["date", "time", "timestamp"]):
                normalized_type = "date"
            else:
                normalized_type = "string"

            # Determine recommended filter type
            recommended_filter_type = determine_filter_type_from_column(row["data_type"])

            columns.append(
                ColumnResponse(
                    name=row["column_name"],
                    type=normalized_type,
                    data_type=row["data_type"],  # Original data type
                    recommended_filter_type=recommended_filter_type,
                    nullable=row["is_nullable"] == "YES" or row["is_nullable"] is True,
                )
            )

        logger.info(f"Found {len(columns)} columns in table {schema_name}.{table_name}")
        return columns

    except Exception as e:
        logger.error(f"Error fetching columns for table {schema_name}.{table_name}: {str(e)}")
        raise HttpError(500, "Error fetching columns")


@filter_router.get("/preview/", response=FilterPreviewResponse)
@has_permission(["can_view_warehouse_data"])
def get_filter_preview(
    request,
    schema_name: str,
    table_name: str,
    column_name: str,
    filter_type: str,  # 'value', 'numerical', or 'datetime'
    limit: int = 100,
):
    """Get preview data for a filter (values, numerical stats, or date range)"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = get_warehouse_client(org_warehouse)

        if filter_type == "value":
            # Get distinct values with counts for categorical filter
            query_builder = AggQueryBuilder()
            query_builder.add_column(column(column_name).label("value"))
            query_builder.add_aggregate_column(None, "count", alias="count")
            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))
            query_builder.group_cols_by(column_name)
            query_builder.order_cols_by([("count", "desc"), ("value", "asc")])
            query_builder.limit_rows(limit)

            # Execute query using charts_service function
            results = execute_query(warehouse_client, query_builder)

            options = [
                FilterOptionResponse(
                    label=str(row["value"]) if row["value"] is not None else "NULL",
                    value=str(row["value"]) if row["value"] is not None else "",
                    count=int(row["count"]),
                )
                for row in results
            ]

            return FilterPreviewResponse(options=options)

        elif filter_type == "numerical":
            # Get numerical statistics for numerical filter
            query_builder = AggQueryBuilder()
            query_builder.add_aggregate_column(column_name, "min", alias="min_value")
            query_builder.add_aggregate_column(column_name, "max", alias="max_value")

            # For average, we need to cast to float for accurate results
            query_builder.add_column(func.avg(cast(column(column_name), Float)).label("avg_value"))
            query_builder.add_aggregate_column(
                column_name, "count_distinct", alias="distinct_count"
            )

            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))

            # Execute query using charts_service function
            results = execute_query(warehouse_client, query_builder)
            row = results[0]

            stats = {
                "min_value": float(row["min_value"]) if row["min_value"] is not None else 0.0,
                "max_value": float(row["max_value"]) if row["max_value"] is not None else 100.0,
                "avg_value": float(row["avg_value"]) if row["avg_value"] is not None else 50.0,
                "distinct_count": (
                    int(row["distinct_count"]) if row["distinct_count"] is not None else 0
                ),
            }

            return FilterPreviewResponse(stats=stats)

        elif filter_type == "datetime":
            # Get date range for datetime filter
            query_builder = AggQueryBuilder()

            # Different handling for different warehouses
            if org_warehouse.wtype == "postgres":
                # PostgreSQL: cast to date
                query_builder.add_column(
                    func.min(cast(column(column_name), Date)).label("min_date")
                )
                query_builder.add_column(
                    func.max(cast(column(column_name), Date)).label("max_date")
                )
                query_builder.add_column(
                    func.count(distinct(func.date(column(column_name)))).label("distinct_days")
                )
            elif org_warehouse.wtype == "bigquery":
                # BigQuery: use DATE function
                query_builder.add_column(func.min(func.date(column(column_name))).label("min_date"))
                query_builder.add_column(func.max(func.date(column(column_name))).label("max_date"))
                query_builder.add_column(
                    func.count(distinct(func.date(column(column_name)))).label("distinct_days")
                )
            else:
                # Generic approach
                query_builder.add_aggregate_column(column_name, "min", alias="min_date")
                query_builder.add_aggregate_column(column_name, "max", alias="max_date")
                query_builder.add_aggregate_column(
                    column_name, "count_distinct", alias="distinct_days"
                )

            query_builder.add_aggregate_column(None, "count", alias="total_records")
            query_builder.fetch_from(table_name, schema_name)
            query_builder.where_clause(column(column_name).isnot(None))

            # Execute query using charts_service function
            results = execute_query(warehouse_client, query_builder)
            row = results[0]

            # Return as dictionary (not NumericalStatsResponse)
            stats = {
                "min_date": row["min_date"].isoformat() if row["min_date"] else None,
                "max_date": row["max_date"].isoformat() if row["max_date"] else None,
                "distinct_days": int(row["distinct_days"]) if row["distinct_days"] else 0,
                "total_records": int(row["total_records"]) if row["total_records"] else 0,
            }

            return FilterPreviewResponse(stats=stats)

        else:
            raise HttpError(400, f"Invalid filter type: {filter_type}")

    except Exception as e:
        logger.error(f"Error getting filter preview: {str(e)}")
        raise HttpError(500, "Error getting filter preview")
