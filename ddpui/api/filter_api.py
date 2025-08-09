"""Filter API endpoints for dashboard filters"""

from typing import List, Optional, Dict, Any
from django.shortcuts import get_object_or_404
from ninja import Router, Schema
from ninja.errors import HttpError

from ddpui.auth import has_permission
from ddpui.models.org_user import OrgUser
from ddpui.models.org import OrgWarehouse
from ddpui.core.charts import charts_service
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
    stats: Optional[NumericalStatsResponse] = None


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
        warehouse_client = charts_service.get_warehouse_client(org_warehouse)

        # Get schemas based on warehouse type
        if org_warehouse.wtype == "postgres":
            # For PostgreSQL, get all schemas except system ones
            query = """
            SELECT schema_name as name
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
            """
        elif org_warehouse.wtype == "bigquery":
            # For BigQuery, list datasets
            query = f"""
            SELECT schema_name as name
            FROM `{org_warehouse.bq_location}.INFORMATION_SCHEMA.SCHEMATA`
            ORDER BY schema_name
            """
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        results = warehouse_client.execute(query)
        schemas = [SchemaResponse(name=row[0]) for row in results]

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
        warehouse_client = charts_service.get_warehouse_client(org_warehouse)

        # Get tables based on warehouse type
        if org_warehouse.wtype == "postgres":
            query = """
            SELECT table_name as name
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            results = warehouse_client.execute(query, [schema_name])
        elif org_warehouse.wtype == "bigquery":
            query = f"""
            SELECT table_name as name
            FROM `{org_warehouse.bq_location}.{schema_name}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            results = warehouse_client.execute(query)
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        tables = [TableResponse(name=row[0]) for row in results]

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
    """List columns in a specific table"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = charts_service.get_warehouse_client(org_warehouse)

        # Get columns based on warehouse type
        if org_warehouse.wtype == "postgres":
            query = """
            SELECT column_name as name, data_type as type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            results = warehouse_client.execute(query, [schema_name, table_name])
        elif org_warehouse.wtype == "bigquery":
            query = f"""
            SELECT column_name as name, data_type as type, is_nullable
            FROM `{org_warehouse.bq_location}.{schema_name}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            results = warehouse_client.execute(query)
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        columns = []
        for row in results:
            # Normalize data types
            col_type = row[1].lower()
            if any(t in col_type for t in ["int", "float", "decimal", "numeric", "double"]):
                normalized_type = "number"
            elif any(t in col_type for t in ["date", "time", "timestamp"]):
                normalized_type = "date"
            else:
                normalized_type = "string"

            columns.append(
                ColumnResponse(
                    name=row[0], type=normalized_type, nullable=row[2] == "YES" or row[2] is True
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
    filter_type: str,  # 'value' or 'numerical'
    limit: int = 100,
):
    """Get preview data for a filter (values or numerical stats)"""
    orguser = request.orguser

    # Get org warehouse
    org_warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if not org_warehouse:
        raise HttpError(404, "Warehouse not configured")

    try:
        warehouse_client = charts_service.get_warehouse_client(org_warehouse)

        # Build the table reference based on warehouse type
        if org_warehouse.wtype == "postgres":
            table_ref = f'"{schema_name}"."{table_name}"'
            column_ref = f'"{column_name}"'
        elif org_warehouse.wtype == "bigquery":
            table_ref = f"`{org_warehouse.bq_location}.{schema_name}.{table_name}`"
            column_ref = f"`{column_name}`"
        else:
            raise HttpError(400, f"Unsupported warehouse type: {org_warehouse.wtype}")

        if filter_type == "value":
            # Get distinct values with counts for categorical filter
            query = f"""
            SELECT {column_ref} as value, COUNT(*) as count
            FROM {table_ref}
            WHERE {column_ref} IS NOT NULL
            GROUP BY {column_ref}
            ORDER BY count DESC, {column_ref}
            LIMIT {limit}
            """

            results = warehouse_client.execute(query)
            options = [
                FilterOptionResponse(
                    label=str(row[0]) if row[0] is not None else "NULL",
                    value=str(row[0]) if row[0] is not None else "",
                    count=int(row[1]),
                )
                for row in results
            ]

            return FilterPreviewResponse(options=options)

        elif filter_type == "numerical":
            # Get numerical statistics for numerical filter
            query = f"""
            SELECT 
                MIN({column_ref}) as min_value,
                MAX({column_ref}) as max_value,
                AVG(CAST({column_ref} AS FLOAT)) as avg_value,
                COUNT(DISTINCT {column_ref}) as distinct_count
            FROM {table_ref}
            WHERE {column_ref} IS NOT NULL
            """

            results = warehouse_client.execute(query)
            row = results[0]

            stats = NumericalStatsResponse(
                min_value=float(row[0]) if row[0] is not None else 0.0,
                max_value=float(row[1]) if row[1] is not None else 100.0,
                avg_value=float(row[2]) if row[2] is not None else 50.0,
                distinct_count=int(row[3]) if row[3] is not None else 0,
            )

            return FilterPreviewResponse(stats=stats)

        else:
            raise HttpError(400, f"Invalid filter type: {filter_type}")

    except Exception as e:
        logger.error(f"Error getting filter preview: {str(e)}")
        raise HttpError(500, "Error getting filter preview")
