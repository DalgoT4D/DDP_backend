"""Test endpoints for chart APIs without authentication"""
from ninja import Router
from ninja.errors import HttpError
from ddpui.models.org import OrgWarehouse
from ddpui.core import dbtautomation_service
from ddpui.datainsights.query_builder import QueryBuilder, AggQueryBuilder
from sqlalchemy import column
from sqlalchemy.dialects import postgresql

test_router = Router()


@test_router.get("/test-connection")
def test_connection(request):
    """Test basic connection without auth"""
    return {"status": "ok", "message": "API is reachable"}


@test_router.get("/test-warehouse")
def test_warehouse_connection(request):
    """Test warehouse connection"""
    try:
        # Get first org warehouse (for testing only)
        org_warehouse = OrgWarehouse.objects.first()
        if not org_warehouse:
            return {"error": "No warehouse configured"}

        warehouse = dbtautomation_service._get_wclient(org_warehouse)
        result = warehouse.execute("SELECT 1 as test")

        return {
            "status": "connected",
            "warehouse_type": org_warehouse.wtype,
            "org": org_warehouse.org.name,
            "test_result": result[0] if result else None,
        }
    except Exception as e:
        return {"error": str(e)}


@test_router.post("/test-query")
def test_query_builder(request, payload: dict):
    """Test query builder without auth"""
    try:
        if payload.get("computation_type") == "raw":
            qb = QueryBuilder()
            qb.fetch_from(payload["table_name"], payload["schema_name"])

            if payload.get("x_axis"):
                qb.add_column(payload["x_axis"])
            if payload.get("y_axis"):
                qb.add_column(payload["y_axis"])

            qb.limit_rows(payload.get("limit", 10))

        else:  # aggregated
            qb = AggQueryBuilder()
            qb.fetch_from(payload["table_name"], payload["schema_name"])

            if payload.get("dimension_col"):
                qb.add_column(column(payload["dimension_col"]))
            if payload.get("aggregate_col") and payload.get("aggregate_func"):
                qb.add_aggregate_column(
                    payload["aggregate_col"],
                    payload["aggregate_func"],
                    f"{payload['aggregate_func']}_{payload['aggregate_col']}",
                )
            if payload.get("dimension_col"):
                qb.group_cols_by(payload["dimension_col"])

            qb.limit_rows(payload.get("limit", 10))

        # Build and compile query
        stmt = qb.build()
        compiled = stmt.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
        sql = str(compiled)

        return {
            "status": "success",
            "sql": sql,
            "computation_type": payload.get("computation_type"),
            "table": f"{payload['schema_name']}.{payload['table_name']}",
        }

    except Exception as e:
        return {"error": str(e), "type": type(e).__name__}


@test_router.get("/test-schemas")
def test_schemas(request):
    """Get schemas without auth for testing"""
    try:
        org_warehouse = OrgWarehouse.objects.first()
        if not org_warehouse:
            return {"error": "No warehouse configured"}

        warehouse = dbtautomation_service._get_wclient(org_warehouse)
        schemas = warehouse.get_schemas()

        return {"status": "success", "schemas": schemas, "count": len(schemas)}
    except Exception as e:
        return {"error": str(e)}


@test_router.get("/test-tables/{schema_name}")
def test_tables(request, schema_name: str):
    """Get tables without auth for testing"""
    try:
        org_warehouse = OrgWarehouse.objects.first()
        if not org_warehouse:
            return {"error": "No warehouse configured"}

        warehouse = dbtautomation_service._get_wclient(org_warehouse)
        tables = warehouse.get_tables(schema_name)

        return {"status": "success", "schema": schema_name, "tables": tables, "count": len(tables)}
    except Exception as e:
        return {"error": str(e)}
