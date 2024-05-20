import json
import sqlalchemy
from dbt_automation.utils.warehouseclient import get_client
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
import sqlalchemy.exc

from ddpui import auth
from ddpui.core import dbtautomation_service
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger
from ddpui.auth import has_permission

from ddpui.datainsights.insights.insight_factory import InsightsFactory
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.generate_result import GenerateResult

from ddpui.schemas.warehouse_api_schemas import ColumnMetrics

warehouseapi = NinjaAPI(urls_namespace="warehouse")
logger = CustomLogger("ddpui")


@warehouseapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@warehouseapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@warehouseapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.exception(exc)
    return Response({"detail": "something went wrong"}, status=500)


def get_warehouse_data(request, data_type: str, **kwargs):
    """
    Fetches data from a warehouse based on the data type
    and optional parameters
    """
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()

        data = []
        client = dbtautomation_service._get_wclient(org_warehouse)
        if data_type == "tables":
            data = client.get_tables(kwargs["schema_name"])
        elif data_type == "schemas":
            data = client.get_schemas()
        elif data_type == "table_columns":
            data = client.get_table_columns(kwargs["schema_name"], kwargs["table_name"])
        elif data_type == "table_data":
            data = client.get_table_data(
                schema=kwargs["schema_name"],
                table=kwargs["table_name"],
                limit=kwargs["limit"],
                page=kwargs["page"],
                order_by=kwargs["order_by"],
                order=kwargs["order"],
            )
            for element in data:
                for key, value in element.items():
                    if (isinstance(value, list) or isinstance(value, dict)) and value:
                        element[key] = json.dumps(value)
    except Exception as error:
        logger.exception(f"Exception occurred in get_{data_type}: {error}")
        raise HttpError(500, f"Failed to get {data_type}")

    return data


@warehouseapi.get("/tables/{schema_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_table(request, schema_name: str):
    """Fetches table names from a warehouse"""
    return get_warehouse_data(request, "tables", schema_name=schema_name)


@warehouseapi.get("/schemas", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_schema(request):
    """Fetches schema names from a warehouse"""
    return get_warehouse_data(request, "schemas")


@warehouseapi.get(
    "/table_columns/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_warehouse_data"])
def get_table_columns(request, schema_name: str, table_name: str):
    """Fetches column names for a specific table from a warehouse"""
    return get_warehouse_data(
        request, "table_columns", schema_name=schema_name, table_name=table_name
    )


@warehouseapi.get(
    "/table_data/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_warehouse_data"])
def get_table_data(
    request,
    schema_name: str,
    table_name: str,
    page: int = 1,
    limit: int = 10,
    order_by: str = None,
    order: int = 1,
):
    """Fetches data from a specific table in a warehouse"""
    return get_warehouse_data(
        request,
        "table_data",
        schema_name=schema_name,
        table_name=table_name,
        page=page,
        limit=limit,
        order_by=order_by,
        order=order,
    )


@warehouseapi.get(
    "/table_count/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_warehouse_data"])
def get_table_count(request, schema_name: str, table_name: str):
    """Fetches the total number of rows for a specified table."""
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()

        client = dbtautomation_service._get_wclient(org_warehouse)
        total_rows = client.get_total_rows(schema_name, table_name)
        return {"total_rows": total_rows}
    except Exception as e:
        logger.error(f"Failed to fetch total rows for {schema_name}.{table_name}: {e}")
        raise HttpError(
            500, f"Failed to fetch total rows for {schema_name}.{table_name}"
        )


@warehouseapi.get("/dbt_project/json_columnspec/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_json_column_spec(
    request, source_schema: str, input_name: str, json_column: str
):
    """Get the json column spec of a table in a warehouse"""
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    if not all([source_schema, input_name, json_column]):
        raise HttpError(400, "Missing required parameters")

    json_columnspec = dbtautomation_service.json_columnspec(
        org_warehouse, source_schema, input_name, json_column
    )
    return json_columnspec


@warehouseapi.get(
    "/v1/table_data/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_warehouse_data"])
def get_table_data_v1(request, schema_name: str, table_name: str):
    """
    Get the json column spec of a table in a warehouse
    This fetches table data using the sqlalchemy engine
    """
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

    try:
        cols = wclient.get_table_columns(schema_name, table_name)
        return cols
    except sqlalchemy.exc.NoSuchTableError:
        raise HttpError(404, "Table not found")
    except Exception as err:
        logger.error(err)
        raise HttpError(500, err)


@warehouseapi.post("/insights/metrics/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_data_insights(request, payload: ColumnMetrics):
    """
    Run all the require queries to fetch insights for a column
    Will also save to redis results as queries are processed
    """
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

    try:
        col_type = wclient.get_col_python_type(
            payload.db_schema, payload.db_table, payload.column_name
        )

        if not col_type:
            raise ValueError(
                f"Column '{payload.column_name}' not found in '{payload.db_schema}.{payload.db_table}'"
            )

        insight_obj = InsightsFactory.initiate_insight(
            payload.column_name,
            payload.db_table,
            payload.db_schema,
            col_type,
            payload.filter,
            wclient.get_wtype(),
        )

        return GenerateResult.generate_col_insights(org, insight_obj, wclient)
    except Exception as err:
        raise HttpError(500, str(err))
