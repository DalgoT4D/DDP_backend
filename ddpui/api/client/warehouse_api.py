import json

from dbt_automation.utils.warehouseclient import get_client
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

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
        wtype = org_warehouse.wtype
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

        if wtype == "bigquery":
            credentials = json.loads(credentials)

        data = []
        client = get_client(wtype, credentials)
        if data_type == "tables":
            data = client.get_tables(kwargs["schema_name"])
        elif data_type == "schemas":
            data = client.get_schemas()
        elif data_type == "table_columns":
            data = client.get_table_columns(kwargs["schema_name"], kwargs["table_name"])
        elif data_type == "table_data":
            limit = 10
            data = client.get_table_data(
                kwargs["schema_name"], kwargs["table_name"], limit
            )
    except Exception as error:
        logger.exception(f"Exception occurred in get_{data_type}: {error}")
        raise HttpError(500, f"Failed to get {data_type}")

    return data


@warehouseapi.get("/tables/{schema_name}", auth=auth.CanManagePipelines())
def get_table(request, schema_name: str):
    """Fetches table names from a warehouse"""
    return get_warehouse_data(request, "tables", schema_name=schema_name)


@warehouseapi.get("/schemas", auth=auth.CanManagePipelines())
def get_schema(request):
    """Fetches schema names from a warehouse"""
    return get_warehouse_data(request, "schemas")


@warehouseapi.get(
    "/table_columns/{schema_name}/{table_name}", auth=auth.CanManagePipelines()
)
def get_table_columns(request, schema_name: str, table_name: str):
    """Fetches column names for a specific table from a warehouse"""
    return get_warehouse_data(
        request, "table_columns", schema_name=schema_name, table_name=table_name
    )


@warehouseapi.get(
    "/table_data/{schema_name}/{table_name}", auth=auth.CanManagePipelines()
)
def get_table_data(request, schema_name: str, table_name: str):
    """Fetches data from a specific table in a warehouse"""
    return get_warehouse_data(
        request, "table_data", schema_name=schema_name, table_name=table_name
    )
