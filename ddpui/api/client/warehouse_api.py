import os
from dbt_automation.utils.warehouseclient import get_client
from ninja import NinjaAPI
from ddpui import auth
from ninja.errors import ValidationError

from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
import os
import json
from ninja import NinjaAPI

from ninja.errors import ValidationError
from ninja.responses import Response

from pydantic.error_wrappers import ValidationError as PydanticValidationError
from ddpui import auth

from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger


warehouseapi = NinjaAPI(urls_namespace="warehouse")
logger = CustomLogger("airbyte")


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


@warehouseapi.get("/tables/{schema_name}", auth=auth.CanManagePipelines())
def get_table(request, schema_name: str):
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).last()
        wtype = org_warehouse.wtype
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
        if wtype == "postgres":
            credentials = credentials
        elif wtype == "bigquery":
            credentials = json.loads(credentials)

        client = get_client(wtype, credentials)
        if wtype == "postgres":
            tables = client.get_tables(schema_name)
        elif wtype == "bigquery":
            tables = client.get_tables(schema_name)
    except Exception as error:
        logger.exception("Exception occurred in get_table: %s", error)
        tables = []

    return {"tables": tables}


@warehouseapi.get("/schemas", auth=auth.CanManagePipelines())
def get_schema(request):
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()
        wtype = org_warehouse.wtype

        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
        if wtype == "postgres":
            credentials = credentials
        elif wtype == "bigquery":
            credentials = json.loads(credentials)

        client = get_client(wtype, credentials)
        if wtype == "postgres":
            client = get_client(wtype, credentials)
            schemas = client.get_schemas()
        elif wtype == "bigquery":
            client = get_client(wtype, credentials)
            schemas = client.get_schemas()
    except Exception as error:
        logger.exception("Exception occurred in get_schema: %s", error)
        schemas = []

    return {"schemas": schemas}


@warehouseapi.get(
    "/table_columns/{schema_name}/{table_name}", auth=auth.CanManagePipelines()
)
def get_table_columns(request, schema_name: str, table_name: str):
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)
        wtype = org_warehouse.wtype
        if wtype == "postgres":
            credentials = credentials
        elif wtype == "bigquery":
            credentials = json.loads(credentials)

        client = get_client(wtype, credentials)
        if wtype == "postgres":
            data = client.get_table_columns(schema_name, table_name)
        elif wtype == "bigquery":
            data = client.get_table_columns(schema_name, table_name)

    except Exception as error:
        logger.exception("Exception occurred in get_table_columns: %s", error)
        data = []

    return {"data": data}


@warehouseapi.get(
    "/table_data/{schema_name}/{table_name}", auth=auth.CanManagePipelines()
)
def get_table_data(request, schema_name: str, table_name: str):
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()
        wtype = org_warehouse.wtype
        credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

        if wtype == "postgres":
            credentials = credentials
        elif wtype == "bigquery":
            credentials = json.loads(credentials)

        client = get_client(wtype, credentials)
        limit = 10
        if wtype == "postgres":
            data = client.get_table_data(schema_name, table_name, limit)
        elif wtype == "bigquery":
            data = client.get_table_data(schema_name, table_name, limit)

    except Exception as error:
        logger.exception("Exception occurred in get_table_data: %s", error)
        data = []

    return {"data": data}
