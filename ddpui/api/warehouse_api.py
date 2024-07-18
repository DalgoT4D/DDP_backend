import json
import uuid
import sqlalchemy
from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
import sqlalchemy.exc

from django.http import StreamingHttpResponse
from ddpui import auth
from ddpui.core import dbtautomation_service
from ddpui.models.org import OrgWarehouse
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.taskprogress import TaskProgress
from ddpui.auth import has_permission

from ddpui.datainsights.insights.insight_factory import InsightsFactory
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.generate_result import GenerateResult, poll_for_column_insights

from ddpui.schemas.warehouse_api_schemas import RequestorColumnSchema
from ddpui.utils import secretsmanager

warehouseapi = NinjaAPI(urls_namespace="warehouse")
logger = CustomLogger("ddpui")

from decimal import Decimal
from django.http import JsonResponse


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {key: self.default(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.default(element) for element in obj]
        elif isinstance(obj, tuple):
            return tuple(self.default(element) for element in obj)
        return super(DecimalEncoder, self).default(obj)


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
    return JsonResponse(
        get_warehouse_data(
            request,
            "table_data",
            schema_name=schema_name,
            table_name=table_name,
            page=page,
            limit=limit,
            order_by=order_by,
            order=order,
        ),
        encoder=DecimalEncoder,
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
        raise HttpError(500, str(err))


@warehouseapi.post("/insights/metrics/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_data_insights(request, payload: RequestorColumnSchema):
    """
    Run all the require queries to fetch insights for a column
    Will also save to redis results as queries are processed
    """
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:

        task_id = str(uuid.uuid4())

        taskprogress = TaskProgress(task_id, TaskProgressHashPrefix.DATAINSIGHTS)
        taskprogress.add(
            {
                "message": "Fetching insights",
                "status": GenerateResult.RESULT_STATUS_FETCHING,
                "results": [],
            }
        )

        poll_for_column_insights.delay(org_warehouse.id, payload.dict(), task_id)

        return {"task_id": task_id}
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))


@warehouseapi.get(
    "/download/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_view_warehouse_data"])
def get_download_warehouse_data(request, schema_name: str, table_name: str):
    """Stream and download data from a table in the warehouse"""

    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    def stream_warehouse_data(
        request, schema_name, table_name, page_size=10, order_by=None, order=1
    ):
        page = 0
        header_written = False
        while True:
            data = get_warehouse_data(
                request,
                "table_data",
                schema_name=schema_name,
                table_name=table_name,
                page=page,
                limit=page_size,
                order_by=order_by,
                order=order,
            )
            if not data:
                break
            if not header_written:
                yield ",".join(data[0].keys()) + "\n"  # Write CSV header
                header_written = True
            for row in data:
                yield ",".join(map(str, row.values())) + "\n"  # Write CSV row
            page += 1
            logger.info(f"Streaming page {page} of {schema_name}.{table_name}")

    response = StreamingHttpResponse(
        stream_warehouse_data(request, schema_name, table_name, page_size=30000),
        content_type="application/octet-stream",
    )
    response["Content-Disposition"] = (
        f"attachment; filename={schema_name}__{table_name}.csv"
    )

    return response
