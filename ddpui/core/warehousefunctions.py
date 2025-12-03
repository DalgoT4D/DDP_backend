import json
from ninja.errors import HttpError

from ddpui.core import dbtautomation_service
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.helpers import convert_to_standard_types
from ddpui.models.org import OrgWarehouse
from ddpui.models.dbt_workflow import OrgDbtModelType
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("ddpui")


def get_warehouse_data(request, data_type: str, **kwargs):
    """
    Fetches data from a warehouse based on the data type
    and optional parameters
    """
    try:
        org_warehouse = kwargs.get("org_warehouse", None)
        if not org_warehouse:
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
    except Exception as error:
        logger.exception(f"Exception occurred in get_{data_type}: {error}")
        raise HttpError(500, f"Failed to get {data_type}")

    return convert_to_standard_types(data)


def fetch_warehouse_tables(request, org_warehouse, cache_key=None):
    """
    Fetch all the tables from the warehouse
    Cache the results
    """
    res = []
    schemas = get_warehouse_data(request, "schemas", org_warehouse=org_warehouse)
    logger.info(f"Inside helper function for fetching tables : {cache_key}")
    for schema in schemas:
        for table in get_warehouse_data(
            request, "tables", schema_name=schema, org_warehouse=org_warehouse
        ):
            res.append(
                {
                    "schema": schema,
                    "name": table,
                    "type": OrgDbtModelType.SOURCE.value,
                    "id": schema + "-" + table,
                }
            )

    if cache_key:
        RedisClient.get_instance().set(cache_key, json.dumps(res), ex=24 * 60 * 60)

    return res
