"""constructs and returns an instance of the client for the right warehouse"""

from ddpui.utils.warehouse.old_client.postgres import PostgresClient
from ddpui.utils.warehouse.old_client.bigquery import BigQueryClient
from ddpui.utils.warehouse.old_client.warehouse_interface import WarehouseInterface


def get_client(warehouse: str, conn_info: dict = None, location: str = None) -> WarehouseInterface:
    """constructs and returns an instance of the client for the right warehouse"""
    if warehouse == "postgres":
        client = PostgresClient(conn_info)
    elif warehouse == "bigquery":
        client = BigQueryClient(conn_info, location)
    else:
        raise ValueError("unknown warehouse")
    return client
