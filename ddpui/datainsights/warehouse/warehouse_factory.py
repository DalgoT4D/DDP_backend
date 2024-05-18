from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.postgres import PostgresClient
from ddpui.datainsights.warehouse.bigquery import BigqueryClient


class WarehouseFactory:

    @classmethod
    def connect(cls, creds: dict, wtype: str) -> Warehouse:
        if wtype == "postgres":
            return PostgresClient(creds)
        elif wtype == "bigquery":
            return BigqueryClient(creds)
        else:
            raise ValueError("Column type not supported for insights generation")
