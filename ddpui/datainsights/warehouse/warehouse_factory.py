from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.postgres import PostgresClient
from ddpui.datainsights.warehouse.bigquery import BigqueryClient
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType


class WarehouseFactory:

    @classmethod
    def connect(cls, creds: dict, wtype: str) -> Warehouse:
        if wtype == WarehouseType.POSTGRES:
            return PostgresClient(creds)
        elif wtype == WarehouseType.BIGQUERY:
            return BigqueryClient(creds)
        else:
            raise ValueError("Column type not supported for insights generation")
