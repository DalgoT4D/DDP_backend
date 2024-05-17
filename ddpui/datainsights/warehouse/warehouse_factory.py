from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.postgres import PostgresClient


class WarehouseFactory:

    @classmethod
    def connect(cls, creds: dict, wtype: str) -> Warehouse:
        if wtype == "postgres":
            return PostgresClient(creds)
        else:
            raise ValueError("Column type not supported for insights generation")
