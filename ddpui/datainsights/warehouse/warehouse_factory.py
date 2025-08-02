from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.postgres import PostgresClient
from ddpui.datainsights.warehouse.bigquery import BigqueryClient
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType
from ddpui.models.org import OrgWarehouse
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials


class WarehouseFactory:
    @classmethod
    def connect(cls, creds: dict, wtype: str) -> Warehouse:
        if wtype == WarehouseType.POSTGRES:
            return PostgresClient(creds)
        elif wtype == WarehouseType.BIGQUERY:
            return BigqueryClient(creds)
        else:
            raise ValueError("Column type not supported for insights generation")

    @classmethod
    def get_warehouse_client(cls, org_warehouse: OrgWarehouse) -> Warehouse:
        if not org_warehouse:
            raise ValueError("Organization warehouse not configured")

        creds = retrieve_warehouse_credentials(org_warehouse)
        if not creds:
            raise ValueError("Warehouse credentials not found")

        return cls.connect(creds, org_warehouse.wtype)
