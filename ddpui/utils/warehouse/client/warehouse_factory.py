from typing import TYPE_CHECKING

from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.postgres import PostgresClient
from ddpui.utils.warehouse.client.bigquery import BigqueryClient
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType

if TYPE_CHECKING:
    from ddpui.models.org import OrgWarehouse


class WarehouseFactory:
    @classmethod
    def connect(cls, creds: dict, wtype: str, location: str | None = None) -> Warehouse:
        if wtype == WarehouseType.POSTGRES:
            return PostgresClient(creds)
        elif wtype == WarehouseType.BIGQUERY:
            return BigqueryClient(creds, location)
        else:
            raise ValueError("Column type not supported for insights generation")

    @classmethod
    def get_warehouse_client(cls, org_warehouse: "OrgWarehouse") -> Warehouse:
        if not org_warehouse:
            raise ValueError("Organization warehouse not configured")

        from ddpui.utils.secretsmanager import retrieve_warehouse_credentials

        creds = retrieve_warehouse_credentials(org_warehouse)
        if not creds:
            raise ValueError("Warehouse credentials not found")

        return cls.connect(creds, org_warehouse.wtype, org_warehouse.bq_location)
