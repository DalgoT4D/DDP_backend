from ddpui.utils.warehouse.client.warehouse_interface import Warehouse
from ddpui.utils.warehouse.client.postgres import PostgresClient
from ddpui.utils.warehouse.client.bigquery import BigqueryClient
from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType
from ddpui.models.org import OrgWarehouse
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials


class WarehouseFactory:
    @classmethod
    def connect(cls, creds: dict, wtype: str) -> Warehouse:
        """
        Build a warehouse client. Cheap to call per request: the underlying engine and
        its connection pool are shared per credentials by the engine registry, so only
        the thin client wrapper is new.
        """
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
