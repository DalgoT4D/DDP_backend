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


class ConnectionPooledWarehouseFactory:
    """Factory for creating warehouse clients with connection pooling"""

    # Connection pool configurations per org tier
    CONNECTION_CONFIGS = {
        "small": {
            "pool_size": 3,
            "max_overflow": 1,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        },
        "medium": {
            "pool_size": 5,
            "max_overflow": 2,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        },
        "large": {
            "pool_size": 10,
            "max_overflow": 5,
            "pool_timeout": 60,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        },
    }

    @classmethod
    def connect(cls, creds: dict, wtype: str, connection_tier: str = "medium") -> Warehouse:
        """Create warehouse connection with connection pooling"""

        connection_config = cls.CONNECTION_CONFIGS.get(
            connection_tier, cls.CONNECTION_CONFIGS["medium"]
        )

        if wtype == WarehouseType.POSTGRES:
            return PostgresClient(creds, connection_config=connection_config)
        elif wtype == WarehouseType.BIGQUERY:
            return BigqueryClient(creds, connection_config=connection_config)
        else:
            raise ValueError(f"Warehouse type {wtype} not supported for connection pooling")

    @classmethod
    def get_warehouse_client(
        cls, org_warehouse: OrgWarehouse, connection_tier: str = "medium"
    ) -> Warehouse:
        """Get warehouse client with connection pooling"""
        if not org_warehouse:
            raise ValueError("Organization warehouse not configured")

        creds = retrieve_warehouse_credentials(org_warehouse)
        if not creds:
            raise ValueError("Warehouse credentials not found")

        return cls.connect(creds, org_warehouse.wtype, connection_tier)
