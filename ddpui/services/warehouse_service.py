"""Warehouse service for business logic

This module encapsulates all warehouse-related business logic,
separating it from the API layer for better testability and maintainability.
"""

from typing import Optional

from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.warehouse_service")


class WarehouseService:
    """Service class for warehouse-related operations"""

    @staticmethod
    def get_org_warehouse(org: Org) -> Optional[OrgWarehouse]:
        """Get the warehouse for an organization.

        Args:
            org: The organization

        Returns:
            OrgWarehouse instance or None if not configured
        """
        return OrgWarehouse.objects.filter(org=org).first()
