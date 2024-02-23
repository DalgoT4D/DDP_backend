from django.core.management.base import BaseCommand
from ddpui.models.org import OrgWarehouse
from ddpui.ddpairbyte import airbyte_service

import logging

logger = logging.getLogger("migration")


class Command(BaseCommand):
    """
    This script updates the location for bigquery warehouses from airbyte
    """

    help = "My shiny new management command."

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        for org_warehouse in OrgWarehouse.objects.all():
            if org_warehouse.wtype == "bigquery":
                try:
                    destination = airbyte_service.get_destination(
                        org_warehouse.org.airbyte_workspace_id,
                        org_warehouse.airbyte_destination_id,
                    )
                    org_warehouse.bq_location = (
                        destination["connectionConfiguration"]["dataset_location"]
                        if "connectionConfiguration" in destination
                        and "dataset_location" in destination["connectionConfiguration"]
                        else None
                    )
                    org_warehouse.save()
                    logger.info(
                        f"Updated the bq location for the org '{org_warehouse.org.slug}' and warehouse '{org_warehouse.name}'"
                    )
                except Exception:
                    logger.error(
                        f"failed to fetch airbyte destination for the warehouse '{org_warehouse.name}' in org '{org_warehouse.org.slug}'"
                    )
