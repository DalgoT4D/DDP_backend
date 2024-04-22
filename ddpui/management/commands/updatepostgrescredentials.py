from django.core.management.base import BaseCommand

from ddpui.models.org import OrgWarehouse
from ddpui.utils.secretsmanager import (
    retrieve_warehouse_credentials,
    update_warehouse_credentials,
)


class Command(BaseCommand):
    """
    Iterates through postgres warehouses,
    Retrieves credentials from the secrets manager,
    If there is no "tunnel_method" key,
    Add
      tunnel_method: { "tunnel_method": "NO_TUNNEL" }
    Save the credentials back to the secrets manager
    """

    help = "Adds tunnel_method: NO_TUNNEL to the postgres credentials in the secrets manager."

    def handle(self, *args, **options):
        """For every org"""
        for warehouse in OrgWarehouse.objects.filter(wtype="postgres"):
            credentials = retrieve_warehouse_credentials(warehouse)
            if "tunnel_method" not in credentials:
                credentials["tunnel_method"] = {"tunnel_method": "NO_TUNNEL"}
                update_warehouse_credentials(warehouse, credentials)
                print(f"Added tunnel_method for postgres {warehouse.org.name}")
            else:
                print(f"Skipping {warehouse.org.name}")
        for warehouse in OrgWarehouse.objects.filter(wtype="bigquery"):
            credentials = retrieve_warehouse_credentials(warehouse)
            if "tunnel_method" in credentials:
                del credentials["tunnel_method"]
                update_warehouse_credentials(warehouse, credentials)
                print(f"Remove tunnel_method from bigquery {warehouse.org.name}")
        print("Done.")
