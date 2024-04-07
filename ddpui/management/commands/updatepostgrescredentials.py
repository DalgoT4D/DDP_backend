from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.secretsmanager import retrieve_warehouse_credentials


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
        for org in Org.objects.all():
            warehouse = OrgWarehouse.objects.filter(org=org).first()
            credentials = retrieve_warehouse_credentials(warehouse)
            if "tunnel_method" not in credentials:
                credentials["tunnel_method"] = {"tunnel_method": "NO_TUNNEL"}
                warehouse.credentials = credentials
                warehouse.save()
                print(f"Updated {org.name}")
            else:
                print(f"Skipping {org.name}")
        print("Done.")
