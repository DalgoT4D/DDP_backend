"""
Script to verify that the warehouse in secrets manager is the same as the one in airbyte
"""

from logging import ERROR
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.ddpairbyte import airbyte_service


class Command(BaseCommand):
    """
    Script to verify that the warehouse in secrets manager is the same as the one in airbyte
    """

    def handle(self, *args, **options):
        airbyte_service.logger.logger.setLevel(ERROR)
        secretsmanager.logger.logger.setLevel(ERROR)

        for org in Org.objects.all():
            warehouse = OrgWarehouse.objects.filter(org=org).first()
            if warehouse is None:
                continue
            warehouse_secret = secretsmanager.retrieve_warehouse_credentials(warehouse)
            res = airbyte_service.abreq(
                "destinations/get", {"destinationId": warehouse.airbyte_destination_id}
            )
            print(org.slug)
            if warehouse.wtype == "postgres":
                for key in ["host", "database", "port", "username"]:
                    if warehouse_secret[key] != res["connectionConfiguration"][key]:
                        print(
                            f'{key} mismatch: {warehouse_secret[key]} vs {res["connectionConfiguration"][key]}'
                        )
            elif warehouse.wtype == "bigquery":
                # the credentials_json is not available, and the dataset_id is not available
                if warehouse_secret["project_id"] != res["connectionConfiguration"]["project_id"]:
                    print(
                        f'project_id mismatch: {warehouse_secret["project_id"]} vs {res["connectionConfiguration"]["project_id"]}'
                    )
            print("=" * 80)
