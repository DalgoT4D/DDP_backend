from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.ddpprefect import prefect_service


class Command(BaseCommand):
    """
    This script lets us edit the dbt cli profile for a Postgres warehouse
    """

    help = "Edit a dbt cli profile"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument("--show", action="store_true", help="Show the current dbt cli profile")
        parser.add_argument("--remove", help="key to remove")
        parser.add_argument("--add", help="key to add")
        parser.add_argument("--value", help="value to add")

    def handle(self, *args, **options):
        """Edit a dbt cli profile"""
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        warehouse = OrgWarehouse.objects.filter(org=org).first()
        if warehouse is None:
            print(f"Warehouse does not exist for org {org.slug}")
            return
        dbt_credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

        if options["show"]:
            print(dbt_credentials)
            return

        if options["remove"]:
            key = options["remove"]
            if key in dbt_credentials:
                del dbt_credentials[key]
            else:
                print(f"key {key} not found in dbt cli profile")
                return

        if options["add"]:
            key = options["add"]
            value = options["value"]
            dbt_credentials[key] = value

        secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=org, block_type=DBTCLIPROFILE
        ).first()

        if cli_profile_block is None:
            print(f"dbt cli profile block does not exist for org {org.slug}")
            return

        result = prefect_service.update_dbt_cli_profile_block(
            block_name=cli_profile_block.block_name,
            wtype=warehouse.wtype,
            credentials=dbt_credentials,
        )
        print(result)
        # print("dbt cli profile updated")
