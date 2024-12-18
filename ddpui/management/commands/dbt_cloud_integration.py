from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.ddpprefect import prefect_service


class Command(BaseCommand):
    """
    This script lets us run tasks/jobs related to dbt cloud integrations in dalgo
    """

    help = "Dbt cloud related tasks"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument(
            "--api-key", type=str, help="Api key for your dbt cloud account", required=True
        )
        parser.add_argument(
            "--account-id", type=int, help="Account id for your dbt cloud account", required=True
        )

    def handle(self, *args, **options):
        """
        Create/update dbt cloud creds block
        This should be replaced by configuration option on settings panel where users can add this
        """
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        if options["api_key"] and options["account_id"]:
            block: OrgPrefectBlockv1 = prefect_service.create_or_update_dbt_cloud_creds_block(
                org, options["account_id"], options["api_key"]
            )
            print("DBT Cloud credentials block created/updated %s", block.block_name)
            return
