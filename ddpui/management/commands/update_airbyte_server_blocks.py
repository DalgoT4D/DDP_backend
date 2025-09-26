from django.core.management.base import BaseCommand
from ddpui.models.org import Org, OrgPrefectBlockv1
from ddpui.ddpprefect import prefect_service


class Command(BaseCommand):
    """Update airbyte server blocks in prefect"""

    help = "Update airbyte server blocks in prefect"

    def add_arguments(self, parser):
        """adds command line arguments"""
        parser.add_argument("org", type=str, help="Org slug, use 'all' to update for all orgs")

    def handle(self, *args, **options):
        """Use airbyte host and port from the .env"""
        orgs = Org.objects.all()
        if options["org"] != "all":
            orgs = orgs.filter(slug=options["org"])

        print(f"Updating airbyte server blocks in prefect for {len(orgs)} orgs")

        for org in orgs:
            server_block = OrgPrefectBlockv1.objects.filter(org=org).first()

            if not server_block:
                print(f"Org {org.slug} does not have a server block")
                continue

            # update host and port
            try:
                prefect_service.update_airbyte_server_block(server_block.block_name)
                print(f"Updated airbyte server block for org {org.slug}")
            except Exception as e:
                print(f"Error updating airbyte server block for org {org.slug}: {e}")
                continue
