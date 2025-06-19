from django.core.management.base import BaseCommand, CommandError
from ddpui.models.org import Org
from ddpui.ddpairbyte.airbytehelpers import fetch_and_update_airbyte_jobs_for_all_connections


class Command(BaseCommand):
    help = "Fetch and update Airbyte jobs for all connections or for a specific org"

    def add_arguments(self, parser):
        parser.add_argument(
            "--last_n_days",
            type=int,
            default=0,
            help="Number of days to look back for jobs (default: 0)",
        )
        parser.add_argument(
            "--orgslug",
            type=str,
            default=None,
            help="Slug of the org to filter jobs (default: all orgs)",
        )
        parser.add_argument(
            "--connection_id",
            type=str,
            default=None,
            help="Connection ID to filter jobs (default: all connections)",
        )

    def handle(self, *args, **options):
        last_n_days = options["last_n_days"]
        orgslug = options["orgslug"]
        connection_id = options["connection_id"]

        if last_n_days <= 0:
            raise CommandError("last_n_days must be greater than 0.")

        org = None
        if orgslug:
            try:
                org = Org.objects.get(slug=orgslug)
            except Org.DoesNotExist:
                raise CommandError(f"Org with slug '{orgslug}' does not exist.")

        fetch_and_update_airbyte_jobs_for_all_connections(
            last_n_days=last_n_days, org=org, connection_id=connection_id
        )
        self.stdout.write(self.style.SUCCESS("Successfully fetched and updated Airbyte jobs."))
