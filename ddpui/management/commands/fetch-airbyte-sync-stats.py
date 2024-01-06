"""for each org fetch orchestration pipelines for airbyte sync"""
from datetime import datetime
from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.tasks import DataflowOrgTask
from ddpui.ddpairbyte.airbyte_service import get_jobs_for_connection

# from ddpui.ddpairbyte.airbytehelpers import get_job_info_for_connection


class Command(BaseCommand):
    """Docstring"""

    help = "Updates the mapping with the correct value of dataflow type in orgdataflow table"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--org", type=str, help="org slug")

    def handle(self, *args, **options):
        """for each org, fetch orchestration pipeline(s) for airbyte sync"""
        for dfot in DataflowOrgTask.objects.filter(
            orgtask__task__slug="airbyte-sync", dataflow__dataflow_type="orchestrate"
        ):
            if options["org"] and options["org"] != dfot.orgtask.org.slug:
                continue
            org = dfot.orgtask.org
            response = get_jobs_for_connection(dfot.orgtask.connection_id)
            for job in response["jobs"]:
                for attempt in job["attempts"]:
                    timestamp = datetime.fromtimestamp(attempt["createdAt"])
                    bytes_synced = attempt["bytesSynced"]
                    yyyymmdd = timestamp.strftime("%Y-%m-%d")
                    hhmm = timestamp.strftime("%H:%M:%S")
                    print(f"{org.slug:20} {yyyymmdd:20} {hhmm:6} {bytes_synced}")
