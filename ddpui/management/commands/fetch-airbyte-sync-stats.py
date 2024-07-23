"""for each org fetch orchestration pipelines for airbyte sync"""

from datetime import datetime
import pytz
from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.tasks import DataflowOrgTask
from ddpui.models.syncstats import SyncStats
from ddpui.ddpairbyte.airbyte_service import get_jobs_for_connection

# from ddpui.ddpairbyte.airbytehelpers import get_job_info_for_connection


class Command(BaseCommand):
    """Docstring"""

    help = "Updates the mapping with the correct value of dataflow type in orgdataflow table"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--org", type=str, help="org slug, optional")

    def handle(self, *args, **options):
        """for each org, fetch orchestration pipeline(s) for airbyte sync"""
        for dfot in DataflowOrgTask.objects.filter(
            orgtask__task__slug="airbyte-sync",
        ):
            if options["org"] and options["org"] != dfot.orgtask.org.slug:
                continue
            org = dfot.orgtask.org
            sync_type = dfot.dataflow.dataflow_type

            response = get_jobs_for_connection(dfot.orgtask.connection_id)

            for job in response["jobs"]:
                for attempt in job["attempts"]:
                    attempt: dict
                    # attempt = {
                    #     "id": 0,
                    #     "status": "succeeded",
                    #     "createdAt": 1719468189,
                    #     "updatedAt": 1719468223,
                    #     "endedAt": 1719468223,
                    #     "totalStats": {
                    #         "recordsEmitted": 0,
                    #         "bytesEmitted": 0,
                    #         "recordsCommitted": 0,
                    #     },
                    #     "streamStats": [],
                    # }

                    start_time = datetime.fromtimestamp(
                        attempt["createdAt"]
                    ).astimezone(pytz.UTC)
                    end_time = datetime.fromtimestamp(attempt["endedAt"]).astimezone(
                        pytz.UTC
                    )
                    duration = (end_time - start_time).total_seconds()

                    if not SyncStats.objects.filter(
                        org=org,
                        connection_id=dfot.orgtask.connection_id,
                        attempt=attempt["id"],
                    ).exists():
                        ss = SyncStats.objects.create(
                            org=org,
                            connection_id=dfot.orgtask.connection_id,
                            attempt=attempt["id"],
                            status=attempt["status"],
                            sync_type=sync_type,
                            sync_time=start_time,
                            sync_duration_s=duration,
                            sync_records=attempt.get("totalStats", {}).get(
                                "recordsEmitted", 0
                            ),
                            sync_data_volume_b=attempt.get("totalStats", {}).get(
                                "bytesEmitted", 0
                            ),
                        )
                        print(ss.to_json())
