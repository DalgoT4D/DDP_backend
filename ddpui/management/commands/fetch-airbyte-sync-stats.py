"""for each org fetch orchestration pipelines for airbyte sync"""

from datetime import datetime
import pytz
from django.core.management.base import BaseCommand, CommandParser

from ddpui.models.tasks import DataflowOrgTask
from ddpui.models.airbyte import SyncStats
from ddpui.ddpairbyte.airbyte_service import get_jobs_for_connection, abreq

# from ddpui.ddpairbyte.airbytehelpers import get_job_info_for_connection


class Command(BaseCommand):
    """Docstring"""

    help = "Sync connection stats between the given start_date and end_date for all connection in any/all orgs"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--org", type=str, help="org slug, optional")
        parser.add_argument(
            "--start_date",
            type=str,
            required=True,
            help="Start date in YYYY-MM-DD format (e.g., 2024-07-24)",
        )
        parser.add_argument(
            "--end_date",
            type=str,
            required=True,
            help="End date in YYYY-MM-DD format (e.g., 2024-07-31)",
        )

    def handle(self, *args, **options):
        """for each org, fetch orchestration pipeline(s) for airbyte sync"""
        start_date = datetime.strptime(options["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(options["end_date"], "%Y-%m-%d")

        if start_date > end_date:
            raise ValueError("start_date must be before end_date")

        for dfot in DataflowOrgTask.objects.filter(
            orgtask__task__slug="airbyte-sync",
        ):
            if options["org"] and options["org"] != dfot.orgtask.org.slug:
                continue
            org = dfot.orgtask.org
            sync_type = dfot.dataflow.dataflow_type

            # response = get_jobs_for_connection(dfot.orgtask.connection_id)
            response = abreq(
                "jobs/list",
                {
                    "configTypes": ["sync"],
                    "configId": dfot.orgtask.connection_id,
                    "createdAtStart": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "createdAtEnd": end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "orderByField": "createdAt",
                    "orderByMethod": "DESC",
                },
            )

            for job in response["jobs"]:
                job_id = job["job"]["id"]
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

                    start_time = datetime.fromtimestamp(attempt["createdAt"]).astimezone(pytz.UTC)
                    end_time = datetime.fromtimestamp(attempt["endedAt"]).astimezone(pytz.UTC)
                    duration = (end_time - start_time).total_seconds()

                    if not SyncStats.objects.filter(
                        org=org,
                        connection_id=dfot.orgtask.connection_id,
                        attempt=attempt["id"],
                        job_id=job_id,
                    ).exists():
                        ss = SyncStats.objects.create(
                            org=org,
                            connection_id=dfot.orgtask.connection_id,
                            job_id=job_id,
                            attempt=attempt["id"],
                            status=attempt["status"],
                            sync_type=sync_type,
                            sync_time=start_time,
                            sync_duration_s=duration,
                            sync_records=attempt.get("totalStats", {}).get("recordsEmitted", 0),
                            sync_data_volume_b=attempt.get("totalStats", {}).get("bytesEmitted", 0),
                        )
                        print(ss.to_json())
