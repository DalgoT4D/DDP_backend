""" Sync flow runs from prefect's db into ours; this is just one way. No new runs are added from prefect in our db """

from pathlib import Path
from django.core.management.base import BaseCommand
from ninja.errors import HttpError
from django.db.models import Window
from django.db.models.functions import RowNumber

from ddpui.models.org import Org, OrgDataFlowv1, OrgDbt
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.ddpprefect import (
    prefect_service,
    FLOW_RUN_RUNNING_STATE_NAME,
    FLOW_RUN_PENDING_STATE_NAME,
)
from ddpui.api.webhook_api import create_or_update_flowrun


class Command(BaseCommand):
    """Sync flow runs from prefect's db into ours; this is just one way. No new runs are added from prefect in our db"""

    help = "Sync flow runs from prefect's db into ours; this is just one way. No new runs are added from prefect in our db"

    def add_arguments(self, parser):
        parser.add_argument(
            "--delete-runs",
            action="store_true",
            help="Delets flow runs that are not found in prefect",
        )

    def handle(self, *args, **options):
        """run the command"""
        diff_state_runs_updated = 0
        flow_runs_to_delete = []

        # remove all duplicates with the same flow_run_id
        # use row over number to do this
        flow_runs_with_row_number = PrefectFlowRun.objects.filter(
            flow_run_id__isnull=False
        ).annotate(
            row_number=Window(
                expression=RowNumber(),
                partition_by="flow_run_id",
                order_by="-start_time",
            )
        )
        duplicate_runs = flow_runs_with_row_number.filter(row_number__gt=1)

        for run in duplicate_runs:
            run.delete()

        print(f"Deleted duplicate flow runs: {duplicate_runs.count()}")

        # run through all current flows and update their attributes
        for flow_run in PrefectFlowRun.objects.order_by("-start_time").all():

            # get the flow run from prefect
            try:
                prefect_flow_run = prefect_service.get_flow_run(flow_run.flow_run_id)
            except HttpError as err:
                if err.status_code == 404:
                    print(f"Flow run {flow_run.flow_run_id} not found in prefect")
                    flow_runs_to_delete.append(flow_run.flow_run_id)
                else:
                    print(f"Error getting flow run {flow_run.flow_run_id} - {str(err)}")
                continue
            except Exception as err:
                print(f"Error getting flow run {flow_run.flow_run_id} - {str(err)}")
                continue

            if flow_run.state_name != prefect_flow_run["state_name"]:
                print(f"Updating flow run {flow_run.flow_run_id}")
                diff_state_runs_updated += 1
                create_or_update_flowrun(prefect_flow_run, flow_run.deployment_id)
                print(f"Updated flow run {flow_run.flow_run_id}")

        print(f"Updated flow runs in db: {diff_state_runs_updated}")

        flow_runs_in_non_terminal_state = PrefectFlowRun.objects.filter(
            state_name__in=[FLOW_RUN_RUNNING_STATE_NAME, FLOW_RUN_PENDING_STATE_NAME]
        ).count()

        print(
            f"Total flow runs in non-terminal state: {flow_runs_in_non_terminal_state}"
        )

        print(f"Deleted duplicate flow runs: {duplicate_runs.count()}")

        print(f"Flow runs not found in prefect : {len(flow_runs_to_delete)}")

        if options["delete_runs"]:
            print("Deleting flow runs not found in prefect")
            PrefectFlowRun.objects.filter(flow_run_id__in=flow_runs_to_delete).delete()
            print("Deleted flow runs not found in prefect")
        else:
            print(
                "Skipping delete of flow runs. Use --delete-runs to delete flow runs not found in prefect"
            )
