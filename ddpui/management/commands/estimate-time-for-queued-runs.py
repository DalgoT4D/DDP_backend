from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.ddpprefect.prefect_service import (
    estimate_time_for_next_queued_run_of_dataflow,
    compute_dataflow_run_times_from_history,
)
from ddpui.ddpprefect.schema import DeploymentRunTimes, DeploymentCurrentQueueTime


class Command(BaseCommand):
    """
    This script lets us edit the dbt cli profile for a Postgres warehouse
    """

    help = "Estimate time for queued runs"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument(
            "--deployment_id", type=str, help="Dataflows's deployment id", required=False
        )
        parser.add_argument(
            "--look_back", type=int, help="No of days to look for computing run times", default=7
        )

    def handle(self, *args, **options):
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        dataflows = OrgDataFlowv1.objects.filter(org=org)
        if "deployment_id" in options and options["deployment_id"]:
            dataflows = dataflows.filter(deployment_id=options["deployment_id"])

        days_to_look_back = options["look_back"] or 7

        for dataflow in dataflows:
            print(
                f"Computing the runs times for last {days_to_look_back} days for dataflow {dataflow.name}"
            )
            run_times: DeploymentRunTimes = compute_dataflow_run_times_from_history(
                dataflow, days_to_look=days_to_look_back
            )

            print(
                f"Run times for {dataflow.name} for last seven days in seconds : {run_times.dict()} "
            )

        print("Computing the current queue position and time for each dataflow")
        for dataflow in dataflows:
            try:
                current_queue: DeploymentCurrentQueueTime = (
                    estimate_time_for_next_queued_run_of_dataflow(dataflow)
                )

                print(f"Current queue time for {dataflow.name} : {current_queue}")
            except Exception as err:
                print("Failed to compute current queue time " + str(err))
