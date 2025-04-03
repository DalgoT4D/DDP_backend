from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.ddpprefect.prefect_service import (
    estimate_time_for_next_queued_run_of_dataflow,
    compute_dataflow_run_times_from_history,
)
from ddpui.ddpprefect.schema import DeploymentRunTimes, DeploymentCurrentQueueTime


class Command(BaseCommand):
    """This script lets us compute estimated time for queued runs. It also does the computation of run times"""

    help = "Estimate time for queued runs"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help="Org slug; use 'all' to run for all orgs at once")
        parser.add_argument(
            "--deployment_id", type=str, help="Dataflows's deployment id", required=False
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="No of last x flow runs to look for computing run times",
            default=20,
        )
        parser.add_argument("--compute-runtimes", action="store_true")

    def handle(self, *args, **options):
        orgs = Org.objects.all()
        if options["org"] != "all":
            orgs = orgs.filter(slug=options["org"])

        for org in orgs:
            print("=" * 40 + org.slug + "=" * 40)

            dataflows = OrgDataFlowv1.objects.filter(org=org)
            if "deployment_id" in options and options["deployment_id"]:
                dataflows = dataflows.filter(deployment_id=options["deployment_id"])

            limit = options["limit"] or 20

            if options["compute_runtimes"]:
                print("Compute run times is set to true")
                for dataflow in dataflows:
                    print(
                        f"Computing the runs times over last {limit} flow runs for dataflow {dataflow.name}"
                    )

                    run_times: DeploymentRunTimes = compute_dataflow_run_times_from_history(
                        dataflow, limit=limit
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
