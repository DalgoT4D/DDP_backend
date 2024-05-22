from django.core.management.base import BaseCommand
from ninja.errors import HttpError
from ddpui.ddpprefect.prefect_service import prefect_put
from ddpui.models.org import OrgDataFlowv1
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE, DDP_WORK_QUEUE


class Command(BaseCommand):
    """Docstring"""

    help = "Sets the name of the worker pool for all manual deployments."

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument("--run", action="store_true", help="Update the deployments")
        parser.add_argument("--org")

    def handle(self, *args, **options):
        """Docstring"""
        q_dataflows = OrgDataFlowv1.objects.filter(dataflow_type="manual")
        if options["org"]:
            q_dataflows = q_dataflows.filter(org__slug=options["org"])
        for dataflow in q_dataflows:

            work_queue_name = None

            if (
                dataflow.name.find("airbyte-sync") != -1
                or dataflow.name.find("airbyte-reset") != -1
            ):
                work_queue_name = DDP_WORK_QUEUE
            elif dataflow.name.find("dbt-run") != -1:
                work_queue_name = MANUL_DBT_WORK_QUEUE

            if work_queue_name:
                print(f"{dataflow.name:50} {work_queue_name}")
                if options["run"]:
                    try:
                        res = prefect_put(
                            f"v1/deployments/{dataflow.deployment_id}",
                            {"work_queue_name": work_queue_name},
                        )
                        print(res)
                    except HttpError as e:
                        print(
                            f"Error updating deployment {dataflow.deployment_id}: {e}"
                        )
            else:
                print(f"Could not determine work_queue_name for {dataflow.name}.")
