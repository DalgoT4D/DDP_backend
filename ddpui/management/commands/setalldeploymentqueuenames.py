from django.core.management.base import BaseCommand

from ddpui.ddpprefect.prefect_service import prefect_put
from ddpui.models.org import OrgDataFlowv1


class Command(BaseCommand):
    """Docstring"""

    help = "Sets the name of the worker pool for all manual deployments."

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument("--run", action="store_true", help="Update the deployments")

    def handle(self, *args, **options):
        """Docstring"""
        for dataflow in OrgDataFlowv1.objects.filter(dataflow_type="manual"):

            work_queue_name = None

            if dataflow.name.find("airbyte-sync") != -1:
                work_queue_name = "manual-sync"
            elif dataflow.name.find("dbt-run") != -1:
                work_queue_name = "manual-dbt"

            if work_queue_name:
                print(f"{dataflow.name:50} {work_queue_name}")
                if options["run"]:
                    res = prefect_put(
                        f"v1/deployments/{dataflow.deployment_id}",
                        {"work_queue_name": work_queue_name},
                    )
                    print(res)
            else:
                print(f"Could not determine work_queue_name for {dataflow.name}.")
