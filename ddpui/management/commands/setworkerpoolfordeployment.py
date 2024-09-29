from django.core.management.base import BaseCommand

from ddpui.ddpprefect.prefect_service import prefect_put


class Command(BaseCommand):
    """Docstring"""

    help = "Sets the name of the worker pool for a deployment."

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument("--deployment-id", type=str, help="The deployment ID.", required=True)
        parser.add_argument(
            "--work-queue-name",
            type=str,
            help="The name of the work queue to set for the deployment.",
            required=True,
        )

    def handle(self, *args, **options):
        """Docstring"""
        deployment_id = options["deployment_id"]
        work_queue_name = options["work_queue_name"]

        res = prefect_put(
            f"v1/deployments/{deployment_id}",
            {"work_queue_name": work_queue_name},
        )
        print(res)
