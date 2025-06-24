from django.core.management.base import BaseCommand, CommandError

from ddpui.models.org import OrgDataFlowv1
from ddpui.ddpprefect import prefect_service
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET, TASK_AIRBYTECLEAR
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3


class Command(BaseCommand):
    help = "Updates the timeout parameter in Prefect deployments for Airbyte tasks"

    def add_arguments(self, parser):
        parser.add_argument("--timeout", type=int, help="New timeout value in seconds")
        parser.add_argument(
            "--deployment_id",
            type=str,
            default="",
            help="Update timeout of a particular deployment. Its ID should be passed as an argument",
        )
        parser.add_argument(
            "--orgslug",
            type=str,
            help="Update timeout for all deployments in a specific organization",
        )

    def handle(self, *args, **options):
        timeout = options.get("timeout")
        deployment_id = options.get("deployment_id")
        orgslug = options.get("orgslug")

        if not deployment_id and not orgslug:
            raise CommandError("You must provide either a deployment ID or an organization slug.")

        dataflows = OrgDataFlowv1.objects

        if orgslug:
            dataflows = dataflows.filter(org__slug=orgslug)

        if deployment_id:
            dataflows = dataflows.filter(deployment_id=deployment_id)

        if not dataflows.exists():
            raise CommandError(f"No deployments found for organization slug: {orgslug}")

        for dataflow in dataflows:
            deployment = prefect_service.get_deployment(
                dataflow.deployment_id,
            )

            parameters = deployment.get("parameters", {})
            for task in parameters.get("config", {}).get("tasks", []):
                task_slug = task.get("slug", "")
                if task_slug in [
                    TASK_AIRBYTECLEAR,
                    TASK_AIRBYTERESET,
                    TASK_AIRBYTESYNC,
                ] or task.get("connection_id", None):
                    self.stdout.write(
                        f"updating timeout for task {task_slug} in deployment {dataflow.deployment_name}|{dataflow.deployment_id} "
                    )
                    task["timeout"] = timeout

            payload = PrefectDataFlowUpdateSchema3(deployment_params=parameters)
            prefect_service.update_dataflow_v1(dataflow.deployment_id, payload)
            self.stdout.write(
                f"updated timeout for deployment {dataflow.deployment_name} with id : {dataflow.deployment_id}"
            )
