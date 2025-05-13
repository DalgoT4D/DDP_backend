import time
from django.core.management.base import BaseCommand
from ddpui.models.org import OrgDataFlowv1
from ddpui.ddpprefect import prefect_service


class Command(BaseCommand):
    """Pause & then resume scheduled deployments. This is a handy script when we see duplicate schedules or when we upgrade prefect"""

    help = "Pause & then resume scheduled deployments. This is a handy script when we see duplicate schedules or when we upgrade prefect"

    def add_arguments(self, parser):
        """Docstring"""
        parser.add_argument("--org")
        parser.add_argument("--time_in_secs", type=int, default=5)

    def handle(self, *args, **options):
        """Docstring"""
        q_dataflows = OrgDataFlowv1.objects.filter(dataflow_type="orchestrate")
        if options["org"]:
            q_dataflows = q_dataflows.filter(org__slug=options["org"])

        for dataflow in q_dataflows:
            # only those deployments with active schedules should be refreshed
            deployment = prefect_service.get_deployment(dataflow.deployment_id)

            sleep_time = options["time_in_secs"]

            if deployment["isScheduleActive"]:
                # deactivate the deployment (schedule)
                try:
                    prefect_service.set_deployment_schedule(dataflow.deployment_id, "inactive")
                    print(
                        f"Deactivated the schedule(s) for dataflow {dataflow.name}|{dataflow.deployment_id}"
                    )
                except Exception as e:
                    print(
                        f"Failed to deactive the schedule(s) for dataflow {dataflow.name}|{dataflow.deployment_id}: {e}"
                    )
                    continue

                time.sleep(sleep_time)

                # activate the deployment (schedule)
                try:
                    prefect_service.set_deployment_schedule(dataflow.deployment_id, "active")
                    print(
                        f"Activated the schedule(s) back again for dataflow {dataflow.name}|{dataflow.deployment_id}"
                    )
                except Exception as e:
                    print(
                        f"Failed to activate the schedule(s) for dataflow {dataflow.name}|{dataflow.deployment_id}: {e}"
                    )
