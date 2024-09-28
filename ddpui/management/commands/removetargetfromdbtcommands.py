"""dbt commands in our deployemnts have the --target parameter. this script removes them"""

import json
from django.core.management.base import BaseCommand

from ddpui.models.tasks import DataflowOrgTask
from ddpui.ddpprefect.prefect_service import prefect_put, prefect_get


class Command(BaseCommand):
    """dbt commands in our deployemnts have the --target parameter. this script removes them"""

    help = "Removes the --target parameter from dbt commands in deployments"

    def add_arguments(self, parser):
        """add arguments to the command"""
        parser.add_argument(
            "--modify",
            action="store_true",
            help="Without this flag, just print the changes that would be made",
        )
        parser.add_argument("--stop-at-one", action="store_true", help="Stop at one deployment")

    def handle(self, *args, **options):
        # Your code to remove the --target parameter from dbt commands goes here
        for dforgtask in DataflowOrgTask.objects.filter(orgtask__task__type="dbt"):
            is_modified = False
            dataflow = dforgtask.dataflow

            print(
                dforgtask.orgtask.org.slug,
                dataflow.deployment_id,
                dataflow.deployment_name,
            )
            prefect_deployment = prefect_get(f"deployments/{dataflow.deployment_id}")
            deployment_params = prefect_deployment["parameters"]

            print(json.dumps(deployment_params, indent=2))

            for task in deployment_params["config"]["tasks"]:
                if task["type"] == "dbt Core Operation":
                    # there is (usually?) only one command in a dbt task
                    assert len(task["commands"]) == 1
                    eidx = task["commands"][0].find("--target")
                    if eidx > -1:
                        task["commands"][0] = task["commands"][0][:eidx].strip()
                        is_modified = True

            if options["modify"] and is_modified:
                print(json.dumps(deployment_params, indent=2))
                res = prefect_put(
                    f"v1/deployments/{dataflow.deployment_id}",
                    {"deployment_params": deployment_params},
                )
                print(res)
            else:
                print("No changes made")
            if options["stop_at_one"]:
                break
