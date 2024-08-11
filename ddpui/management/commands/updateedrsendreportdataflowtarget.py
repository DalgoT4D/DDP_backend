""" create the dataflow for edr send report """

from pathlib import Path
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDbt
from ddpui.models.tasks import OrgTask
from ddpui.models.tasks import DataflowOrgTask
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.core.orgtaskfunctions import fetch_elementary_profile_target
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.core.dbtfunctions import gather_dbt_project_params
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import PrefectDataFlowUpdateSchema3


class Command(BaseCommand):
    """Update the edr --profile-target in orgtask as well as the generate edr deployments"""

    help = "Update the edr --profile-target in orgtask as well as the generate edr deployments"

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            type=str,
            help="Org slug: use 'all' to run for all orgs at once",
            required=True,
        )

    def handle(self, *args, **options):
        orgs = Org.objects.all()
        if options["org"] != "all":
            orgs = orgs.filter(slug=options["org"])

        for org in orgs:
            print(f"running for {org.slug}")

            org_dbt = OrgDbt.objects.filter(org=org).first()
            if not org_dbt:
                print(f"OrgDbt for {org.slug} not found")
                continue

            org_task = get_edr_send_report_task(org, orgdbt=org_dbt, overwrite=True)
            if org_task is None:
                print(f"edr orgtask not found {org.slug}; skipping to the next org")
                continue
            else:
                print(
                    f"Updated the edr org_task {org.slug} with the new elementary target"
                )

            dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

            dataflow = dataflow_orgtask.dataflow if dataflow_orgtask else None
            if dataflow is None:
                print(
                    f"No generate edr dataflow found for {org.slug}; skipping to the next org"
                )
                continue

            # update the deployment in prefect with the profile-target
            try:
                dbt_project_params, error = gather_dbt_project_params(org)
                if error:
                    print(error)
                    continue

                dbt_env_dir = Path(org_dbt.dbt_venv)

                task_config = setup_edr_send_report_task_config(
                    org_task, dbt_project_params.project_dir, dbt_env_dir
                )

                prefect_service.update_dataflow_v1(
                    dataflow.deployment_id,
                    PrefectDataFlowUpdateSchema3(
                        deployment_params={
                            "config": {
                                "tasks": [task_config.to_json()],
                                "org_slug": org_task.org.slug,
                            }
                        }
                    ),
                )

                print(
                    f"Updated the generate edr deployment with new target ;deployment_id {dataflow.deployment_id}"
                )

            except Exception as err:
                print(str(err))
                print(
                    f"Failed to update the generate edr deployment with deployment_id {dataflow.deployment_id}. Skipping to the next org"
                )
