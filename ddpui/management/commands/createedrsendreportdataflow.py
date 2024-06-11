""" create the dataflow for edr send report """

from pathlib import Path
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.core.dbtfunctions import gather_dbt_project_params
from ddpui.ddpprefect import prefect_service
from ddpui.utils.helpers import generate_hash_id
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE


class Command(BaseCommand):
    """Create the dataflow for edr send report"""

    help = "Create the dataflow for edr send report"

    def add_arguments(self, parser):
        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument(
            "--schedule", choices=["manual", "orchestrate"], default="manual"
        )
        parser.add_argument("--cron", type=str, default="0 0 * * *")

    def handle(self, *args, **options):

        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        org_task = get_edr_send_report_task(org)
        if org_task is None:
            print("creating OrgTask for edr-send-report")
            org_task = get_edr_send_report_task(org, create=True)

        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

        dataflow = dataflow_orgtask.dataflow if dataflow_orgtask else None
        if dataflow is None:
            print("No existing dataflow found for generate-edr, creating one")

            dbt_project_params, error = gather_dbt_project_params(org)
            if error:
                print(error)
                return

            dbt_env_dir = Path(org.dbt.dbt_venv)

            task_config = setup_edr_send_report_task_config(
                org_task, dbt_project_params.project_dir, dbt_env_dir
            )
            print("task_config for deployment")
            print(task_config.to_json())

            hash_code = generate_hash_id(8)
            deployment_name = (
                f"manual-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
            )
            print(f"creating deployment {deployment_name}")

            dataflow = prefect_service.create_dataflow_v1(
                PrefectDataFlowCreateSchema3(
                    deployment_name=deployment_name,
                    flow_name=deployment_name,
                    orgslug=org_task.org.slug,
                    deployment_params={
                        "config": {
                            "tasks": [task_config.to_json()],
                            "org_slug": org_task.org.slug,
                        }
                    },
                ),
                MANUL_DBT_WORK_QUEUE,
            )

            print(
                f"creating `{options['schedule']}` OrgDataFlowv1 named {dataflow['deployment']['name']} with deployment_id {dataflow['deployment']['id']}"
            )
            OrgDataFlowv1.objects.create(
                org=org,
                name=dataflow["deployment"]["name"],
                deployment_name=dataflow["deployment"]["name"],
                deployment_id=dataflow["deployment"]["id"],
                dataflow_type=options["schedule"],
                cron=options["cron"] if options["schedule"] == "orchestrate" else None,
            )
