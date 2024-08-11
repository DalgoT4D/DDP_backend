""" create the dataflow for edr send report """

from pathlib import Path
from django.core.management.base import BaseCommand

from ddpui.models.org import Org, OrgDataFlowv1, OrgDbt
from ddpui.models.tasks import OrgTask, DataflowOrgTask
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
        parser.add_argument(
            "org", type=str, help='Org slug, provide "all" to fix links'
        )
        parser.add_argument("--cron", type=str, default="0 0 * * *")
        parser.add_argument(
            "--fix-links",
            action="store_true",
            help="Create missing links between OrgTask and OrgDataFlowv1",
        )

    def create_dataflow(self, org: Org, org_task: OrgTask, cron: str):
        """create the DataflowOrgTask for the orgtask"""
        print("No existing dataflow found for generate-edr, creating one")

        dbt_project_params, error = gather_dbt_project_params(org)
        if error:
            print(error)
            return None

        dbt_env_dir = Path(org.dbt.dbt_venv)

        task_config = setup_edr_send_report_task_config(
            org_task, dbt_project_params.project_dir, dbt_env_dir
        )
        print("task_config for deployment")
        print(task_config.to_json())

        hash_code = generate_hash_id(8)
        deployment_name = (
            f"pipeline-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
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
            f"creating OrgDataFlowv1 named {dataflow['deployment']['name']} with deployment_id {dataflow['deployment']['id']}"
        )
        orgdataflow = OrgDataFlowv1.objects.create(
            org=org,
            name=dataflow["deployment"]["name"],
            deployment_name=dataflow["deployment"]["name"],
            deployment_id=dataflow["deployment"]["id"],
            dataflow_type="manual",  # we dont want it to show in flows/pipelines page
            cron=cron,
        )
        return orgdataflow

    def handle(self, *args, **options):

        if options["fix_links"]:
            for org in Org.objects.exclude(dbt__isnull=True):

                org_task = get_edr_send_report_task(org)
                if org_task is None:
                    print(f"no edr OrgTask found for {org.slug}, skipping")
                    continue

                if DataflowOrgTask.objects.filter(orgtask=org_task).exists():
                    print(f"DataflowOrgTask already exists for {org.slug}, skipping")
                    continue

                deployment_name_prefix = (
                    f"pipeline-{org_task.org.slug}-{org_task.task.slug}-"
                )
                dataflow = OrgDataFlowv1.objects.filter(
                    org=org,
                    deployment_name__startswith=deployment_name_prefix,
                ).first()
                if dataflow is None:
                    print(f"no OrgDataFlowv1 found for {org.slug}, skipping")
                    continue

                DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)
                print(f"created DataflowOrgTask for {org.slug}")

            return

        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return

        if not org.dbt:
            print(f"OrgDbt for {org.slug} not found")
            return

        org_task = get_edr_send_report_task(org)
        if org_task is None:
            print("creating OrgTask for edr-send-report")
            org_task = get_edr_send_report_task(org, create=True)

        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

        if dataflow_orgtask is None:
            dataflow = self.create_dataflow(org, org_task, options["cron"])
            DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)
