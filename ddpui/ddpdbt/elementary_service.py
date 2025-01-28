""" functions to set up elementary """

from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import OrgTask
from ddpui.core.pipelinefunctions import setup_edr_send_report_task_config
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.utils.helpers import generate_hash_id
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpprefect import MANUL_DBT_WORK_QUEUE


def create_edr_sendreport_dataflow(org: Org, org_task: OrgTask, cron: str):
    """create the DataflowOrgTask for the orgtask"""
    print("No existing dataflow found for generate-edr, creating one")

    dbt_project_params: DbtProjectParams = None
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
    except Exception as error:
        print(error)
        return None

    task_config = setup_edr_send_report_task_config(
        org_task, dbt_project_params.project_dir, dbt_project_params.venv_binary
    )
    print("task_config for deployment")
    print(task_config.to_json())

    hash_code = generate_hash_id(8)
    deployment_name = f"pipeline-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
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
            cron=cron,
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
