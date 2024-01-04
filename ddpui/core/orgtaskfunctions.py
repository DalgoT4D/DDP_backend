"""
functions to work with transform related tasks or orgtasks in general
do not raise http errors here
"""

from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import prefect_service
from ddpui.core.pipelinefunctions import setup_dbt_core_task_config
from ddpui.utils.constants import (
    TASK_DBTRUN,
)
from ddpui.utils.helpers import generate_hash_id
from ddpui.ddpdbt.schema import DbtProjectParams

logger = CustomLogger("ddpui")


def create_transform_tasks(
    org: Org, cli_profile_block: OrgPrefectBlockv1, dbt_project_params: DbtProjectParams
):
    """Create all the transform (git, dbt) tasks"""
    for task in Task.objects.filter(type__in=["dbt", "git"]).all():
        org_task = OrgTask.objects.create(org=org, task=task)

        if task.slug == TASK_DBTRUN:
            # create deployment
            hash_code = generate_hash_id(8)
            deployment_name = f"manual-{org.slug}-{task.slug}-{hash_code}"
            dataflow = prefect_service.create_dataflow_v1(
                PrefectDataFlowCreateSchema3(
                    deployment_name=deployment_name,
                    flow_name=deployment_name,
                    orgslug=org.slug,
                    deployment_params={
                        "config": {
                            "tasks": [
                                setup_dbt_core_task_config(
                                    org_task, cli_profile_block, dbt_project_params
                                ).to_json()
                            ]
                        }
                    },
                )
            )

            # store deployment record in django db
            existing_dataflow = OrgDataFlowv1.objects.filter(
                deployment_id=dataflow["deployment"]["id"]
            ).first()
            if existing_dataflow:
                existing_dataflow.delete()

            new_dataflow = OrgDataFlowv1.objects.create(
                org=org,
                name=deployment_name,
                deployment_name=dataflow["deployment"]["name"],
                deployment_id=dataflow["deployment"]["id"],
                dataflow_type="manual",
            )

            DataflowOrgTask.objects.create(
                dataflow=new_dataflow,
                orgtask=org_task,
            )

    return None, None
