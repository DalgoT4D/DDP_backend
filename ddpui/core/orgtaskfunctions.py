"""
functions to work with transform related tasks or orgtasks in general
do not raise http errors here
"""

import uuid
from ddpui.models.tasks import OrgTask, Task, DataflowOrgTask
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
)
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import prefect_service
from ddpui.core.pipelinefunctions import setup_dbt_core_task_config
from ddpui.utils.constants import TASK_DBTRUN
from ddpui.utils.helpers import generate_hash_id

logger = CustomLogger("ddpui")


def create_default_transform_tasks(
    org: Org, cli_profile_block: OrgPrefectBlockv1, dbt_project_params: DbtProjectParams
):
    """Create all the transform (git, dbt) tasks"""
    if org.dbt is None:
        raise ValueError("dbt is not configured for this org")

    # if transform_type is "ui" then we don't set up git-pull
    task_types = ["dbt", "git"] if org.dbt.transform_type == "git" else ["dbt"]
    for task in Task.objects.filter(type__in=task_types, is_system=True).all():
        org_task = OrgTask.objects.create(org=org, task=task, uuid=uuid.uuid4())

        if task.slug == TASK_DBTRUN:
            # create deployment
            create_prefect_deployment_for_dbtcore_task(
                org_task, cli_profile_block, dbt_project_params
            )

    return None, None


def create_prefect_deployment_for_dbtcore_task(
    org_task: OrgTask,
    cli_profile_block: OrgPrefectBlockv1,
    dbt_project_params: DbtProjectParams,
):
    """
    create a prefect deployment for a single dbt command and save the deployment id to an OrgDataFlowv1 object
    """
    hash_code = generate_hash_id(8)
    deployment_name = f"manual-{org_task.org.slug}-{org_task.task.slug}-{hash_code}"
    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org_task.org.slug,
            deployment_params={
                "config": {
                    "tasks": [
                        setup_dbt_core_task_config(
                            org_task, cli_profile_block, dbt_project_params
                        ).to_json()
                    ],
                    "org_slug": org_task.org.slug,
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
        org=org_task.org,
        name=deployment_name,
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",
    )

    DataflowOrgTask.objects.create(
        dataflow=new_dataflow,
        orgtask=org_task,
    )

    return new_dataflow


def delete_orgtask(org_task: OrgTask):
    """Delete an orgtask; along with its deployment if its there"""

    for dataflow_orgtask in DataflowOrgTask.objects.filter(
        orgtask=org_task
    ).all():  # only long running task like TASK_DBTRUN, TASK_AIRBYTESYNC will have dataflow

        # delete the manual deployment for this
        dataflow = dataflow_orgtask.dataflow
        if dataflow:
            logger.info(f"deleting manual deployment for {org_task.task.slug}")

            # do this in try catch because it can fail & throw error
            try:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            except Exception:
                pass
            logger.info("FINISHED deleting manual deployment for dbt run")
            logger.info("deleting OrgDataFlowv1")
            dataflow.delete()

        logger.info("deleting DataflowOrgTask")
        dataflow_orgtask.delete()

    logger.info("deleting org task %s", org_task.task.slug)
    org_task.delete()

    return None, None
