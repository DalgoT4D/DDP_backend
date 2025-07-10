from ddpui.models.org import (
    Org,
    OrgSchemaChange,
)
from ddpui.ddpprefect.schema import (
    PrefectDataFlowUpdateSchema3,
)
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

logger = CustomLogger("airbyte")


def delete_connection(org: Org, connection_id: str):
    """deletes an airbyte connection"""

    dataflows_to_delete: dict[int, OrgDataFlowv1] = {}
    orgtask_to_delete: list[OrgTask] = []

    # delete manual-sync and manual-reset dataflows
    for org_task in OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ):
        for dataflow_orgtask in DataflowOrgTask.objects.filter(
            orgtask=org_task, dataflow__dataflow_type="manual"
        ):
            # if there is a reset and a sync then the dataflow will appear twice in this loop
            # ... that's why we use a dict keyed on the dataflow.id instead of a list
            dataflows_to_delete[dataflow_orgtask.dataflow.id] = dataflow_orgtask.dataflow
            logger.info("will delete %s", dataflow_orgtask.dataflow.deployment_name)

        orgtask_to_delete.append(org_task)

    # delete all deployments
    for dataflow in dataflows_to_delete.values():
        logger.info("deleting prefect deployment %s", dataflow.deployment_name)
        prefect_service.delete_deployment_by_id(dataflow.deployment_id)

    # delete all dataflows
    logger.info("deleting org dataflows from db")
    for dataflow in dataflows_to_delete.values():
        # if there is a reset and a sync then the dataflow will appear twice in this list
        dataflow.delete()
    dataflows_to_delete = None

    # remove from orchestration dataflows
    for org_task in OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ):
        # this org_task is already in the to_delete list from above

        for dataflow_orgtask in DataflowOrgTask.objects.filter(
            orgtask=org_task, dataflow__dataflow_type="orchestrate"
        ):
            # fetch config from prefect
            deployment = prefect_service.get_deployment(dataflow_orgtask.dataflow.deployment_id)
            # { name, deploymentId, tags, cron, isScheduleActive, parameters }
            # parameters = {config: {org_slug, tasks}}
            # tasks = list of
            #    {seq, slug, type, timeout, orgtask__uuid, connection_id, airbyte_server_block}
            parameters = deployment["parameters"]
            # logger.info(parameters)
            tasks_to_keep = []
            for task in parameters["config"]["tasks"]:
                if task.get("connection_id") == connection_id:
                    logger.info(f"deleting task {task['slug']} from deployment")
                else:
                    tasks_to_keep.append(task)
            parameters["config"]["tasks"] = tasks_to_keep
            # logger.info(parameters)
            if len(parameters["config"]["tasks"]) > 0:
                payload = PrefectDataFlowUpdateSchema3(deployment_params=parameters)
                prefect_service.update_dataflow_v1(dataflow_orgtask.dataflow.deployment_id, payload)
                logger.info("updated deployment %s", dataflow_orgtask.dataflow.deployment_name)
            else:
                prefect_service.delete_deployment_by_id(dataflow_orgtask.dataflow.deployment_id)

    # delete all orgtasks
    for org_task in orgtask_to_delete:
        logger.info("deleting orgtask %s", org_task.task.slug)
        org_task.delete()

    # delete airbyte connection
    logger.info("deleting airbyte connection %s", connection_id)
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    ndeleted, _ = OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
    logger.info(f"Deleted {ndeleted} schema changes for connection {connection_id}")

    return None, None
