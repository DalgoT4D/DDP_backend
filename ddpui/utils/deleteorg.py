import uuid
from ninja.errors import HttpError
from ddpui.models.org_user import Org, OrgUser, OrgUserRole
from ddpui.models.org import OrgWarehouse
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.models.org import OrgPrefectBlockv1
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.ddpdbt import dbt_service
from ddpui.utils import secretsmanager
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def is_valid_uuid(s):
    """returns whether or not a string is a UUID"""
    try:
        uuid.UUID(s)
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def delete_dbt_orgtasks(org: Org):
    """deletes dbt org tasks"""
    logger.info("deleting dbt org tasks")
    for org_task in OrgTask.objects.filter(org=org, task__type="dbt").all():
        logger.info(f"deleting org task {org_task.task.slug} ")
        org_task.delete()


def delete_dbt_workspace(org: Org):  # skipcq: PYL-R0201
    """deletes the dbt workspace"""
    dbt_service.delete_dbt_workspace(org)


def delete_warehouse_v1(org: Org):  # skipcq: PYL-R0201
    """
    deletes airbyte/django/prefect destinations, connections
    """
    logger.info(
        "=============== Remove Dataflows/deployments: connections & reset connections ==========="
    )
    for dataflow_orgtask in DataflowOrgTask.objects.filter(
        orgtask__org=org, orgtask__task__slug__in=[TASK_AIRBYTESYNC, TASK_AIRBYTERESET]
    ):
        dataflow = dataflow_orgtask.dataflow
        try:
            prefect_service.delete_deployment_by_id(dataflow.deployment_id)
        except HttpError:
            pass
        dataflow.delete()
        logger.info(f"Deleted deployment - {dataflow.deployment_id}")

    logger.info(
        "================ Remove OrgTasks:connections & reset connections ==========="
    )
    for org_task in OrgTask.objects.filter(
        org=org, task__slug__in=[TASK_AIRBYTESYNC, TASK_AIRBYTERESET]
    ).all():
        try:
            if org_task.connection_id:
                logger.info("deleting connection in Airbyte " + org_task.connection_id)
                airbyte_service.delete_connection(
                    org.airbyte_workspace_id, org_task.connection_id
                )
                logger.info(f"deleted connection in Airbyte - {org_task.connection_id}")
        except Exception:
            pass
        org_task.delete()

    logger.info(
        "================ Remove remaining airbyte connections & destinations (warehouse) ==========="
    )
    if is_valid_uuid(org.airbyte_workspace_id):
        for connection in airbyte_service.get_connections(org.airbyte_workspace_id)[
            "connections"
        ]:
            logger.info("deleting connection in Airbyte " + connection["connectionId"])
            airbyte_service.delete_connection(
                org.airbyte_workspace_id, connection["connectionId"]
            )
            logger.info(f"deleted connection in Airbyte - {connection['connectionId']}")

        for destination in airbyte_service.get_destinations(org.airbyte_workspace_id)[
            "destinations"
        ]:
            logger.info(
                "deleting destination in Airbyte " + destination["destinationId"]
            )
            airbyte_service.delete_destination(
                org.airbyte_workspace_id, destination["destinationId"]
            )
            logger.info(
                f"deleted destination in Airbyte - {destination['destinationId']}"
            )

    logger.info(
        "================ Remove django warehouse and the credentials in secrets manager ==========="
    )
    for warehouse in OrgWarehouse.objects.filter(org=org):
        secretsmanager.delete_warehouse_credentials(warehouse)
        warehouse.delete()


def delete_airbyte_workspace_v1(org: Org):  # skipcq: PYL-R0201
    """
    deletes airbyte sources and the workspace
    deletes airbyte/django/prefect server blocks
    """

    if is_valid_uuid(org.airbyte_workspace_id):
        for source in airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]:
            logger.info("deleting source in Airbyte " + source["sourceId"])
            airbyte_service.delete_source(org.airbyte_workspace_id, source["sourceId"])

        try:
            airbyte_service.delete_workspace(org.airbyte_workspace_id)
        except Exception:
            pass

    for block in OrgPrefectBlockv1.objects.filter(
        org=org, block_type=AIRBYTESERVER
    ).all():
        try:
            prefect_service.prefect_delete_a_block(block.block_id)
        except Exception:
            pass
        block.delete()


def delete_orgusers(org: Org):  # skipcq: PYL-R0201
    """delete all login users"""
    for orguser in OrgUser.objects.filter(org=org):
        orguser.delete()
        # this deletes the orguser as well via CASCADE


def display_org(org: Org):
    """show org"""
    account_admin = OrgUser.objects.filter(
        org=org, role=OrgUserRole.ACCOUNT_MANAGER
    ).first()
    logger.info(
        "OrgName: %-25s %-25s Airbyte workspace ID: %-40s Admin: %s",
        org.name,
        org.slug,
        org.airbyte_workspace_id,
        account_admin.user.email if account_admin else "<unknonw>",
    )


def delete_one_org(org: Org):
    """delete one org"""
    delete_dbt_orgtasks(org)
    delete_dbt_workspace(org)
    if org.airbyte_workspace_id:
        delete_airbyte_workspace_v1(org)
    delete_warehouse_v1(org)
    delete_orgusers(org)
    org.delete()
