import uuid
from ninja.errors import HttpError
from ddpui.models.org_user import Org, OrgUser
from ddpui.models.org import OrgWarehouse, OrgDataFlowv1
from ddpui.models.tasks import OrgTask, DataflowOrgTask
from ddpui.models.org import OrgPrefectBlockv1
from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect import AIRBYTESERVER, DBTCLIPROFILE, SECRET
from ddpui.ddpdbt import dbt_service
from ddpui.utils import secretsmanager
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET
from ddpui.auth import ACCOUNT_MANAGER_ROLE

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


def delete_orgprefectblocks(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """delete all prefect blocks: secrets, cli profiles, shell blocks"""
    for block in OrgPrefectBlockv1.objects.filter(org=org).all():
        logger.info("will delete prefect block %s", block.block_name)
        if not dry_run:
            try:
                prefect_service.prefect_delete_a_block(block.block_id)
            except Exception:
                pass
            block.delete()


def delete_orgdataflows(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """delete all dataflows"""
    for odf in OrgDataFlowv1.objects.filter(org=org).all():
        logger.info("will delete dataflow %s", odf.deployment_name)
        if not dry_run:
            prefect_service.delete_deployment_by_id(odf.deployment_id)
            odf.delete()


def delete_warehouse_v1(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """
    deletes airbyte/django/prefect destinations, connections
    """
    logger.info("=============== Remove all Dataflows/deployments===========")
    for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask__org=org):
        dataflow = dataflow_orgtask.dataflow
        logger.info("will delete %s from prefect", dataflow.deployment_name)
        if not dry_run:
            try:
                prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            except HttpError:
                pass
            dataflow.delete()
            logger.info(f"Deleted deployment - {dataflow.deployment_id}")

    logger.info("================ Remove OrgTasks:connections & reset connections ===========")
    for org_task in OrgTask.objects.filter(org=org).all():
        try:
            if org_task.connection_id:
                logger.info("will delete connection %s in Airbyte ", org_task.connection_id)
                if not dry_run:
                    airbyte_service.delete_connection(
                        org.airbyte_workspace_id, org_task.connection_id
                    )
                    logger.info(f"deleted connection in Airbyte - {org_task.connection_id}")
        except Exception:
            pass

        if not dry_run:
            org_task.delete()

    logger.info(
        "================ Remove remaining airbyte connections & destinations (warehouse) ==========="
    )
    delete_airbyte_workspace_v1(org, dry_run=dry_run)

    logger.info("================ Remove dbt workspace and transform tasks ===========")
    delete_dbt_workspace(org, dry_run=dry_run)

    logger.info(
        "================ Remove django warehouse and the credentials in secrets manager ==========="
    )
    for warehouse in OrgWarehouse.objects.filter(org=org):
        logger.info("will delete warehouse credentials")
        if not dry_run:
            secretsmanager.delete_warehouse_credentials(warehouse)
            warehouse.delete()

    logger.info("================ Remove prefect blocks from django and prefect ===========")
    # delete prefect blocks
    for prefect_block in OrgPrefectBlockv1.objects.filter(org=org).all():
        if prefect_block.block_type == SECRET:
            logger.info("will delete secret block in prefect")
            if not dry_run:
                if org and org.dbt:
                    secretsmanager.delete_github_pat(org.dbt.gitrepo_access_token_secret)
                prefect_service.prefect_delete_a_block(prefect_block.block_id)
                prefect_block.delete()
        else:
            logger.info(f"will delete block {prefect_block.block_type} in prefect")
            if not dry_run:
                prefect_service.prefect_delete_a_block(prefect_block.block_id)
                prefect_block.delete()


def delete_orgtasks(org: Org, dry_run: bool = False):
    """deletes org tasks"""
    for org_task in OrgTask.objects.filter(org=org).all():
        logger.info(f"will delete org task {org_task.task.slug} ")
        if not dry_run:
            org_task.delete()


def delete_dbt_workspace(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """deletes the dbt workspace"""
    logger.info("will delete dbt workspace")
    if not dry_run:
        dbt_service.delete_dbt_workspace(org)


def delete_airbyte_workspace_v1(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """
    deletes airbyte sources, destinations and the workspace
    deletes airbyte/django/prefect server blocks
    """

    if is_valid_uuid(org.airbyte_workspace_id):
        for source in airbyte_service.get_sources(org.airbyte_workspace_id)["sources"]:
            logger.info("will delete source in Airbyte " + source["sourceId"])
            if not dry_run:
                airbyte_service.delete_source(org.airbyte_workspace_id, source["sourceId"])
                logger.info(f"deleted source in Airbyte - {source['sourceId']}")

        for destination in airbyte_service.get_destinations(org.airbyte_workspace_id)[
            "destinations"
        ]:
            logger.info("will delete destination in Airbyte " + destination["destinationId"])
            if not dry_run:
                airbyte_service.delete_destination(
                    org.airbyte_workspace_id, destination["destinationId"]
                )
                logger.info(f"deleted destination in Airbyte - {destination['destinationId']}")

        if not dry_run:
            logger.info("will delete airbyte workspace %s", org.airbyte_workspace_id)
            try:
                airbyte_service.delete_workspace(org.airbyte_workspace_id)
                logger.info(f"deleted airbyte workspace - {org.airbyte_workspace_id}")
            except Exception as err:
                logger.error("error deleting airbyte workspace: %s", str(err))
                pass

    for block in OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).all():
        logger.info("will delete airbyte server block in prefect")
        if not dry_run:
            try:
                prefect_service.prefect_delete_a_block(block.block_id)
                block.delete()
            except Exception as err:
                logger.error("error deleting airbyte server block in prefect: %s", str(err))
                pass


def delete_orgusers(org: Org, dry_run: bool = False):  # skipcq: PYL-R0201
    """delete all login users"""
    for orguser in OrgUser.objects.filter(org=org):
        logger.info("will delete orguser %s", orguser.user.email)
        if not dry_run:
            orguser.delete()
            # this deletes the orguser as well via CASCADE


def display_org(org: Org):
    """show org"""
    account_admin = OrgUser.objects.filter(org=org, new_role__slug=ACCOUNT_MANAGER_ROLE).first()
    logger.info(
        "OrgName: %-25s %-25s Airbyte workspace ID: %-40s Admin: %s",
        org.name,
        org.slug,
        org.airbyte_workspace_id,
        account_admin.user.email if account_admin else "<unknonw>",
    )


def delete_one_org(org: Org, dry_run: bool = False):
    """delete one org"""

    # OrgPrefectBlocks <-> Prefect Blocks
    delete_orgprefectblocks(org, dry_run=dry_run)

    # OrgDataFlows <-> Prefect Deployments
    delete_orgdataflows(org, dry_run=dry_run)

    # deletes airbyte connections and destination
    # deletes connection sync and reset tasks
    # delete warehouse secrets
    delete_warehouse_v1(org, dry_run=dry_run)

    # OrgTasks <-> Airbyte Connections, dbt Tasks, shell tasks
    # the airbyte tasks will be gone by now
    delete_orgtasks(org, dry_run=dry_run)

    # the tasks, dataflows and blocks will be gone by now, delete the dbt folder
    delete_dbt_workspace(org, dry_run=dry_run)

    # delete airbyte workspace and the server block
    if org.airbyte_workspace_id:
        delete_airbyte_workspace_v1(org, dry_run=dry_run)

    # delete the org users
    delete_orgusers(org, dry_run=dry_run)

    if not dry_run:
        org.delete()
