"""
functions which work with airbyte and with the dalgo database
"""

import json
import os
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import yaml
from ninja.errors import HttpError
from django.utils.text import slugify
from django.conf import settings
from django.db import transaction
from django.db.models import F, Window, Q
from django.db.models.functions import RowNumber
from django.forms.models import model_to_dict

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteConnectionSchemaUpdate, AirbyteWorkspace
from ddpui.ddpprefect import prefect_service, schema, DBTCORE
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgSchemaChange,
    OrgWarehouseSchema,
    ConnectionJob,
    ConnectionMeta,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import timezone
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
)
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.core.dbtfunctions import map_airbyte_destination_spec_to_dbtcli_profile
from ddpui.models.org import OrgDataFlowv1, OrgWarehouse
from ddpui.models.tasks import (
    Task,
    OrgTask,
    DataflowOrgTask,
)
from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    TASK_AIRBYTERESET,
    UPDATE_SCHEMA,
    TASK_AIRBYTECLEAR,
)
from ddpui.utils.helpers import (
    generate_hash_id,
    update_dict_but_not_stars,
    get_schedule_time_for_large_jobs,
)
from ddpui.utils import secretsmanager
from ddpui.assets.whitelist import DEMO_WHITELIST_SOURCES
from ddpui.core.pipelinefunctions import (
    setup_airbyte_sync_task_config,
    setup_airbyte_update_schema_task_config,
)
from ddpui.core.orgtaskfunctions import fetch_orgtask_lock, fetch_orgtask_lock_v1
from ddpui.models.tasks import TaskLock
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.elementary_service import create_elementary_profile, elementary_setup_status

logger = CustomLogger("airbyte")


def add_custom_airbyte_connector(
    workspace_id: str,
    connector_name: str,
    connector_docker_repository: str,
    connector_docker_image_tag: str,
    connector_documentation_url,
) -> None:
    """creates a custom source definition in airbyte"""
    airbyte_service.create_custom_source_definition(
        workspace_id=workspace_id,
        name=connector_name,
        docker_repository=connector_docker_repository,
        docker_image_tag=connector_docker_image_tag,
        documentation_url=connector_documentation_url,
    )
    logger.info(
        f"added custom source {connector_name} [{connector_docker_repository}:{connector_docker_image_tag}]"
    )


def upgrade_custom_sources(workspace_id: str) -> None:
    """
    compares the versions of the custom sources listed above with those in the airbyte workspace
    and upgrades if necessary
    """
    source_definitions = airbyte_service.get_source_definitions(workspace_id)

    for custom_source, custom_source_info in settings.AIRBYTE_CUSTOM_SOURCES.items():
        found_custom_source = False

        for source_def in source_definitions["sourceDefinitions"]:
            if (
                custom_source == source_def["dockerRepository"]
                and custom_source_info["name"] == source_def["name"]
            ):
                found_custom_source = True
                if source_def["dockerImageTag"] < custom_source_info["docker_image_tag"]:
                    logger.info(
                        f"ready to upgrade {custom_source} from "
                        f"{custom_source_info['docker_image_tag']} to "
                        f"{source_def['dockerImageTag']}"
                    )
                    # instead we should update the existing sourceDef to use the new docker image
                    #  /v1/source_definitions/update
                    add_custom_airbyte_connector(
                        workspace_id,
                        custom_source_info["name"],
                        custom_source_info["docker_repository"],
                        custom_source_info["docker_image_tag"],
                        custom_source_info["documentation_url"],
                    )
                else:
                    logger.info(
                        f"{custom_source} version {custom_source_info['docker_image_tag']} has not changed"
                    )

        if not found_custom_source:
            logger.info(
                f'did not find {custom_source}, adding version {custom_source_info["docker_image_tag"]} now'
            )
            add_custom_airbyte_connector(
                workspace_id,
                custom_source_info["name"],
                custom_source_info["docker_repository"],
                custom_source_info["docker_image_tag"],
                custom_source_info["documentation_url"],
            )


def setup_airbyte_workspace_v1(wsname: str, org: Org) -> AirbyteWorkspace:
    """creates an airbyte workspace and attaches it to the org
    also creates an airbyte server block in prefect if there isn't one already
    we don't need to update any existing server block because it does not hold
    the workspace id... only the connection details of the airbyte server
    """
    workspace = airbyte_service.create_workspace(wsname)

    org.airbyte_workspace_id = workspace["workspaceId"]
    org.save()

    # Airbyte server block details. prefect doesn't know the workspace id
    block_name = f"{org.slug}-{slugify(AIRBYTESERVER)}"

    airbyte_server_block_cleaned_name = block_name
    try:
        airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(block_name)
    except Exception as exc:
        raise Exception("could not connect to prefect-proxy") from exc

    if airbyte_server_block_id is None:
        (
            airbyte_server_block_id,
            airbyte_server_block_cleaned_name,
        ) = prefect_service.create_airbyte_server_block(block_name)
        logger.info(f"Created Airbyte server block with ID {airbyte_server_block_id}")

    if not OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).exists():
        org_airbyte_server_block = OrgPrefectBlockv1(
            org=org,
            block_type=AIRBYTESERVER,
            block_id=airbyte_server_block_id,
            block_name=airbyte_server_block_cleaned_name,
        )
        try:
            org_airbyte_server_block.save()
        except Exception as error:
            prefect_service.delete_airbyte_server_block(airbyte_server_block_id)
            raise Exception("could not create orgprefectblock for airbyte-server") from error

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


def create_airbyte_deployment(org: Org, org_task: OrgTask, server_block: OrgPrefectBlockv1):
    """Creates OrgDataFlowv1 & the prefect deployment based on the airbyte OrgTask"""
    hash_code = generate_hash_id(8)
    logger.info(f"using the hash code {hash_code} for the deployment name")

    deployment_name = f"manual-{org.slug}-{org_task.task.slug}-{hash_code}"
    dataflow = prefect_service.create_dataflow_v1(
        PrefectDataFlowCreateSchema3(
            deployment_name=deployment_name,
            flow_name=deployment_name,
            orgslug=org.slug,
            deployment_params={
                "config": {
                    "org_slug": org.slug,
                    "tasks": [setup_airbyte_sync_task_config(org_task, server_block).to_json()],
                }
            },
        )
    )

    existing_dataflow = OrgDataFlowv1.objects.filter(
        deployment_id=dataflow["deployment"]["id"]
    ).first()
    if existing_dataflow:
        existing_dataflow.delete()

    org_dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name=deployment_name,
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        dataflow_type="manual",
    )
    DataflowOrgTask.objects.create(
        dataflow=org_dataflow,
        orgtask=org_task,
    )

    return org_dataflow


def create_connection(org: Org, payload: AirbyteConnectionCreate):
    """creates an airbyte connection and tracking objects in the database"""
    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"
    if warehouse.airbyte_destination_id is None:
        return None, "warehouse has no airbyte_destination_id"
    payload.destinationId = warehouse.airbyte_destination_id

    org_airbyte_server_block = OrgPrefectBlockv1.objects.filter(
        org=org,
        block_type=AIRBYTESERVER,
    ).first()
    if org_airbyte_server_block is None:
        raise Exception(f"{org.slug} has no {AIRBYTESERVER} block in OrgPrefectBlock")

    sync_task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
    if sync_task is None:
        return None, "sync task not supported"

    clear_task = Task.objects.filter(slug=TASK_AIRBYTECLEAR).first()
    if clear_task is None:
        return None, "clear task not supported"

    airbyte_conn = airbyte_service.create_connection(org.airbyte_workspace_id, payload)

    try:
        with transaction.atomic():
            # create a sync deployment & dataflow
            org_task = OrgTask.objects.create(
                org=org, task=sync_task, connection_id=airbyte_conn["connectionId"]
            )

            sync_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, org_task, org_airbyte_server_block
            )

            # create clear connection task & dataflow/deployment
            org_task = OrgTask.objects.create(
                org=org, task=clear_task, connection_id=airbyte_conn["connectionId"]
            )

            clear_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, org_task, org_airbyte_server_block
            )

            sync_dataflow.clear_conn_dataflow = clear_dataflow
            sync_dataflow.save()

    except Exception as err:
        # delete the airbyte connection; since the deployment didn't get created
        logger.info("deleting airbyte connection")
        airbyte_service.delete_connection(org.airbyte_workspace_id, airbyte_conn["connectionId"])
        logger.error(err)
        return None, "Couldn't create the connection, please try again"

    res = {
        "name": payload.name,
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"]},
        "destination": {
            "id": airbyte_conn["destinationId"],
        },
        "catalogId": airbyte_conn["sourceCatalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "status": airbyte_conn["status"],
        "deploymentId": sync_dataflow.deployment_id,
        "resetConnDeploymentId": None,
        "clearConnDeploymentId": clear_dataflow.deployment_id,
    }
    return res, None


def get_connections(org: Org):
    """return connections with last run details"""

    sync_dataflows = (
        OrgDataFlowv1.objects.filter(
            org=org,
            dataflow_type="manual",
        )
        .filter(Q(reset_conn_dataflow_id__isnull=False) | Q(clear_conn_dataflow_id__isnull=False))
        .select_related("clear_conn_dataflow")
    )

    dataflow_ids = sync_dataflows.values_list("id", flat=True)
    clear_dataflow_ids = sync_dataflows.values_list("clear_conn_dataflow_id", flat=True)
    all_dataflow_orgtasks = DataflowOrgTask.objects.filter(
        dataflow_id__in=[
            pk_id for pk_id in list(dataflow_ids) + list(clear_dataflow_ids) if pk_id is not None
        ]
    ).select_related("orgtask")

    org_task_ids = all_dataflow_orgtasks.values_list("orgtask_id", flat=True)
    all_org_task_locks = TaskLock.objects.filter(orgtask_id__in=org_task_ids)

    # for the above of orgtask_ids, we fetch all the dataflows of each orgtasks
    # this will be used to find the last run of each dataflow
    all_orgtask_dataflows = DataflowOrgTask.objects.filter(
        orgtask_id__in=org_task_ids
    ).select_related("dataflow")

    warehouse = OrgWarehouse.objects.filter(org=org).first()

    airbyte_connections = airbyte_service.get_webbackend_connections(org.airbyte_workspace_id)

    res = []

    for sync_dataflow in sync_dataflows:
        sync_dataflow_orgtasks = [
            dfot for dfot in all_dataflow_orgtasks if dfot.dataflow_id == sync_dataflow.id
        ]

        clear_dataflow_orgtasks = [
            dfot
            for dfot in all_dataflow_orgtasks
            if dfot.dataflow_id
            == (sync_dataflow.clear_conn_dataflow.id if sync_dataflow.clear_conn_dataflow else -1)
        ]

        org_tasks: list[OrgTask] = [
            dataflow_orgtask.orgtask for dataflow_orgtask in sync_dataflow_orgtasks
        ]

        if len(org_tasks) == 0:
            logger.error(
                f"Something wrong with the sync dataflow {sync_dataflow.deployment_name}; no orgtasks found"
            )
            continue

        if len(org_tasks) > 1:
            logger.error(
                f"Something wrong with the sync dataflow {sync_dataflow.deployment_name}; more than 1 orgtasks attached"
            )
            continue

        org_task: OrgTask = org_tasks[0]

        clear_org_task: OrgTask = None
        if len(clear_dataflow_orgtasks) > 0:
            rst_orgtasks = [
                dfot.orgtask
                for dfot in clear_dataflow_orgtasks
                if dfot.dataflow_id
                == (
                    sync_dataflow.clear_conn_dataflow.id
                    if sync_dataflow.clear_conn_dataflow
                    else -1
                )
            ]
            if len(rst_orgtasks) > 0:
                clear_org_task = rst_orgtasks[0]

        # find the airbyte connection
        connection = [
            conn for conn in airbyte_connections if conn["connectionId"] == org_task.connection_id
        ]
        if len(connection) == 0:
            logger.error(f"could not find connection {org_task.connection_id} in airbyte")
            continue
        connection = connection[0]

        look_up_last_run_deployment_ids = []

        for df_orgtask in [
            df_orgtask
            for df_orgtask in all_orgtask_dataflows
            if df_orgtask.orgtask_id == org_task.id
        ]:
            look_up_last_run_deployment_ids.append(df_orgtask.dataflow.deployment_id)

        clear_dataflow: OrgDataFlowv1 = sync_dataflow.clear_conn_dataflow

        lock = None

        sync_lock = None
        for lock in [
            org_task_locks
            for org_task_locks in all_org_task_locks
            if org_task_locks.orgtask_id == org_task.id
        ]:
            sync_lock = lock
            break

        if sync_lock:
            lock = fetch_orgtask_lock_v1(org_task, sync_lock)

        if not lock and clear_org_task:
            for lock in [
                org_task_locks
                for org_task_locks in all_org_task_locks
                if org_task_locks.orgtask_id == clear_org_task.id
            ]:
                lock = fetch_orgtask_lock_v1(clear_org_task, lock)
                break

        connection["destination"]["name"] = warehouse.name
        res.append(
            {
                "name": connection["name"],
                "connectionId": connection["connectionId"],
                "source": connection["source"],
                "destination": connection["destination"],
                "status": connection["status"],
                "deploymentId": sync_dataflow.deployment_id,
                "lastRun": None,  # updated below
                "lock": lock,  # this will have the status of the flow run
                "resetConnDeploymentId": None,
                "clearConnDeploymentId": clear_dataflow.deployment_id if clear_dataflow else None,
                "look_up_last_run_deployment_ids": look_up_last_run_deployment_ids,
            }
        )

    # fetch all last runs in one go
    all_last_run_deployment_ids = []
    for conn in res:
        all_last_run_deployment_ids.extend(conn["look_up_last_run_deployment_ids"])

    flow_runs_with_row_number = PrefectFlowRun.objects.filter(
        deployment_id__in=all_last_run_deployment_ids
    ).annotate(
        row_number=Window(
            expression=RowNumber(),
            partition_by=[F("deployment_id")],
            order_by=F("start_time").desc(),
        )
    )
    last_flow_run_per_deployment = flow_runs_with_row_number.filter(row_number=1)

    # attach last run for each conn based on latest(look_up_last_run_deployment_ids)
    for conn in res:
        last_runs = []
        for run in last_flow_run_per_deployment:
            if run.deployment_id in conn["look_up_last_run_deployment_ids"]:
                last_runs.append(run.to_json())

        last_runs.sort(
            key=lambda run: (run["startTime"] if run["startTime"] else run["expectedStartTime"])
        )

        conn["lastRun"] = last_runs[-1] if len(last_runs) > 0 else None
        del conn["look_up_last_run_deployment_ids"]

    return res, None


def get_one_connection(org: Org, connection_id: str):
    """retrieve details of a single connection"""
    org_task = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()

    if org_task is None or org_task.connection_id is None:
        return None, "connection not found"

    # fetch airbyte connection
    airbyte_conn = airbyte_service.get_connection(org.airbyte_workspace_id, org_task.connection_id)

    dataflow_orgtask = DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ).first()

    if dataflow_orgtask is None:
        return None, "deployment not found"

    reset_dataflow: OrgDataFlowv1 = dataflow_orgtask.dataflow.reset_conn_dataflow

    # fetch the source and destination names
    # the web_backend/connections/get fetches the source & destination objects also so we dont need to query again
    source_name = airbyte_conn["source"]["name"]

    destination_name = airbyte_conn["destination"]["name"]

    lock = fetch_orgtask_lock(org_task)

    if not lock and reset_dataflow:
        reset_dataflow_orgtask = DataflowOrgTask.objects.filter(dataflow=reset_dataflow).first()

        if reset_dataflow_orgtask.orgtask:
            lock = fetch_orgtask_lock(reset_dataflow_orgtask.orgtask)

    res = {
        "name": airbyte_conn["name"],
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"], "name": source_name},
        "destination": {"id": airbyte_conn["destinationId"], "name": destination_name},
        "catalogId": airbyte_conn["catalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "destinationSchema": (
            airbyte_conn["namespaceFormat"]
            if airbyte_conn["namespaceDefinition"] == "customformat"
            else ""
        ),
        "status": airbyte_conn["status"],
        "deploymentId": (
            dataflow_orgtask.dataflow.deployment_id if dataflow_orgtask.dataflow else None
        ),
        "lock": lock,
        "resetConnDeploymentId": (reset_dataflow.deployment_id if reset_dataflow else None),
    }

    return res, None


def reset_connection(org: Org, connection_id: str):
    """
    reset the connection via the sync deployment
    this will run a full reset + sync
    """
    org_server_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).first()

    if not org_server_block:
        logger.error("Airbyte server block not found")
        return None, "airbyte server block not found"

    sync_org_task = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC,
    ).first()
    if not sync_org_task:
        return None, "connection not found"

    sync_dataflow_orgtask = DataflowOrgTask.objects.filter(
        orgtask=sync_org_task, dataflow__dataflow_type="manual"
    ).first()

    if sync_dataflow_orgtask is None:
        logger.error("Sync dataflow not found")
        return None, "sync dataflow not found"

    reset_org_task = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTERESET
    ).first()

    if not reset_org_task:
        logger.error("Reset OrgTask not found")
        return None, "reset OrgTask not found"

    # full reset + sync; run via the manual sync deployment
    params = {
        "config": {
            "tasks": [
                setup_airbyte_sync_task_config(reset_org_task, org_server_block, seq=1).to_json(),
                setup_airbyte_sync_task_config(sync_org_task, org_server_block, seq=2).to_json(),
            ],
            "org_slug": org.slug,
        }
    }

    # check if the connection is "large" for scheduling
    connection_meta = ConnectionMeta.objects.filter(connection_id=connection_id).first()
    is_connection_large_enough = (
        True == connection_meta.schedule_large_jobs if connection_meta else False
    )

    schedule_at = None
    job: ConnectionJob = None
    if is_connection_large_enough:
        schedule_at = get_schedule_time_for_large_jobs()
    # if there is a flow run scheduled , delete it
    # if the connection is large enough, we will schedule a new flow run
    # if the connection is not larged enough, we run it now
    # either way we need to delete this job
    job = ConnectionJob.objects.filter(
        connection_id=connection_id, job_type=TASK_AIRBYTERESET
    ).first()
    if job:
        try:
            prefect_service.delete_flow_run(job.flow_run_id)
            job.delete()
            job = None
        except Exception as err:
            logger.exception(err)
            raise HttpError(400, "failed to remove the previous flow run") from err

    try:
        res = prefect_service.schedule_deployment_flow_run(
            sync_dataflow_orgtask.dataflow.deployment_id,
            params,
            scheduled_time=schedule_at,
        )
        # save the new flow run scheduled to our db
        if is_connection_large_enough:
            if not job:
                job = ConnectionJob.objects.create(
                    connection_id=connection_id,
                    job_type=TASK_AIRBYTERESET,
                    flow_run_id=res["flow_run_id"],
                    scheduled_at=schedule_at,
                )
            else:
                job.flow_run_id = res["flow_run_id"]
                job.scheduled_at = schedule_at
                job.save()

        logger.info("Successfully triggered Prefect flow run for reset")
    except Exception as error:
        logger.error("Failed to trigger Prefect flow run for reset: %s", error)
        return None, "failed to trigger Prefect flow run for reset"

    return None, None


def update_connection(org: Org, connection_id: str, payload: AirbyteConnectionUpdate):
    """updates an airbyte connection"""

    if not OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ).exists():
        return None, "connection not found"

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"
    if warehouse.airbyte_destination_id is None:
        return None, "warehouse has no airbyte_destination_id"

    payload.destinationId = warehouse.airbyte_destination_id

    if len(payload.streams) == 0:
        return None, "must specify stream names"

    # fetch connection by id from airbyte
    connection = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)
    connection["operationIds"] = []

    # update name
    if payload.name:
        connection["name"] = payload.name

    # always reset the connection
    connection["skipReset"] = False

    # update the airbyte connection
    res = airbyte_service.update_connection(org.airbyte_workspace_id, payload, connection)

    return res, None


def delete_connection(org: Org, connection_id: str):
    """deletes an airbyte connection"""

    dataflows_to_delete: list[OrgDataFlowv1] = []
    orgtask_to_delete: list[OrgTask] = []

    # delete manual-sync and manual-reset dataflows
    for org_task in OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ):
        for dataflow_orgtask in DataflowOrgTask.objects.filter(
            orgtask=org_task, dataflow__dataflow_type="manual"
        ):
            dataflows_to_delete.append(dataflow_orgtask.dataflow)
            logger.info("will delete %s", dataflow_orgtask.dataflow.deployment_name)

        orgtask_to_delete.append(org_task)

    # delete all deployments
    for dataflow in dataflows_to_delete:
        logger.info("deleting prefect deployment %s", dataflow.deployment_name)
        prefect_service.delete_deployment_by_id(dataflow.deployment_id)

    # delete all dataflows
    logger.info("deleting org dataflows from db")
    for dataflow in dataflows_to_delete:
        # if there is a reset and a sync then the dataflow will appear twice in this list
        if dataflow.id:
            dataflow.delete()

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
            for task in parameters["config"]["tasks"]:
                if task.get("connection_id") == connection_id:
                    logger.info(f"deleting task {task['slug']} from deployment")
                    parameters["config"]["tasks"].remove(task)
            # logger.info(parameters)
            payload = PrefectDataFlowUpdateSchema3(deployment_params=parameters)
            prefect_service.update_dataflow_v1(dataflow_orgtask.dataflow.deployment_id, payload)
            logger.info("updated deployment %s", dataflow_orgtask.dataflow.deployment_name)

    # delete all orgtasks
    for org_task in orgtask_to_delete:
        logger.info("deleting orgtask %s", org_task.task.slug)
        org_task.delete()

    # delete airbyte connection
    logger.info("deleting airbyte connection %s", connection_id)
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    if OrgSchemaChange.objects.filter(connection_id=connection_id).exists():
        OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
        logger.info(f"Deleted schema changes for connection {connection_id}")

    return None, None


def get_job_info_for_connection(org: Org, connection_id: str):
    """get the info for the latest airbyte job for a connection"""
    org_task = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC,
    ).first()

    if org_task is None:
        return None, "connection not found"

    result = airbyte_service.get_jobs_for_connection(connection_id)
    if len(result["jobs"]) == 0:
        return {
            "status": "not found",
        }, None

    latest_job = result["jobs"][0]
    job_info = airbyte_service.parse_job_info(latest_job)
    logs = airbyte_service.get_logs_for_job(job_info["job_id"])
    job_info["logs"] = logs["logs"]["logLines"]

    return job_info, None


def get_sync_job_history_for_connection(
    org: Org, connection_id: str, limit: int = 10, offset: int = 0
):
    """
    Get all sync jobs (paginated) for a connection
    Returns
    - Date
    - Records synced
    - Bytes synced
    - Duration
    - logs

    In case there no sync jobs, return an empty list
    """

    org_task = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC,
    ).first()

    if org_task is None:
        return None, "connection not found"

    res = {"history": [], "totalSyncs": 0}
    result = airbyte_service.get_jobs_for_connection(
        connection_id, limit, offset, job_types=["sync", "reset_connection"]
    )
    res["totalSyncs"] = result["totalJobCount"]
    if len(result["jobs"]) == 0:
        return [], None

    for job in result["jobs"]:
        job_info = airbyte_service.parse_job_info(job)

        res["history"].append(job_info)

    return res, None


def update_destination(org: Org, destination_id: str, payload: AirbyteDestinationUpdate):
    """updates an airbyte destination and dbt cli profile if credentials changed"""
    destination = airbyte_service.update_destination(
        destination_id, payload.name, payload.config, payload.destinationDefId
    )
    logger.info("updated destination having id " + destination["destinationId"])
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if warehouse.name != payload.name:
        warehouse.name = payload.name
        warehouse.save()

    dbt_credentials = {}
    if warehouse.wtype == "postgres":
        dbt_credentials = update_dict_but_not_stars(payload.config)
        # i've forgotten why we have this here, airbyte sends "database" - RC
        if "dbname" in dbt_credentials:
            dbt_credentials["database"] = dbt_credentials["dbname"]

    elif warehouse.wtype == "bigquery":
        dbt_credentials = json.loads(payload.config["credentials_json"])
    elif warehouse.wtype == "snowflake":
        dbt_credentials = update_dict_but_not_stars(payload.config)

    else:
        return None, "unknown warehouse type " + warehouse.wtype

    curr_credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    # copy over the value of keys that are missing from in dbt_credentials (these are probably that have all '*****')
    for key, value in curr_credentials.items():
        if key not in dbt_credentials:
            dbt_credentials[key] = value

    secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

    (cli_profile_block, dbt_project_params), error = create_or_update_org_cli_block(
        org, warehouse, dbt_credentials
    )
    if error:
        return None, error

    # if elementary is set up for this client, we need to update the elemntary_profiles/profiles.yml
    if elementary_setup_status(org) == "set-up":
        # get prefect-dbt to create a new profiles.yml by running "dbt debug"
        now = timezone.as_ist(datetime.now())
        dbtdebugtask = schema.PrefectDbtTaskSetup(
            seq=1,
            slug="dbt-debug",
            commands=[f"{dbt_project_params.dbt_binary} debug"],
            type=DBTCORE,
            env={},
            working_dir=dbt_project_params.project_dir,
            profiles_dir=f"{dbt_project_params.project_dir}/profiles/",
            project_dir=dbt_project_params.project_dir,
            cli_profile_block=cli_profile_block.block_name,
            cli_args=[],
            orgtask_uuid=str(uuid4()),
            flow_name=f"{org.slug}-dbt-debug",
            flow_run_name=f"{now.isoformat()}",
        )

        logger.info("running dbt debug to generate new profiles/profiles.yml")
        prefect_service.run_dbt_task_sync(dbtdebugtask)

        logger.info("recreating elementary_profiles/profiles.yml")
        create_elementary_profile(org)

    return destination, None


def create_warehouse(org: Org, payload: OrgWarehouseSchema):
    """creates a warehouse for an org"""

    if payload.wtype not in ["postgres", "bigquery", "snowflake"]:
        return None, "unrecognized warehouse type " + payload.wtype

    destination = airbyte_service.create_destination(
        org.airbyte_workspace_id,
        f"{payload.wtype}-warehouse",
        payload.destinationDefId,
        payload.airbyteConfig,
    )
    logger.info("created destination having id " + destination["destinationId"])

    # prepare the dbt credentials from airbyteConfig
    dbt_credentials = None
    if payload.wtype == "postgres":
        # host, database, port, username, password
        # jdbc_url_params
        # ssl: true | false
        # ssl_mode:
        #   mode: disable | allow | prefer | require | verify-ca | verify-full
        #   ca_certificate: string if mode is require, verify-ca, or verify-full
        #   client_key_password: string if mode is verify-full
        # tunnel_method:
        #   tunnel_method = NO_TUNNEL | SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_host: string if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_port: int if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   tunnel_user: string if SSH_KEY_AUTH | SSH_PASSWORD_AUTH
        #   ssh_key: string if SSH_KEY_AUTH
        #   tunnel_user_password: string if SSH_PASSWORD_AUTH
        dbt_credentials = payload.airbyteConfig
    elif payload.wtype == "bigquery":
        credentials_json = json.loads(payload.airbyteConfig["credentials_json"])
        dbt_credentials = credentials_json
    elif payload.wtype == "snowflake":
        dbt_credentials = payload.airbyteConfig

    destination_definition = airbyte_service.get_destination_definition(
        org.airbyte_workspace_id, payload.destinationDefId
    )

    warehouse = OrgWarehouse(
        org=org,
        name=payload.name,
        wtype=payload.wtype,
        credentials="",
        airbyte_destination_id=destination["destinationId"],
        airbyte_docker_repository=destination_definition["dockerRepository"],
        airbyte_docker_image_tag=destination_definition["dockerImageTag"],
    )
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(warehouse, dbt_credentials)
    warehouse.credentials = credentials_lookupkey
    if "dataset_location" in destination["connectionConfiguration"]:
        warehouse.bq_location = destination["connectionConfiguration"]["dataset_location"]
    warehouse.save()

    create_or_update_org_cli_block(org, warehouse, payload.airbyteConfig)

    return None, None


def create_or_update_org_cli_block(org: Org, warehouse: OrgWarehouse, airbyte_creds: dict):
    """
    Create/update the block in db and also in prefect
    """

    bqlocation = None
    if warehouse.wtype == "bigquery":
        bqlocation = (
            airbyte_creds["dataset_location"] if "dataset_location" in airbyte_creds else None
        )
    profile_name = None
    target = None
    dbt_project_params: DbtProjectParams = None
    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

        dbt_project_filename = str(Path(dbt_project_params.project_dir) / "dbt_project.yml")
        if not os.path.exists(dbt_project_filename):
            raise HttpError(400, dbt_project_filename + " is missing")

        with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
            dbt_project = yaml.safe_load(dbt_project_file)
            if "profile" not in dbt_project:
                raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

        profile_name = dbt_project["profile"]
        target = dbt_project_params.target
    except Exception as err:
        logger.error(
            "Failed to fetch the dbt profile - looks like transformation has not been setup. Using 'default' as profile name and continuing"
        )
        logger.error(err)

    dbt_creds = map_airbyte_destination_spec_to_dbtcli_profile(airbyte_creds, dbt_project_params)

    dbt_creds.pop("ssl_mode", None)
    dbt_creds.pop("ssl", None)

    # set defaults to target and profile
    # cant create a cli profile without these two
    # idea is these should be updated when we setup transformation or update the warehouse
    if not profile_name:
        profile_name = "default"

    if not target:
        target = "default"

    logger.info("Found org=%s profile_name=%s target=%s", org.slug, profile_name, target)
    cli_profile_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=DBTCLIPROFILE).first()
    if cli_profile_block:
        logger.info(
            f"Updating the cli profile block : {cli_profile_block.block_name} for org={org.slug} with profile={profile_name} target={target}"
        )
        try:
            prefect_service.update_dbt_cli_profile_block(
                block_name=cli_profile_block.block_name,
                wtype=warehouse.wtype,
                credentials=dbt_creds,
                bqlocation=bqlocation,
                profilename=profile_name,
                target=target,
            )
        except Exception as error:
            logger.error(
                "Failed to update the cli profile block %s , err=%s",
                cli_profile_block.block_name,
                str(error),
            )
            return (None, None), "Failed to update the cli profile block"
        logger.info(f"Successfully updated the cli profile block : {cli_profile_block.block_name}")
    else:
        logger.info(
            "Creating a new cli profile block for %s with profile=%s & target=%s ",
            org.slug,
            profile_name,
            target,
        )
        new_block_name = f"{org.slug}-{profile_name}"

        try:
            cli_block_response = prefect_service.create_dbt_cli_profile_block(
                block_name=new_block_name,
                profilename=profile_name,
                target=target,
                wtype=warehouse.wtype,
                bqlocation=bqlocation,
                credentials=dbt_creds,
            )
        except Exception as error:
            logger.error(
                "Failed to create a new cli profile block %s , err=%s",
                new_block_name,
                str(error),
            )
            return (None, None), "Failed to update the cli profile block"

        # save the cli profile block in django db
        cli_profile_block = OrgPrefectBlockv1.objects.create(
            org=org,
            block_type=DBTCLIPROFILE,
            block_id=cli_block_response["block_id"],
            block_name=cli_block_response["block_name"],
        )

    return (cli_profile_block, dbt_project_params), None


def get_warehouses(org: Org):
    """return list of warehouses for an Org"""
    warehouses = [
        {
            "wtype": warehouse.wtype,
            # "credentials": warehouse.credentials,
            "name": warehouse.name,
            "airbyte_destination": airbyte_service.get_destination(
                org.airbyte_workspace_id, warehouse.airbyte_destination_id
            ),
            "airbyte_docker_repository": warehouse.airbyte_docker_repository,
            "airbyte_docker_image_tag": warehouse.airbyte_docker_image_tag,
        }
        for warehouse in OrgWarehouse.objects.filter(org=org)
    ]
    return warehouses, None


def get_demo_whitelisted_source_config(type_: str):
    """Returns the config of whitelisted source based on type"""
    ret_src = None
    for src in DEMO_WHITELIST_SOURCES:
        if src["type"] == type_:
            ret_src = src
            break

    if not ret_src:
        return ret_src, "source not found"

    return ret_src["config"], None


def delete_source(org: Org, source_id: str):
    """deletes an airbyte source and related connections"""
    connections = airbyte_service.get_connections(org.airbyte_workspace_id)["connections"]
    connections_of_source = [
        conn["connectionId"] for conn in connections if conn["sourceId"] == source_id
    ]

    # Fetch org tasks and deployments mapped to each connection_id
    org_tasks = OrgTask.objects.filter(org=org, connection_id__in=connections_of_source).all()

    # Fetch dataflows(manual or pipelines)
    df_orgtasks = DataflowOrgTask.objects.filter(orgtask__in=org_tasks).all()
    dataflows = [df_orgtask.dataflow for df_orgtask in df_orgtasks]

    # Delete the deployments
    logger.info("Deleting the deployemtns from prefect and django")
    for dataflow in dataflows:
        try:
            # delete from prefect
            prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            logger.info(f"delete deployment: {dataflow.deployment_id} from prefect")

            # delete from django
            dataflow.delete()
        except Exception as error:
            logger.info(
                f"something went wrong deleting the prefect deplyment {dataflow.deployment_id}"
            )
            logger.exception(error)
            continue

    # Delete the orgtasks
    logger.info("Deleting org tasks related to connections in this source")
    for org_task in org_tasks:
        org_task.delete()

    # delete the source from airbyte
    airbyte_service.delete_source(org.airbyte_workspace_id, source_id)
    logger.info(f"deleted airbyte source {source_id}")

    return None, None


def fetch_and_update_org_schema_changes(org: Org, connection_id: str):
    """
    Fetches the schema change catalog from airbyte and updates OrgSchemaChnage in our db
    """
    try:
        logger.info(f"Fetching schema change (catalog) for connection {org.slug}|{connection_id}")
        connection_catalog = airbyte_service.get_connection_catalog(connection_id, timemout=60)
    except Exception as err:
        return (
            None,
            f"Something went wrong fetching schema change (catalog) for connection {connection_id}: {err}",
        )

    # update schema change type in our db
    # delete the schema change if type is "no_change" & if type is "non_breaking" but catalogDiff is empty list
    try:
        change_type = connection_catalog.get("schemaChange")
        logger.info(f"Schema change detected for org {org.slug}: {change_type}")

        if change_type not in ["breaking", "non_breaking", "no_change"]:
            raise ValueError("Invalid schema change type")

        catalog_diff: dict = connection_catalog.get("catalogDiff")

        if change_type == "breaking" or (
            change_type == "non_breaking"
            and catalog_diff
            and len(catalog_diff.get("transforms", [])) > 0
        ):
            OrgSchemaChange.objects.update_or_create(
                connection_id=connection_id,
                defaults={"change_type": change_type, "org": org},
            )
        else:
            schema_change = OrgSchemaChange.objects.filter(connection_id=connection_id).first()
            # see if any jobs are scheduled for the schema change; delete them
            if schema_change:
                if schema_change.schedule_job:
                    job = schema_change.schedule_job
                    try:
                        if job.flow_run_id:
                            prefect_service.delete_flow_run(job.flow_run_id)
                        job.delete()
                        schema_change.delete()
                    except Exception as err:
                        logger.exception("Failed to delete the large schema change job - %s", err)
                else:
                    schema_change.delete()

    except Exception as err:
        return (
            None,
            f"Something went wrong updating OrgSchemaChange {connection_id}: {err}",
        )

    return connection_catalog, None


def update_connection_schema(org: Org, connection_id: str, payload: AirbyteConnectionSchemaUpdate):
    """
    Update the schema changes of a connection.
    """
    if not OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ).exists():
        return None, "connection not found"

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"

    connection = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)

    connection["skipReset"] = True

    res = airbyte_service.update_schema_change(org, payload, connection)
    if res:
        OrgSchemaChange.objects.filter(connection_id=connection_id).delete()

    return res, None


def get_schema_changes(org: Org):
    """
    Get the schema changes of a connection in an org.
    """
    org_schema_change = OrgSchemaChange.objects.filter(org=org).select_related("schedule_job").all()

    if org_schema_change is None:
        return None, "No schema change found"

    large_connections = (
        ConnectionMeta.objects.filter(
            connection_id__in=[change.connection_id for change in org_schema_change],
            schedule_large_jobs=True,
        )
        .all()
        .values_list("connection_id", flat=True)
    )

    schema_changes = []
    for change in org_schema_change:
        schema_changes.append(
            {
                **model_to_dict(change, exclude=["org", "id"]),
                **{
                    "schedule_job": (
                        {
                            "scheduled_at": change.schedule_job.scheduled_at,
                            "flow_run_id": change.schedule_job.flow_run_id,
                            "job_type": change.schedule_job.job_type,
                            "status": None,
                            "state_name": None,
                        }
                        if change.schedule_job
                        else None
                    ),
                    "is_connection_large": change.connection_id in large_connections,
                    "next_job_at": get_schedule_time_for_large_jobs(),
                    "run": None,
                },
            }
        )

    # check if the flow runs have been executed or not
    # if the flow run have been executed attach the run object and remove the schedule_job reference
    logger.info(schema_changes)
    all_flow_run_ids = [
        change["schedule_job"]["flow_run_id"]
        for change in schema_changes
        if change["schedule_job"] and "flow_run_id" in change["schedule_job"]
    ]

    runs = PrefectFlowRun.objects.filter(flow_run_id__in=all_flow_run_ids).all()

    for change in schema_changes:
        if change["schedule_job"] and "flow_run_id" in change["schedule_job"]:
            curr_run: list[PrefectFlowRun] = [
                run for run in runs if run.flow_run_id == change["schedule_job"]["flow_run_id"]
            ]
            change["schedule_job"]["run"] = curr_run[0].to_json() if len(curr_run) >= 1 else None

    return schema_changes, None


def schedule_update_connection_schema(
    orguser: OrgUser,
    connection_id: str,
    payload: AirbyteConnectionSchemaUpdateSchedule,
):
    """Submits a flow run that will execute the schema update flow"""
    server_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org,
        block_type=AIRBYTESERVER,
    ).first()
    if server_block is None:
        raise HttpError(400, "airbyte server block not found")

    org_task = OrgTask.objects.filter(
        org=orguser.org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC,
    ).first()
    if not org_task:
        raise HttpError(400, "Orgtask not found")

    dataflow_orgtask = DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ).first()

    if not dataflow_orgtask:
        raise HttpError(400, "no dataflow mapped")

    # check if the connection is "large" for scheduling
    connection_meta = ConnectionMeta.objects.filter(connection_id=connection_id).first()
    is_connection_large_enough = connection_meta and connection_meta.schedule_large_jobs

    logger.info("connection is large enough: %s", is_connection_large_enough)

    locks: list[TaskLock] = []
    schedule_at = None
    job: ConnectionJob = None
    if not is_connection_large_enough:
        locks = prefect_service.lock_tasks_for_deployment(
            dataflow_orgtask.dataflow.deployment_id,
            orguser,
            dataflow_orgtasks=[dataflow_orgtask],
        )
    else:
        schedule_at = get_schedule_time_for_large_jobs()

    # if there is a flow run scheduled , delete it
    # if the connection is large enough, we will schedule a new flow run
    # if the connection is not larged enough, we run it now
    # either way we need to delete this job
    job = ConnectionJob.objects.filter(connection_id=connection_id, job_type=UPDATE_SCHEMA).first()
    if job:
        try:
            prefect_service.delete_flow_run(job.flow_run_id)
            job.delete()
            job = None
        except Exception as err:
            logger.exception(err)
            raise HttpError(400, "failed to remove the previous flow run") from err

    logger.info("schema change is being scheduled at %s", schedule_at)

    # create the new flow run; schedule now or schedule later for large connections
    try:
        res = prefect_service.schedule_deployment_flow_run(
            dataflow_orgtask.dataflow.deployment_id,
            {
                "config": {
                    "org_slug": orguser.org.slug,
                    "tasks": [
                        setup_airbyte_update_schema_task_config(
                            org_task, server_block, payload.catalogDiff
                        ).to_json()
                    ],
                }
            },
            schedule_at,
        )

        # save the new flow run scheduled to our db
        if is_connection_large_enough:
            if not job:
                job = ConnectionJob.objects.create(
                    connection_id=connection_id,
                    job_type=UPDATE_SCHEMA,
                    flow_run_id=res["flow_run_id"],
                    scheduled_at=schedule_at,
                )
            else:
                job.flow_run_id = res["flow_run_id"]
                job.scheduled_at = schedule_at
                job.save()

            # update the schema change with the scheduled job
            schema_change = OrgSchemaChange.objects.filter(
                connection_id=connection_id, org=orguser.org
            ).first()
            if schema_change:
                schema_change.schedule_job = job
                schema_change.save()

        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()

    except Exception as error:
        for task_lock in locks:
            logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
            task_lock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to start the schema update flow run") from error

    return res
