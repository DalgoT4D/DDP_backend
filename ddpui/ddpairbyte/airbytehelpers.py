"""
functions which work with airbyte and with the dalgo database
"""

import json
import os
from typing import List, Dict, Optional, Tuple, Any, cast
from uuid import uuid4
from pathlib import Path
from datetime import datetime
import yaml
from ninja.errors import HttpError
from django.utils.text import slugify
from django.conf import settings
from django.db import transaction
from django.db.models import F, Window, Q, QuerySet
from django.db.models.functions import RowNumber
from django.forms.models import model_to_dict

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionSchemaUpdate,
    AirbyteWorkspace,
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
    AirbyteGetConnectionsResponse,
)
from ddpui.ddpprefect import prefect_service, schema, DBTCORE
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgSchemaChange,
    OrgWarehouseSchema,
    ConnectionJob,
    ConnectionMeta,
    OrgWarehouse,
    OrgDataFlowv1,
    ConnectionMeta,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import timezone
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.core.dbtfunctions import map_airbyte_destination_spec_to_dbtcli_profile
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask, TaskLockStatus, TaskLock
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
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpdbt.elementary_service import create_elementary_profile, elementary_setup_status

logger: CustomLogger = CustomLogger("airbyte")


def add_custom_airbyte_connector(
    workspace_id: str,
    connector_name: str,
    connector_docker_repository: str,
    connector_docker_image_tag: str,
    connector_documentation_url: str,
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
    source_definitions: Dict[str, Any] = airbyte_service.get_source_definitions(workspace_id)

    for custom_source, custom_source_info in settings.AIRBYTE_CUSTOM_SOURCES.items():
        found_custom_source: bool = False

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
    workspace: Dict[str, Any] = airbyte_service.create_workspace(wsname)

    org.airbyte_workspace_id = workspace["workspaceId"]
    org.save()

    block_name: str = f"{org.slug}-{slugify(AIRBYTESERVER)}"
    airbyte_server_block_cleaned_name: str = block_name
    try:
        airbyte_server_block_id: Optional[str] = prefect_service.get_airbyte_server_block_id(block_name)
    except Exception as exc:
        raise Exception("could not connect to prefect-proxy") from exc

    if airbyte_server_block_id is None:
        (
            airbyte_server_block_id,
            airbyte_server_block_cleaned_name,
        ) = prefect_service.create_airbyte_server_block(block_name)
        logger.info(f"Created Airbyte server block with ID {airbyte_server_block_id}")

    if not OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).exists():
        org_airbyte_server_block: OrgPrefectBlockv1 = OrgPrefectBlockv1(
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


def create_airbyte_deployment(
    org: Org, org_task: OrgTask, server_block: OrgPrefectBlockv1
) -> OrgDataFlowv1:
    """Creates OrgDataFlowv1 & the prefect deployment based on the airbyte OrgTask"""
    hash_code: str = generate_hash_id(8)
    logger.info(f"using the hash code {hash_code} for the deployment name")

    deployment_name: str = f"manual-{org.slug}-{org_task.task.slug}-{hash_code}"
    dataflow: Dict[str, Any] = prefect_service.create_dataflow_v1(
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

    existing_dataflow: Optional[OrgDataFlowv1] = OrgDataFlowv1.objects.filter(
        deployment_id=dataflow["deployment"]["id"]
    ).first()
    if existing_dataflow:
        existing_dataflow.delete()

    org_dataflow: OrgDataFlowv1 = OrgDataFlowv1.objects.create(
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


def create_connection(
    org: Org, payload: AirbyteConnectionCreate
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """creates an airbyte connection and tracking objects in the database"""
    warehouse: Optional[OrgWarehouse] = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"
    if warehouse.airbyte_destination_id is None:
        return None, "warehouse has no airbyte_destination_id"
    payload.destinationId = warehouse.airbyte_destination_id

    org_airbyte_server_block: Optional[OrgPrefectBlockv1] = OrgPrefectBlockv1.objects.filter(
        org=org,
        block_type=AIRBYTESERVER,
    ).first()
    if org_airbyte_server_block is None:
        raise Exception(f"{org.slug} has no {AIRBYTESERVER} block in OrgPrefectBlock")

    sync_task: Optional[Task] = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
    if sync_task is None:
        return None, "sync task not supported"

    clear_task: Optional[Task] = Task.objects.filter(slug=TASK_AIRBYTECLEAR).first()
    if clear_task is None:
        return None, "clear task not supported"

    airbyte_conn: Dict[str, Any] = airbyte_service.create_connection(org.airbyte_workspace_id, payload)

    try:
        with transaction.atomic():
            org_task: OrgTask = OrgTask.objects.create(
                org=org, task=sync_task, connection_id=airbyte_conn["connectionId"]
            )
            sync_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, org_task, org_airbyte_server_block
            )
            org_task = OrgTask.objects.create(
                org=org, task=clear_task, connection_id=airbyte_conn["connectionId"]
            )
            clear_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, org_task, org_airbyte_server_block
            )
            sync_dataflow.clear_conn_dataflow = clear_dataflow
            sync_dataflow.save()

    except Exception as err:
        logger.info("deleting airbyte connection")
        airbyte_service.delete_connection(org.airbyte_workspace_id, airbyte_conn["connectionId"])
        logger.error(err)
        return None, "Couldn't create the connection, please try again"

    res: Dict[str, Any] = {
        "name": payload.name,
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"]},
        "destination": {"id": airbyte_conn["destinationId"]},
        "catalogId": airbyte_conn["sourceCatalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "status": airbyte_conn["status"],
        "deploymentId": sync_dataflow.deployment_id,
        "resetConnDeploymentId": None,
        "clearConnDeploymentId": clear_dataflow.deployment_id,
    }
    return res, None


def get_connections(org: Org) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """return connections with last run details"""
    sync_dataflows: QuerySet[OrgDataFlowv1] = (
        OrgDataFlowv1.objects.filter(org=org, dataflow_type="manual")
        .filter(Q(reset_conn_dataflow_id__isnull=False) | Q(clear_conn_dataflow_id__isnull=False))
        .select_related("clear_conn_dataflow")
    )

    dataflow_ids: List[str] = sync_dataflows.values_list("id", flat=True)
    clear_dataflow_ids: List[str] = sync_dataflows.values_list("clear_conn_dataflow_id", flat=True)
    all_dataflow_orgtasks: QuerySet[DataflowOrgTask] = DataflowOrgTask.objects.filter(
        dataflow_id__in=[pk_id for pk_id in list(dataflow_ids) + list(clear_dataflow_ids) if pk_id is not None]
    ).select_related("orgtask")

    org_task_ids: List[str] = all_dataflow_orgtasks.values_list("orgtask_id", flat=True)
    all_org_task_locks: QuerySet[TaskLock] = TaskLock.objects.filter(orgtask_id__in=org_task_ids)

    all_orgtask_dataflows: QuerySet[DataflowOrgTask] = DataflowOrgTask.objects.filter(orgtask_id__in=org_task_ids).select_related("dataflow")

    warehouse: Optional[OrgWarehouse] = OrgWarehouse.objects.filter(org=org).first()
    airbyte_connections: List[Dict[str, Any]] = airbyte_service.get_webbackend_connections(org.airbyte_workspace_id)

    res: List[Dict[str, Any]] = []

    for sync_dataflow in sync_dataflows:
        sync_dataflow_orgtasks: List[DataflowOrgTask] = [dfot for dfot in all_dataflow_orgtasks if dfot.dataflow_id == sync_dataflow.id]
        clear_dataflow_orgtasks: List[DataflowOrgTask] = [
            dfot for dfot in all_dataflow_orgtasks
            if dfot.dataflow_id == (sync_dataflow.clear_conn_dataflow.id if sync_dataflow.clear_conn_dataflow else -1)
        ]

        org_tasks: List[OrgTask] = [dataflow_orgtask.orgtask for dataflow_orgtask in sync_dataflow_orgtasks]

        if len(org_tasks) == 0:
            logger.error(f"Something wrong with the sync dataflow {sync_dataflow.deployment_name}; no orgtasks found")
            continue
        if len(org_tasks) > 1:
            logger.error(
                f"Something wrong with the sync dataflow {sync_dataflow.deployment_name}; more than 1 orgtasks attached"
            )
            continue

        org_task: OrgTask = org_tasks[0]
        clear_org_task: Optional[OrgTask] = None
        if len(clear_dataflow_orgtasks) > 0:
            rst_orgtasks: List[OrgTask] = [
                dfot.orgtask for dfot in clear_dataflow_orgtasks
                if dfot.dataflow_id == (sync_dataflow.clear_conn_dataflow.id if sync_dataflow.clear_conn_dataflow else -1)
            ]
            if len(rst_orgtasks) > 0:
                clear_org_task = rst_orgtasks[0]

        connection = [conn for conn in airbyte_connections if conn["connectionId"] == org_task.connection_id]
        if len(connection) == 0:
            logger.error(f"could not find connection {org_task.connection_id} in airbyte")
            continue
        connection = connection[0]

        look_up_last_run_deployment_ids: List[str] = [
            df_orgtask.dataflow.deployment_id for df_orgtask in all_orgtask_dataflows if df_orgtask.orgtask_id == org_task.id
        ]

        clear_dataflow: Optional[OrgDataFlowv1] = sync_dataflow.clear_conn_dataflow
        lock: Optional[Dict[str, Any]] = None
        sync_lock: Optional[TaskLock] = next(
            (lock for lock in all_org_task_locks if lock.orgtask_id == org_task.id), None
        )

        if sync_lock:
            lock = fetch_orgtask_lock_v1(org_task, sync_lock)

        if not lock and clear_org_task:
            lock = next(
                (fetch_orgtask_lock_v1(clear_org_task, lock) for lock in all_org_task_locks if lock.orgtask_id == clear_org_task.id),
                None
            )

        connection["destination"]["name"] = warehouse.name if warehouse else ""
        res.append(
            {
                "name": connection["name"],
                "connectionId": connection["connectionId"],
                "source": connection["source"],
                "destination": connection["destination"],
                "status": connection["status"],
                "deploymentId": sync_dataflow.deployment_id,
                "lastRun": None,  # updated below
                "lock": lock,
                "resetConnDeploymentId": None,
                "clearConnDeploymentId": clear_dataflow.deployment_id if clear_dataflow else None,
                "look_up_last_run_deployment_ids": look_up_last_run_deployment_ids,
                "queuedFlowRunWaitTime": (
                    prefect_service.estimate_time_for_next_queued_run_of_dataflow(sync_dataflow)
                    if lock and lock["status"] == TaskLockStatus.QUEUED else None
                ),
            }
        )

    all_last_run_deployment_ids: List[str] = []
    for conn in res:
        all_last_run_deployment_ids.extend(conn["look_up_last_run_deployment_ids"])

    flow_runs_with_row_number: QuerySet[PrefectFlowRun] = PrefectFlowRun.objects.filter(deployment_id__in=all_last_run_deployment_ids).annotate(
        row_number=Window(expression=RowNumber(), partition_by=[F("deployment_id")], order_by=F("start_time").desc())
    )
    last_flow_run_per_deployment: QuerySet[PrefectFlowRun] = flow_runs_with_row_number.filter(row_number=1)

    for conn in res:
        last_runs: List[Dict[str, Any]] = [
            run.to_json() for run in last_flow_run_per_deployment if run.deployment_id in conn["look_up_last_run_deployment_ids"]
        ]
        last_runs.sort(key=lambda run: (run["startTime"] if run["startTime"] else run["expectedStartTime"]))
        conn["lastRun"] = last_runs[-1] if last_runs else None
        del conn["look_up_last_run_deployment_ids"]

    return res, None


def get_one_connection(org: Org, connection_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """retrieve details of a single connection"""
    org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()

    if org_task is None or org_task.connection_id is None:
        return None, "connection not found"

    airbyte_conn: Dict[str, Any] = airbyte_service.get_connection(org.airbyte_workspace_id, org_task.connection_id)

    dataflow_orgtask: Optional[DataflowOrgTask] = DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ).first()

    if dataflow_orgtask is None:
        return None, "deployment not found"

    reset_dataflow: Optional[OrgDataFlowv1] = dataflow_orgtask.dataflow.reset_conn_dataflow
    source_name: str = airbyte_conn["source"]["name"]
    destination_name: str = airbyte_conn["destination"]["name"]

    lock: Optional[Dict[str, Any]] = fetch_orgtask_lock(org_task)
    if not lock and reset_dataflow:
        reset_dataflow_orgtask: Optional[DataflowOrgTask] = DataflowOrgTask.objects.filter(dataflow=reset_dataflow).first()
        if reset_dataflow_orgtask and reset_dataflow_orgtask.orgtask:
            lock = fetch_orgtask_lock(reset_dataflow_orgtask.orgtask)

    res: Dict[str, Any] = {
        "name": airbyte_conn["name"],
        "connectionId": airbyte_conn["connectionId"],
        "source": {"id": airbyte_conn["sourceId"], "name": source_name},
        "destination": {"id": airbyte_conn["destinationId"], "name": destination_name},
        "catalogId": airbyte_conn["catalogId"],
        "syncCatalog": airbyte_conn["syncCatalog"],
        "destinationSchema": airbyte_conn["namespaceFormat"] if airbyte_conn["namespaceDefinition"] == "customformat" else "",
        "status": airbyte_conn["status"],
        "deploymentId": dataflow_orgtask.dataflow.deployment_id if dataflow_orgtask.dataflow else None,
        "lock": lock,
        "resetConnDeploymentId": reset_dataflow.deployment_id if reset_dataflow else None,
    }

    return res, None


def reset_connection(org: Org, connection_id: str) -> Tuple[None, Optional[str]]:
    """reset the connection via the sync deployment this will run a full reset + sync"""
    org_server_block: Optional[OrgPrefectBlockv1] = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=AIRBYTESERVER
    ).first()

    if not org_server_block:
        logger.error("Airbyte server block not found")
        return None, "airbyte server block not found"

    sync_org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()
    if not sync_org_task:
        return None, "connection not found"

    sync_dataflow_orgtask: Optional[DataflowOrgTask] = DataflowOrgTask.objects.filter(
        orgtask=sync_org_task, dataflow__dataflow_type="manual"
    ).first()

    if sync_dataflow_orgtask is None:
        logger.error("Sync dataflow not found")
        return None, "sync dataflow not found"

    reset_org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTERESET
    ).first()

    if not reset_org_task:
        logger.error("Reset OrgTask not found")
        return None, "reset OrgTask not found"

    params: Dict[str, Any] = {
        "config": {
            "tasks": [
                setup_airbyte_sync_task_config(reset_org_task, org_server_block, seq=1).to_json(),
                setup_airbyte_sync_task_config(sync_org_task, org_server_block, seq=2).to_json(),
            ],
            "org_slug": org.slug,
        }
    }

    connection_meta: Optional[ConnectionMeta] = ConnectionMeta.objects.filter(connection_id=connection_id).first()
    is_connection_large_enough: bool = connection_meta.schedule_large_jobs if connection_meta else False

    schedule_at: Optional[datetime] = get_schedule_time_for_large_jobs() if is_connection_large_enough else None
    job: Optional[ConnectionJob] = ConnectionJob.objects.filter(
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
        res: Dict[str, Any] = prefect_service.schedule_deployment_flow_run(
            sync_dataflow_orgtask.dataflow.deployment_id, params, scheduled_time=schedule_at
        )
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


def update_connection(
    org: Org, connection_id: str, payload: AirbyteConnectionUpdate
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """updates an airbyte connection"""
    if not OrgTask.objects.filter(org=org, connection_id=connection_id).exists():
        return None, "connection not found"

    warehouse: Optional[OrgWarehouse] = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"
    if warehouse.airbyte_destination_id is None:
        return None, "warehouse has no airbyte_destination_id"

    payload.destinationId = warehouse.airbyte_destination_id

    if len(payload.streams) == 0:
        return None, "must specify stream names"

    connection: Dict[str, Any] = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)
    connection["operationIds"] = []
    if payload.name:
        connection["name"] = payload.name
    connection["skipReset"] = False

    res: Dict[str, Any] = airbyte_service.update_connection(org.airbyte_workspace_id, payload, connection)
    return res, None


def delete_connection(org: Org, connection_id: str) -> Tuple[None, Optional[str]]:
    """deletes an airbyte connection"""
    dataflows_to_delete: List[OrgDataFlowv1] = []
    orgtask_to_delete: List[OrgTask] = []

    for org_task in OrgTask.objects.filter(org=org, connection_id=connection_id):
        for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask=org_task, dataflow__dataflow_type="manual"):
            dataflows_to_delete.append(dataflow_orgtask.dataflow)
            logger.info("will delete %s", dataflow_orgtask.dataflow.deployment_name)
        orgtask_to_delete.append(org_task)

    for dataflow in dataflows_to_delete:
        logger.info("deleting prefect deployment %s", dataflow.deployment_name)
        prefect_service.delete_deployment_by_id(dataflow.deployment_id)

    logger.info("deleting org dataflows from db")
    for dataflow in dataflows_to_delete:
        if dataflow.id:
            dataflow.delete()

    for org_task in OrgTask.objects.filter(org=org, connection_id=connection_id):
        for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask=org_task, dataflow__dataflow_type="orchestrate"):
            deployment: Dict[str, Any] = prefect_service.get_deployment(dataflow_orgtask.dataflow.deployment_id)
            parameters: Dict[str, Any] = deployment["parameters"]
            for task in parameters["config"]["tasks"]:
                if task.get("connection_id") == connection_id:
                    logger.info(f"deleting task {task['slug']} from deployment")
                    parameters["config"]["tasks"].remove(task)
            payload = PrefectDataFlowUpdateSchema3(deployment_params=parameters)
            prefect_service.update_dataflow_v1(dataflow_orgtask.dataflow.deployment_id, payload)
            logger.info("updated deployment %s", dataflow_orgtask.dataflow.deployment_name)

    for org_task in orgtask_to_delete:
        logger.info("deleting orgtask %s", org_task.task.slug)
        org_task.delete()

    logger.info("deleting airbyte connection %s", connection_id)
    airbyte_service.delete_connection(org.airbyte_workspace_id, connection_id)

    if OrgSchemaChange.objects.filter(connection_id=connection_id).exists():
        OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
        logger.info(f"Deleted schema changes for connection {connection_id}")

    return None, None


def get_job_info_for_connection(org: Org, connection_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """get the info for the latest airbyte job for a connection"""
    org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()

    if org_task is None:
        return None, "connection not found"

    result: Dict[str, Any] = airbyte_service.get_jobs_for_connection(connection_id)
    if len(result["jobs"]) == 0:
        return {"status": "not found"}, None

    latest_job = result["jobs"][0]
    job_info: Dict[str, Any] = airbyte_service.parse_job_info(latest_job)
    job_info["logs"] = airbyte_service.get_logs_for_job(job_info["job_id"])

    return job_info, None


def get_sync_job_history_for_connection(
    org: Org, connection_id: str, limit: int = 10, offset: int = 0
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Get all sync jobs (paginated) for a connection"""
    org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()

    if org_task is None:
        return None, "connection not found"

    res: Dict[str, Any] = {"history": [], "totalSyncs": 0}
    result: Dict[str, Any] = airbyte_service.get_jobs_for_connection(
        connection_id, limit, offset, job_types=["sync", "reset_connection"]
    )
    res["totalSyncs"] = result["totalJobCount"]
    if len(result["jobs"]) == 0:
        return res, None

    for job in result["jobs"]:
        job_info: Dict[str, Any] = airbyte_service.parse_job_info(job)
        res["history"].append(job_info)

    return res, None


def update_destination(
    org: Org, destination_id: str, payload: AirbyteDestinationUpdate
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """updates an airbyte destination and dbt cli profile if credentials changed"""
    destination: Dict[str, Any] = airbyte_service.update_destination(
        destination_id, payload.name, payload.config, payload.destinationDefId
    )
    logger.info("updated destination having id " + destination["destinationId"])
    warehouse: Optional[OrgWarehouse] = OrgWarehouse.objects.filter(org=org).first()

    if warehouse and warehouse.name != payload.name:
        warehouse.name = payload.name
        warehouse.save()

    dbt_credentials: Dict[str, Any] = {}
    if warehouse and warehouse.wtype == "postgres":
        dbt_credentials = update_dict_but_not_stars(payload.config)
        if "dbname" in dbt_credentials:
            dbt_credentials["database"] = dbt_credentials["dbname"]
    elif warehouse and warehouse.wtype == "bigquery":
        dbt_credentials = json.loads(payload.config["credentials_json"])
    elif warehouse and warehouse.wtype == "snowflake":
        dbt_credentials = update_dict_but_not_stars(payload.config)
    else:
        return None, "unknown warehouse type " + (warehouse.wtype if warehouse else "none")

    if warehouse:
        curr_credentials: Dict[str, Any] = secretsmanager.retrieve_warehouse_credentials(warehouse)
        for key, value in curr_credentials.items():
            if key not in dbt_credentials:
                dbt_credentials[key] = value

        secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

        (cli_profile_block, dbt_project_params), error = create_or_update_org_cli_block(org, warehouse, dbt_credentials)
        if error:
            return None, error

        if elementary_setup_status(org) == "set-up":
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


def create_warehouse(org: Org, payload: OrgWarehouseSchema) -> Tuple[None, Optional[str]]:
    """creates a warehouse for an org"""
    if payload.wtype not in ["postgres", "bigquery", "snowflake"]:
        return None, "unrecognized warehouse type " + payload.wtype

    destination: Dict[str, Any] = airbyte_service.create_destination(
        org.airbyte_workspace_id, f"{payload.wtype}-warehouse", payload.destinationDefId, payload.airbyteConfig
    )
    logger.info("created destination having id " + destination["destinationId"])

    dbt_credentials: Optional[Dict[str, Any]] = None
    if payload.wtype == "postgres":
        dbt_credentials = payload.airbyteConfig
    elif payload.wtype == "bigquery":
        dbt_credentials = json.loads(payload.airbyteConfig["credentials_json"])
    elif payload.wtype == "snowflake":
        dbt_credentials = payload.airbyteConfig

    destination_definition: Dict[str, Any] = airbyte_service.get_destination_definition(
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
    credentials_lookupkey: str = secretsmanager.save_warehouse_credentials(warehouse, dbt_credentials)
    warehouse.credentials = credentials_lookupkey
    if "dataset_location" in destination["connectionConfiguration"]:
        warehouse.bq_location = destination["connectionConfiguration"]["dataset_location"]
    warehouse.save()

    create_or_update_org_cli_block(org, warehouse, payload.airbyteConfig)

    return None, None


def create_or_update_org_cli_block(
    org: Org, warehouse: OrgWarehouse, airbyte_creds: Dict[str, Any]
) -> Tuple[Tuple[Optional[OrgPrefectBlockv1], Optional[DbtProjectParams]], Optional[str]]:
    """Create/update the block in db and also in prefect"""
    bqlocation: Optional[str] = (
        airbyte_creds["dataset_location"] if warehouse.wtype == "bigquery" and "dataset_location" in airbyte_creds else None
    )
    profile_name: Optional[str] = None
    target: Optional[str] = None
    dbt_project_params: Optional[DbtProjectParams] = None

    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, org.dbt)
        dbt_project_filename = str(Path(dbt_project_params.project_dir) / "dbt_project.yml")
        if not os.path.exists(dbt_project_filename):
            raise HttpError(400, dbt_project_filename + " is missing")

        with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
            dbt_project: Dict[str, Any] = yaml.safe_load(dbt_project_file)
            if "profile" not in dbt_project:
                raise HttpError(400, "could not find 'profile:' in dbt_project.yml")
        profile_name = dbt_project["profile"]
        target = dbt_project_params.target
    except Exception as err:
        logger.error(
            "Failed to fetch the dbt profile - looks like transformation has not been setup. Using 'default' as profile name and continuing"
        )
        logger.error(err)

    dbt_creds: Dict[str, Any] = map_airbyte_destination_spec_to_dbtcli_profile(airbyte_creds, dbt_project_params)
    dbt_creds.pop("ssl_mode", None)
    dbt_creds.pop("ssl", None)

    profile_name = profile_name or "default"
    target = target or "default"

    logger.info("Found org=%s profile_name=%s target=%s", org.slug, profile_name, target)
    cli_profile_block: Optional[OrgPrefectBlockv1] = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=DBTCLIPROFILE
    ).first()

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
            logger.error("Failed to update the cli profile block %s , err=%s", cli_profile_block.block_name, str(error))
            return (None, None), "Failed to update the cli profile block"
        logger.info(f"Successfully updated the cli profile block : {cli_profile_block.block_name}")
    else:
        logger.info("Creating a new cli profile block for %s with profile=%s & target=%s ", org.slug, profile_name, target)
        new_block_name: str = f"{org.slug}-{profile_name}"
        try:
            cli_block_response: Dict[str, Any] = prefect_service.create_dbt_cli_profile_block(
                block_name=new_block_name,
                profilename=profile_name,
                target=target,
                wtype=warehouse.wtype,
                bqlocation=bqlocation,
                credentials=dbt_creds,
            )
        except Exception as error:
            logger.error("Failed to create a new cli profile block %s , err=%s", new_block_name, str(error))
            return (None, None), "Failed to update the cli profile block"

        cli_profile_block = OrgPrefectBlockv1.objects.create(
            org=org,
            block_type=DBTCLIPROFILE,
            block_id=cli_block_response["block_id"],
            block_name=cli_block_response["block_name"],
        )

    return (cli_profile_block, dbt_project_params), None


def get_warehouses(org: Org) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """return list of warehouses for an Org"""
    warehouses: List[Dict[str, Any]] = [
        {
            "wtype": warehouse.wtype,
            "name": warehouse.name,
            "airbyte_destination": airbyte_service.get_destination(org.airbyte_workspace_id, warehouse.airbyte_destination_id),
            "airbyte_docker_repository": warehouse.airbyte_docker_repository,
            "airbyte_docker_image_tag": warehouse.airbyte_docker_image_tag,
        }
        for warehouse in OrgWarehouse.objects.filter(org=org)
    ]
    return warehouses, None


def get_demo_whitelisted_source_config(type_: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Returns the config of whitelisted source based on type"""
    ret_src: Optional[Dict[str, Any]] = next((src for src in DEMO_WHITELIST_SOURCES if src["type"] == type_), None)
    if not ret_src:
        return None, "source not found"
    return ret_src["config"], None


def delete_source(org: Org, source_id: str) -> Tuple[None, Optional[str]]:
    """deletes an airbyte source and related connections"""
    connections: List[Dict[str, Any]] = airbyte_service.get_connections(org.airbyte_workspace_id)["connections"]
    connections_of_source: List[str] = [conn["connectionId"] for conn in connections if conn["sourceId"] == source_id]

    org_tasks: QuerySet[OrgTask] = OrgTask.objects.filter(org=org, connection_id__in=connections_of_source).all()
    df_orgtasks: QuerySet[DataflowOrgTask] = DataflowOrgTask.objects.filter(orgtask__in=org_tasks).all()
    dataflows: List[OrgDataFlowv1] = [df_orgtask.dataflow for df_orgtask in df_orgtasks]

    logger.info("Deleting the deployments from prefect and django")
    for dataflow in dataflows:
        try:
            prefect_service.delete_deployment_by_id(dataflow.deployment_id)
            logger.info(f"delete deployment: {dataflow.deployment_id} from prefect")
            dataflow.delete()
        except Exception as error:
            logger.info(f"something went wrong deleting the prefect deployment {dataflow.deployment_id}")
            logger.exception(error)
            continue

    logger.info("Deleting org tasks related to connections in this source")
    for org_task in org_tasks:
        org_task.delete()

    airbyte_service.delete_source(org.airbyte_workspace_id, source_id)
    logger.info(f"deleted airbyte source {source_id}")

    return None, None


def fetch_and_update_org_schema_changes(
    org: Org, connection_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Fetches the schema change catalog from airbyte and updates OrgSchemaChange in our db"""
    try:
        logger.info(f"Fetching schema change (catalog) for connection {org.slug}|{connection_id}")
        connection_catalog: Dict[str, Any] = airbyte_service.get_connection_catalog(connection_id, timemout=60)
    except Exception as err:
        return None, f"Something went wrong fetching schema change (catalog) for connection {connection_id}: {err}"

    try:
        change_type: str = connection_catalog.get("schemaChange")
        logger.info(f"Schema change detected for org {org.slug}: {change_type}")

        if change_type not in ["breaking", "non_breaking", "no_change"]:
            raise ValueError("Invalid schema change type")

        catalog_diff: Dict[str, Any] = connection_catalog.get("catalogDiff", {})

        if change_type == "breaking" or (
            change_type == "non_breaking" and catalog_diff and len(catalog_diff.get("transforms", [])) > 0
        ):
            OrgSchemaChange.objects.update_or_create(
                connection_id=connection_id, defaults={"change_type": change_type, "org": org}
            )
        else:
            schema_change: Optional[OrgSchemaChange] = OrgSchemaChange.objects.filter(connection_id=connection_id).first()
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
        return None, f"Something went wrong updating OrgSchemaChange {connection_id}: {err}"

    return connection_catalog, None


def update_connection_schema(
    org: Org, connection_id: str, payload: AirbyteConnectionSchemaUpdate
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Update the schema changes of a connection."""
    if not OrgTask.objects.filter(org=org, connection_id=connection_id).exists():
        return None, "connection not found"

    warehouse: Optional[OrgWarehouse] = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        return None, "need to set up a warehouse first"

    connection: Dict[str, Any] = airbyte_service.get_connection(org.airbyte_workspace_id, connection_id)
    connection["skipReset"] = True

    res: Optional[Dict[str, Any]] = airbyte_service.update_schema_change(org, payload, connection)
    if res:
        OrgSchemaChange.objects.filter(connection_id=connection_id).delete()

    return res, None


def get_schema_changes(org: Org) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Get the schema changes of a connection in an org."""
    org_schema_change: QuerySet[OrgSchemaChange] = OrgSchemaChange.objects.filter(org=org).select_related("schedule_job").all()

    if not org_schema_change.exists():
        return None, "No schema change found"

    large_connections: QuerySet[str] = ConnectionMeta.objects.filter(
        connection_id__in=[change.connection_id for change in org_schema_change], schedule_large_jobs=True
    ).values_list("connection_id", flat=True)

    schema_changes: List[Dict[str, Any]] = [
        {
            **model_to_dict(change, exclude=["org", "id"]),
            "schedule_job": (
                {
                    "scheduled_at": change.schedule_job.scheduled_at,
                    "flow_run_id": change.schedule_job.flow_run_id,
                    "job_type": change.schedule_job.job_type,
                    "status": None,
                    "state_name": None,
                } if change.schedule_job else None
            ),
            "is_connection_large": change.connection_id in large_connections,
            "next_job_at": get_schedule_time_for_large_jobs(),
            "run": None,
        }
        for change in org_schema_change
    ]

    all_flow_run_ids: List[str] = [
        change["schedule_job"]["flow_run_id"]
        for change in schema_changes if change["schedule_job"] and "flow_run_id" in change["schedule_job"]
    ]

    runs: QuerySet[PrefectFlowRun] = PrefectFlowRun.objects.filter(flow_run_id__in=all_flow_run_ids).all()

    for change in schema_changes:
        if change["schedule_job"] and "flow_run_id" in change["schedule_job"]:
            curr_run: List[PrefectFlowRun] = [
                run for run in runs if run.flow_run_id == change["schedule_job"]["flow_run_id"]
            ]
            change["schedule_job"]["run"] = curr_run[0].to_json() if curr_run else None

    return schema_changes, None


def schedule_update_connection_schema(
    orguser: OrgUser, connection_id: str, payload: AirbyteConnectionSchemaUpdateSchedule
) -> Dict[str, Any]:
    """Submits a flow run that will execute the schema update flow"""
    server_block: Optional[OrgPrefectBlockv1] = OrgPrefectBlockv1.objects.filter(
        org=orguser.org, block_type=AIRBYTESERVER
    ).first()
    if server_block is None:
        raise HttpError(400, "airbyte server block not found")

    org_task: Optional[OrgTask] = OrgTask.objects.filter(
        org=orguser.org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()
    if not org_task:
        raise HttpError(400, "Orgtask not found")

    dataflow_orgtask: Optional[DataflowOrgTask] = DataflowOrgTask.objects.filter(
        orgtask=org_task, dataflow__dataflow_type="manual"
    ).first()
    if not dataflow_orgtask:
        raise HttpError(400, "no dataflow mapped")

    connection_meta: Optional[ConnectionMeta] = ConnectionMeta.objects.filter(connection_id=connection_id).first()
    is_connection_large_enough: bool = connection_meta.schedule_large_jobs if connection_meta else False

    logger.info("connection is large enough: %s", is_connection_large_enough)

    locks: List[TaskLock] = []
    schedule_at: Optional[datetime] = None
    job: Optional[ConnectionJob] = None

    if not is_connection_large_enough:
        locks = prefect_service.lock_tasks_for_deployment(
            dataflow_orgtask.dataflow.deployment_id, orguser, dataflow_orgtasks=[dataflow_orgtask]
        )
    else:
        schedule_at = get_schedule_time_for_large_jobs()

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

    try:
        res: Dict[str, Any] = prefect_service.schedule_deployment_flow_run(
            dataflow_orgtask.dataflow.deployment_id,
            {
                "config": {
                    "org_slug": orguser.org.slug,
                    "tasks": [
                        setup_airbyte_update_schema_task_config(org_task, server_block, payload.catalogDiff).to_json()
                    ],
                }
            },
            schedule_at,
        )

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

            schema_change: Optional[OrgSchemaChange] = OrgSchemaChange.objects.filter(
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
