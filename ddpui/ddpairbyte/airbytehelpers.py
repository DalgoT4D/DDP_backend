"""
functions which work with airbyte and with the dalgo database
"""

import json
import re
from typing import List, Tuple
from uuid import uuid4

from datetime import datetime, timedelta
import pytz
from ninja.errors import HttpError
from django.utils.text import slugify
from django.utils import timezone as djangotimezone
from django.conf import settings
from django.db import transaction
from django.db.models import F, Window, Q
from django.db.models.functions import RowNumber
from django.forms.models import model_to_dict

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteWorkspace
from ddpui.core.oauth import google_oauth_service, google_service_account
from ddpui.core.oauth.google_oauth_provider import (
    GSHEETS_SOURCE_NAME,
    get_connector,
    oauth_client_id,
    oauth_client_secret,
)
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgSchemaChange,
    ConnectionMeta,
)
from ddpui.models.airbyte import AirbyteJob
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.org import OrgDataFlowv1, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask, TaskLockStatus

from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
    AirbyteConnectionSchemaUpdateSchedule,
    AirbyteGetConnectionsResponse,
    SourceGoogleOAuthCreate,
    SourceGoogleOAuthUpdate,
)
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.schemas.org_warehouse_schema import OrgWarehouseSchema
from ddpui.ddpprefect import prefect_service, schema, DBTCORE, AIRBYTESERVER
from ddpui.celeryworkers.airbytehelpertasks import delete_airbyte_connections

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import timezone

from ddpui.utils.constants import (
    TASK_AIRBYTESYNC,
    TASK_AIRBYTECLEAR,
    AIRBYTE_CONNECTION_DEPRECATED,
)
from ddpui.utils.helpers import (
    generate_hash_id,
    update_dict_but_not_stars,
    resolve_stars,
    from_timestamp,
    nice_bytes,
    get_integer_env_var,
)
from ddpui.utils import secretsmanager
from ddpui.core.pipelinefunctions import (
    setup_airbyte_sync_task_config,
    setup_airbyte_update_schema_task_config,
    build_connection_block_extra,
)
from ddpui.core.orgtaskfunctions import fetch_orgtask_lock_v1
from ddpui.models.tasks import TaskLock
from ddpui.ddpdbt.elementary_service import create_elementary_profile, elementary_setup_status
from ddpui.ddpdbt.dbthelpers import create_or_update_dbt_profile_secret_blk, write_dbt_profiles_yml
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("airbyte")


def _build_oauth_source_config(
    orguser: OrgUser, source_name: str, refresh_token_ref: str, config: dict
) -> dict:
    """Redeem the refresh_token_ref and return `config` with a backend-built `credentials`
    block merged in. The refresh_token never travels through the browser; it is fetched
    server-side from the ref and folded into the connector credentials here.

    The connector is looked up by source-definition NAME, as supplied by the frontend from
    the same workspace catalog it got `sourceDefId` from — it is the OAuth registry key."""
    refresh_token = google_oauth_service.redeem_refresh_token_ref(
        orguser, refresh_token_ref, source_name
    )
    connector = get_connector(source_name)
    credentials = connector.credentials_builder(
        oauth_client_id(), oauth_client_secret(), refresh_token
    )
    return {**config, "credentials": credentials}


def inject_managed_gsheets_credentials(source_def_name: str, config: dict) -> dict:
    """MANAGED-SA bridge — fill in Dalgo's own Google service-account key for a Google Sheets
    source that arrived without one. See `core/oauth/google_service_account.py` to remove it.

    `source_def_name` is off the request payload, so this trusts the caller about the connector
    type. Fine for an internal API; revisit if that changes."""
    if source_def_name != GSHEETS_SOURCE_NAME:
        return config

    key = google_service_account.managed_service_account_json()
    if key is None:
        return config

    # A pasted key is the bring-your-own-key option — only fill an empty slot. No Client
    # (OAuth) guard needed: sign-in is unreleased, so no saved source carries one.
    credentials = config.get(google_service_account.CREDENTIALS_KEY) or {}
    if credentials.get(google_service_account.SERVICE_INFO_KEY):
        return config

    logger.info("using the dalgo-managed google service account for this google sheets source")
    return {
        **config,
        google_service_account.CREDENTIALS_KEY: {
            google_service_account.AUTH_TYPE_KEY: google_service_account.SERVICE_AUTH_TYPE,
            google_service_account.SERVICE_INFO_KEY: key,
        },
    }


def create_oauth_source(orguser: OrgUser, payload: SourceGoogleOAuthCreate) -> dict:
    """Create a NEW source from a redeemed OAuth refresh_token_ref."""
    config = _build_oauth_source_config(
        orguser, payload.sourceName, payload.refresh_token_ref, payload.config
    )
    source = airbyte_service.create_source(
        orguser.org.airbyte_workspace_id, payload.name, payload.sourceDefId, config
    )
    logger.info("created oauth source having id %s", source["sourceId"])
    return {"sourceId": source["sourceId"]}


def update_oauth_source(orguser: OrgUser, source_id: str, payload: SourceGoogleOAuthUpdate) -> dict:
    """Re-authenticate an EXISTING source from a redeemed OAuth refresh_token_ref.

    Verifies `source_id` belongs to the caller's own workspace before updating — Airbyte's
    sources/update is not workspace-scoped, so without this guard a caller could overwrite a
    source in another org's workspace by passing its id."""
    workspace_id = orguser.org.airbyte_workspace_id
    source = airbyte_service.get_source(workspace_id, source_id)
    if source.get("workspaceId") != workspace_id:
        raise HttpError(404, "source not found")

    config = _build_oauth_source_config(
        orguser, payload.sourceName, payload.refresh_token_ref, payload.config
    )
    source = airbyte_service.update_source(source_id, payload.name, config, payload.sourceDefId)
    logger.info("updated oauth source having id %s", source["sourceId"])
    return {"sourceId": source["sourceId"]}


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
        ),
        org.get_queue_config().connection_sync_queue,  # manual airbyte syncs use this queue
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
            sync_org_task = OrgTask.objects.create(
                org=org,
                task=sync_task,
                connection_id=airbyte_conn["connectionId"],
                post_sync_transform=payload.post_sync_transform or None,
            )

            sync_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, sync_org_task, org_airbyte_server_block
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

            ConnectionMeta.objects.create(
                connection_id=airbyte_conn["connectionId"], connection_name=payload.name
            )

        # Always upsert the AirbyteConnection block so every connection has a corresponding
        # Prefect block (carries the connection name + any post-sync ops). Consistent with
        # update_connection which also always upserts.
        try:
            extra = build_connection_block_extra(sync_org_task)
            prefect_service.upsert_airbyte_connection_block(
                server_block_name=org_airbyte_server_block.block_name,
                connection_id=airbyte_conn["connectionId"],
                connection_name=payload.name,
                extra=extra,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to upsert airbyte connection block for connection=%s: %s",
                airbyte_conn["connectionId"],
                str(err),
            )

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
        "post_sync_transform": payload.post_sync_transform or None,
    }
    return res, None


def get_connections(org: Org) -> Tuple[List[AirbyteGetConnectionsResponse], None]:
    """
        Purpose:

    This function returns all active Airbyte connections for a given organization (org), along with metadata such as:
            •	sync and clear dataflows,
            •	the most recent run,
            •	lock status (i.e., whether a sync is queued or running),
            •	an estimated wait time if a run is queued.

    It also identifies and schedules the deletion of connections that are no longer found in Airbyte or have been deprecated.

    ⸻

    What It Does:
            1.	Fetch Dataflows:
    Filters for manual-type dataflows associated with the organization that have a corresponding clear flow configured.
            2.	Fetch OrgTasks and TaskLocks:
            •	Retrieves OrgTask objects linked to those dataflows.
            •	Retrieves TaskLock entries to assess the state of each task (e.g., queued or running).
            3.	Fetch Connections from Airbyte:
    Retrieves a list of all Airbyte connections in the org’s workspace.
            4.	Iterate Through Each Dataflow:
    For each sync dataflow:
            •	Matches it with its associated OrgTask.
            •	Tries to find the related clear task (if present).
            •	Looks up the corresponding connection info from Airbyte.
            •	If a connection is not found or is marked as deprecated, it adds it to a cleanup list.
            5.	Lock and Run Status:
            •	Finds any task locks.
            •	Uses the lock object to estimate if a sync is queued and how long the wait is likely to be.
            6.	Last Run Information:
            •	Collects all deployment IDs across all relevant org tasks.
            •	Queries for the most recent flow runs for each deployment.
            •	Attaches the latest run information to each connection.
            7.	Cleanup:
            •	If any connections were found to be missing or deprecated, it schedules them for deletion in the background via a Celery task.

    ⸻

    Return Value:

    A list of dictionaries, one per active connection, containing:
            •	basic connection info (name, status, source, destination),
            •	sync deployment ID,
            •	clear/reset deployment IDs (if present),
            •	lock status (queued, running, etc.),
            •	last successful run metadata,
            •	estimated wait time if queued.

    """
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

    # # for the above of orgtask_ids, we fetch all the dataflows of each orgtasks
    # # this will be used to find the last run of each dataflow
    # all_orgtask_dataflows = DataflowOrgTask.objects.filter(
    #     orgtask_id__in=org_task_ids
    # ).select_related("dataflow")

    warehouse = OrgWarehouse.objects.filter(org=org).first()

    airbyte_connections = airbyte_service.get_webbackend_connections(org.airbyte_workspace_id)

    res: list[dict] = []
    connections_to_clean_up: list[str] = []
    redisclient = RedisClient.get_instance()

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

        def ensure_only_one_add_across_parallel_requests(connection_id: str):
            """avoid a race condition in case get_connections() is called twice in parallel"""
            cacheval = redisclient.get(f"deleting-{connection_id}")
            if not cacheval or not cacheval == "true":
                redisclient.set(f"deleting-{connection_id}", "true", 10)  # expire after 10 seconds
                connections_to_clean_up.append(connection_id)

        if len(connection) == 0:
            logger.error(f"could not find connection {org_task.connection_id} in airbyte")
            ensure_only_one_add_across_parallel_requests(org_task.connection_id)
            continue

        connection = connection[0]
        if connection["status"] == AIRBYTE_CONNECTION_DEPRECATED:
            ensure_only_one_add_across_parallel_requests(org_task.connection_id)
            continue

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
                # "look_up_last_run_deployment_ids": look_up_last_run_deployment_ids,
                "queuedFlowRunWaitTime": None,
            }
        )

    connection_ids = [conn["connectionId"] for conn in res]

    latest_airbyte_jobs_grouped = AirbyteJob.objects.filter(config_id__in=connection_ids).annotate(
        row_number=Window(
            expression=RowNumber(), partition_by=[F("config_id")], order_by=F("created_at").desc()
        )
    )

    latest_airbyte_jobs = latest_airbyte_jobs_grouped.filter(row_number=1)

    # attach last run for each conn
    for conn in res:
        # find the latest job for this connection
        latest_job = latest_airbyte_jobs.filter(config_id=conn["connectionId"]).first()

        if latest_job:
            conn["lastRun"] = {
                "airbyteJobId": latest_job.job_id,
                "status": latest_job.status,
                "startTime": latest_job.created_at,
                "expectedStartTime": latest_job.created_at,
            }
        else:
            conn["lastRun"] = None

    if connections_to_clean_up:
        logger.info(f"cleaning up connections {connections_to_clean_up}")
        delete_airbyte_connections.delay(
            f"delete-connections-{org.slug}", org.id, connections_to_clean_up
        )

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
    if airbyte_conn["status"] == AIRBYTE_CONNECTION_DEPRECATED:
        delete_airbyte_connections.delay(
            f"delete-connections-{org.slug}", org.id, [org_task.connection_id]
        )
        return None, "connection not found"

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

    sync_lock = TaskLock.objects.filter(orgtask=org_task).first()
    lock = fetch_orgtask_lock_v1(org_task, sync_lock)

    if not lock and reset_dataflow:
        reset_dataflow_orgtask = DataflowOrgTask.objects.filter(dataflow=reset_dataflow).first()

        if reset_dataflow_orgtask.orgtask:
            reset_lock = TaskLock.objects.filter(orgtask=reset_dataflow_orgtask.orgtask).first()
            lock = fetch_orgtask_lock_v1(reset_dataflow_orgtask.orgtask, reset_lock)

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
        "post_sync_transform": org_task.post_sync_transform,
    }

    return res, None


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
    if connection["status"] == AIRBYTE_CONNECTION_DEPRECATED:
        delete_airbyte_connections.delay(f"delete-connections-{org.slug}", org.id, [connection_id])
        return None, "connection not found"

    connection["operationIds"] = []

    # update name
    if payload.name:
        connection["name"] = payload.name

    # always reset the connection
    connection["skipReset"] = False

    # update the airbyte connection
    res = airbyte_service.update_connection(org.airbyte_workspace_id, payload, connection)

    if payload.name:
        ConnectionMeta.objects.filter(connection_id=connection_id).update(
            connection_name=connection["name"]
        )

    # Update post_sync_transform on the sync OrgTask and refresh the connection block's extra.
    sync_org_task = OrgTask.objects.filter(
        org=org, connection_id=connection_id, task__slug=TASK_AIRBYTESYNC
    ).first()
    if sync_org_task:
        sync_org_task.post_sync_transform = payload.post_sync_transform or None
        sync_org_task.save(update_fields=["post_sync_transform"])

        server_block = OrgPrefectBlockv1.objects.filter(org=org, block_type=AIRBYTESERVER).first()
        if server_block:
            try:
                extra = build_connection_block_extra(sync_org_task)
                prefect_service.upsert_airbyte_connection_block(
                    server_block_name=server_block.block_name,
                    connection_id=connection_id,
                    connection_name=connection.get("name", ""),
                    extra=extra,
                )
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to upsert airbyte connection block for connection=%s: %s",
                    connection_id,
                    str(err),
                )

    return res, None


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
    job_info["logs"] = airbyte_service.get_logs_for_job(job_info["job_id"])

    return job_info, None


def get_sync_job_history_for_connection(
    org: Org, connection_id: str, limit: int = 10, offset: int = 0
):
    """
    Get all sync jobs (paginated) for a connection using AirbyteJob model
    Returns
    - Date
    - Records synced
    - Bytes synced
    - Duration

    In case there no sync jobs, return an empty list
    """

    org_task = OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
        task__slug=TASK_AIRBYTESYNC,
    ).first()

    if org_task is None:
        return None, "connection not found"

    # Query AirbyteJob model instead of calling airbyte service
    airbyte_jobs = AirbyteJob.objects.filter(
        config_id=connection_id, job_type__in=["sync", "reset_connection"]
    ).order_by("-created_at")[offset : offset + limit]

    total_syncs = AirbyteJob.objects.filter(
        config_id=connection_id, job_type__in=["sync", "reset_connection"]
    ).count()

    res = {"history": [], "totalSyncs": total_syncs}

    if not airbyte_jobs.exists():
        return res, None

    for job in airbyte_jobs:
        job_info = {
            "job_id": job.job_id,
            "status": job.status,
            "job_type": job.job_type,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "ended_at": job.ended_at,
            "records_emitted": job.records_emitted or 0,
            "bytes_emitted": nice_bytes(job.bytes_emitted) or 0,
            "records_committed": job.records_committed or 0,
            "bytes_committed": nice_bytes(job.bytes_committed) or 0,
            "stream_stats": job.stream_stats,
            "reset_config": job.reset_config,
            "duration_seconds": job.duration,
            "last_attempt_no": job.last_attempt_no,
        }

        res["history"].append(job_info)

    return res, None


def update_destination(org: Org, destination_id: str, payload: AirbyteDestinationUpdate) -> dict:
    """updates an airbyte destination and dbt cli profile if credentials changed"""
    destination = airbyte_service.update_destination(
        destination_id, payload.name, payload.config, payload.destinationDefId
    )
    logger.info("updated destination having id " + destination["destinationId"])
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if warehouse.name != payload.name:
        warehouse.name = payload.name
        warehouse.save()

    curr_credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    # payload is the authoritative source of truth; only replace starred values
    # from curr_credentials — never carry over keys absent from the new payload
    airbyte_creds = {}
    if warehouse.wtype == "postgres":
        airbyte_creds = resolve_stars(payload.config, curr_credentials)
        # i've forgotten why we have this here, airbyte sends "database" - RC
        if "dbname" in airbyte_creds:
            airbyte_creds["database"] = airbyte_creds["dbname"]

    elif warehouse.wtype == "bigquery":
        if not re.match(r"^\*+$", payload.config["credentials_json"]):
            airbyte_creds = json.loads(payload.config["credentials_json"])

        airbyte_creds["dataset_location"] = payload.config["dataset_location"]
        airbyte_creds["transformation_priority"] = payload.config["transformation_priority"]

        if warehouse.bq_location != payload.config["dataset_location"]:
            warehouse.bq_location = payload.config["dataset_location"]
            warehouse.save()

    else:
        raise ValueError("unknown warehouse type " + warehouse.wtype)

    secretsmanager.update_warehouse_credentials(warehouse, airbyte_creds)

    create_or_update_dbt_profile_secret_blk(org, warehouse, airbyte_creds)

    return destination


def create_warehouse(org: Org, payload: OrgWarehouseSchema) -> OrgWarehouse:
    """creates a warehouse for an org"""

    if payload.wtype not in ["postgres", "bigquery"]:
        raise ValueError(f"unknown warehouse type {payload.wtype}")

    destination = airbyte_service.create_destination(
        org.airbyte_workspace_id,
        f"{payload.wtype}-warehouse",
        payload.destinationDefId,
        payload.airbyteConfig,
    )
    logger.info("created destination having id " + destination["destinationId"])

    airbyte_creds = None
    if payload.wtype == "bigquery":
        credentials_json = json.loads(payload.airbyteConfig["credentials_json"])
        airbyte_creds = credentials_json
        airbyte_creds["dataset_location"] = payload.airbyteConfig["dataset_location"]
        airbyte_creds["transformation_priority"] = payload.airbyteConfig["transformation_priority"]
    else:  # postgres
        airbyte_creds = payload.airbyteConfig

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
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(warehouse, airbyte_creds)
    warehouse.credentials = credentials_lookupkey
    if "dataset_location" in destination["connectionConfiguration"]:
        warehouse.bq_location = destination["connectionConfiguration"]["dataset_location"]
    warehouse.save()

    create_or_update_dbt_profile_secret_blk(org, warehouse, airbyte_creds)

    return warehouse


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


def delete_source(org: Org, source_id: str):
    """deletes an airbyte source and related connections"""
    connections = airbyte_service.get_connections(org.airbyte_workspace_id)["connections"]
    connections_of_source = [
        conn["connectionId"] for conn in connections if conn["sourceId"] == source_id
    ]
    connection_names = [conn["name"] for conn in connections if conn["sourceId"] == source_id]

    if connections_of_source:
        raise HttpError(
            403,
            f"Cannot delete source. It is used in connection(s): {', '.join(connection_names)}. Please remove these connections first.",
        )

    # Fetch org tasks and deployments mapped to each connection_id
    org_tasks = OrgTask.objects.filter(org=org, connection_id__in=connections_of_source).all()

    # Fetch dataflows(manual or pipelines) - only proceed if validation passed
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
        connection_catalog = airbyte_service.get_connection_catalog(
            connection_id,
            timeout=get_integer_env_var(
                "AIRBYTE_FETCH_CONNECTION_CATALOG_TIMEOUT_SECONDS", 300, logger
            ),
        )
    except Exception as err:
        return (
            None,
            f"Something went wrong fetching schema change (catalog) for {org.slug} connection {connection_id}: {err}",
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
            f"Something went wrong updating OrgSchemaChange {org.slug} {connection_id}: {err}",
        )

    return connection_catalog, None


def get_schema_changes(org: Org):
    """
    Get the schema changes of a connection in an org.
    """
    org_schema_change = OrgSchemaChange.objects.filter(org=org).select_related("schedule_job").all()

    if org_schema_change is None:
        return None, "No schema change found"

    schema_changes = []
    for change in org_schema_change:
        schema_changes.append(model_to_dict(change, exclude=["org", "id"]))

    logger.info(schema_changes)

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

    locks: list[TaskLock] = prefect_service.lock_tasks_for_deployment(
        dataflow_orgtask.dataflow.deployment_id,
        orguser,
        dataflow_orgtasks=[dataflow_orgtask],
    )

    # create the new flow run; schedule now or schedule later for large connections
    try:
        res = prefect_service.create_deployment_flow_run(
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
        )
        PrefectFlowRun.objects.create(
            deployment_id=dataflow_orgtask.dataflow.deployment_id,
            flow_run_id=res["flow_run_id"],
            name=res.get("name", ""),
            start_time=None,
            expected_start_time=djangotimezone.now(),
            total_run_time=-1,
            status="Scheduled",
            state_name="Scheduled",
            retries=0,
            orguser=orguser,
        )

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


def fetch_and_update_airbyte_job_details(job_id: str):
    """Fetches the details of an Airbyte job and populates in our db."""
    job_info = airbyte_service.get_job_info_without_logs(job_id)

    job_data: dict = job_info.get("job", {})
    attempts_data: list[dict] = job_info.get("attempts", [])
    if not attempts_data:
        logger.info(f"No attempts found for job_id={job_id}")
        raise Exception(f"No attempts found for job_id={job_id}")

    # attempt with the highest id is the latest attempt
    attempts = list(map(lambda x: x["attempt"], attempts_data))
    latest_attempt: dict = max(attempts, key=lambda x: x["id"], default=None)

    if not latest_attempt:
        logger.error("No attempts found for job_id=%s", job_id)
        raise Exception(f"No attempts found for job_id={job_id}")

    # for started_at ; take the minimum of the startedAt of all attempts
    started_at = min(
        [attempt.get("createdAt") for attempt in attempts if attempt.get("createdAt") is not None],
        default=None,
    )

    if not started_at:
        logger.error("No startedAt found for job_id=%s", job_id)
        raise Exception(f"No startedAt found for job_id={job_id}")

    # Prepare fields for AirbyteJob
    job_fields = {
        "job_id": job_data.get("id"),
        "job_type": job_data.get("configType"),
        "config_id": job_data.get("configId"),
        "status": job_data.get("status"),
        "reset_config": job_data.get("resetConfig"),
        "refresh_config": job_data.get("refreshConfig"),
        "stream_stats": job_data.get("streamAggregatedStats"),
        "records_emitted": job_data.get("aggregatedStats", {}).get("recordsEmitted", 0),
        "bytes_emitted": job_data.get("aggregatedStats", {}).get("bytesEmitted", 0),
        "records_committed": job_data.get("aggregatedStats", {}).get("recordsCommitted", 0),
        "bytes_committed": job_data.get("aggregatedStats", {}).get("bytesCommitted", 0),
        "started_at": from_timestamp(started_at),
        "ended_at": from_timestamp(latest_attempt.get("endedAt")),
        "created_at": from_timestamp(job_data.get("createdAt")),
        "attempts": attempts_data,
    }

    # Create or update the AirbyteJob entry
    airbyte_job, is_created = AirbyteJob.objects.update_or_create(
        job_id=job_fields["job_id"],
        defaults=job_fields,
    )

    if is_created:
        logger.info(f"Created new AirbyteJob with job_id={job_id}")

    return model_to_dict(airbyte_job)


def fetch_and_update_airbyte_jobs_for_all_connections(
    last_n_days: int = 0, last_n_hours: int = 0, connection_id: str = None, org: Org = None
):
    """
    Fetches and updates Airbyte job details for all connections in the given org
    or for a specific connection if provided.
    Args:
        last_n_days (int): Number of days to look back for jobs.
        connection_id (str, optional): Specific connection ID to filter jobs.
        org (Org, optional): Organization object to filter jobs.
    Raises:
        ValueError: If last_n_days is not a positive integer.
    """

    if not isinstance(last_n_days, int) or last_n_days < 0:
        raise ValueError("last_n_days must be a non-negative integer")

    # figure out start datetime and end datetime based on now & last_n_days
    start_time = datetime.now(pytz.utc) - timedelta(days=last_n_days, hours=last_n_hours)
    end_time = datetime.now(pytz.utc)

    org_tasks = OrgTask.objects.filter(connection_id__isnull=False)

    if org:
        org_tasks = org_tasks.filter(org=org)

    if connection_id:
        org_tasks = org_tasks.filter(connection_id=connection_id)

    for orgtask_connection_id in org_tasks.values_list("connection_id", flat=True).distinct():
        try:
            logger.info("Syncing job history for connection %s", orgtask_connection_id)

            offset = 0
            limit = 20
            curr_itr_count = 20
            while curr_itr_count >= limit:
                # by default the jobs are ordered by createdAt
                jobs_dict = airbyte_service.get_jobs_for_connection(
                    orgtask_connection_id,
                    limit=limit,
                    offset=offset,
                    job_types=["sync", "reset_connection", "clear", "refresh"],
                    created_at_start=start_time,
                    created_at_end=end_time,
                )
                if len(jobs_dict.get("jobs", [])) == 0:
                    logger.info("No jobs found for connection %s", orgtask_connection_id)
                    break

                for job in jobs_dict.get("jobs", []):
                    curr_job = job.get("job", {})
                    fetch_and_update_airbyte_job_details(str(curr_job.get("id")))

                offset += limit
                curr_itr_count = jobs_dict.get("totalJobCount", 0)

        except Exception as e:
            logger.error(
                "Failed to sync job history for connection %s: %s",
                orgtask_connection_id,
                str(e),
            )
