"""
functions which work with airbyte and with the dalgo database
"""

import json
from django.utils.text import slugify
from django.conf import settings
from django.db import transaction
from django.db.models import Prefetch

from ddpui.ddpairbyte import airbyte_service
from ddpui.ddpairbyte.schema import AirbyteConnectionSchemaUpdate, AirbyteWorkspace
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgSchemaChange
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.custom_logger import CustomLogger
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
)
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema3,
    PrefectDataFlowUpdateSchema3,
)
from ddpui.ddpprefect import AIRBYTESERVER
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.models.org import OrgDataFlowv1, OrgWarehouse
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask
from ddpui.utils.constants import TASK_AIRBYTESYNC, TASK_AIRBYTERESET
from ddpui.utils.helpers import generate_hash_id, update_dict_but_not_stars
from ddpui.utils import secretsmanager
from ddpui.assets.whitelist import DEMO_WHITELIST_SOURCES
from ddpui.core.pipelinefunctions import setup_airbyte_sync_task_config
from ddpui.core.orgtaskfunctions import fetch_orgtask_lock, fetch_orgtask_lock_v1
from ddpui.models.tasks import TaskLock

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
                if (
                    source_def["dockerImageTag"]
                    < custom_source_info["docker_image_tag"]
                ):
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
        airbyte_server_block_id = prefect_service.get_airbyte_server_block_id(
            block_name
        )
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
            raise Exception(
                "could not create orgprefectblock for airbyte-server"
            ) from error

    return AirbyteWorkspace(
        name=workspace["name"],
        workspaceId=workspace["workspaceId"],
        initialSetupComplete=workspace["initialSetupComplete"],
    )


def create_airbyte_deployment(
    org: Org, org_task: OrgTask, server_block: OrgPrefectBlockv1
):
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
                    "tasks": [
                        setup_airbyte_sync_task_config(org_task, server_block).to_json()
                    ],
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

    reset_task = Task.objects.filter(slug=TASK_AIRBYTERESET).first()
    if reset_task is None:
        return None, "reset task not supported"

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

            # create reset connection deplpyment & dataflow
            org_task = OrgTask.objects.create(
                org=org, task=reset_task, connection_id=airbyte_conn["connectionId"]
            )

            reset_dataflow: OrgDataFlowv1 = create_airbyte_deployment(
                org, org_task, org_airbyte_server_block
            )

            sync_dataflow.reset_conn_dataflow = reset_dataflow
            sync_dataflow.save()

    except Exception as err:
        # delete the airbyte connection; since the deployment didn't get created
        logger.info("deleting airbyte connection")
        airbyte_service.delete_connection(
            org.airbyte_workspace_id, airbyte_conn["connectionId"]
        )
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
        "resetConnDeploymentId": reset_dataflow.deployment_id,
    }
    return res, None


def get_connections(org: Org):
    """return connections with last run details"""

    sync_dataflows = (
        OrgDataFlowv1.objects.filter(
            org=org,
            dataflow_type="manual",
            reset_conn_dataflow_id__isnull=False
        )
        .select_related("reset_conn_dataflow")
        .prefetch_related(
            Prefetch(
                "datafloworgtasks",
                queryset=DataflowOrgTask.objects.all()
                .select_related("orgtask")
                .prefetch_related(
                    Prefetch("orgtask__tasklock", queryset=TaskLock.objects.all()),
                ),
            )
        )
    )

    warehouse = OrgWarehouse.objects.filter(org=org).first()

    airbyte_connections = airbyte_service.get_webbackend_connections(
        org.airbyte_workspace_id
    )

    res = []

    for sync_dataflow in sync_dataflows:
        org_tasks: list[OrgTask] = [
            dataflow_orgtask.orgtask
            for dataflow_orgtask in sync_dataflow.datafloworgtasks.all()
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

        # find the airbyte connection
        connection = [
            conn
            for conn in airbyte_connections
            if conn["connectionId"] == org_task.connection_id
        ]
        if len(connection) == 0:
            logger.error(
                f"could not find connection {org_task.connection_id} in airbyte"
            )
            continue
        connection = connection[0]

        last_runs = []
        for df_orgtask in DataflowOrgTask.objects.filter(
            orgtask=org_task,
        ):
            # if dataflow_last_run is not preset; fetch from prefect
            run = (
                PrefectFlowRun.objects.filter(
                    deployment_id=df_orgtask.dataflow.deployment_id
                )
                .order_by("-start_time")
                .first()
            )
            if run:
                last_runs.append(run.to_json())

        last_runs.sort(
            key=lambda run: (
                run["startTime"] if run["startTime"] else run["expectedStartTime"]
            )
        )

        reset_dataflow: OrgDataFlowv1 = sync_dataflow.reset_conn_dataflow

        lock = None

        sync_lock = None
        for dataflow_orgtask in sync_dataflow.datafloworgtasks.all():
            orgtask = dataflow_orgtask.orgtask
            if hasattr(orgtask, "tasklock"):
                sync_lock = orgtask.tasklock
                break

        if sync_lock:
            lock = fetch_orgtask_lock_v1(org_task, sync_lock)

        if not lock and reset_dataflow:
            for dataflow_orgtask in reset_dataflow.datafloworgtasks.all():
                orgtask = dataflow_orgtask.orgtask
                if hasattr(orgtask, "tasklock"):
                    reset_lock = orgtask.tasklock
                    lock = fetch_orgtask_lock_v1(org_task, reset_lock)
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
                "lastRun": last_runs[-1] if len(last_runs) > 0 else None,
                "lock": lock,  # this will have the status of the flow run
                "resetConnDeploymentId": (
                    reset_dataflow.deployment_id if reset_dataflow else None
                ),
            }
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
    airbyte_conn = airbyte_service.get_connection(
        org.airbyte_workspace_id, org_task.connection_id
    )

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
        reset_dataflow_orgtask = DataflowOrgTask.objects.filter(
            dataflow=reset_dataflow
        ).first()

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
            dataflow_orgtask.dataflow.deployment_id
            if dataflow_orgtask.dataflow
            else None
        ),
        "lock": lock,
        "resetConnDeploymentId": (
            reset_dataflow.deployment_id if reset_dataflow else None
        ),
    }

    return res, None


def reset_connection(org: Org, connection_id: str):
    """reset an airbyte connection"""
    if not OrgTask.objects.filter(
        org=org,
        connection_id=connection_id,
    ).exists():
        return None, "connection not found"

    airbyte_service.reset_connection(connection_id)

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
    res = airbyte_service.update_connection(
        org.airbyte_workspace_id, payload, connection
    )

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
            deployment = prefect_service.get_deployment(
                dataflow_orgtask.dataflow.deployment_id
            )
            # { name, deploymentId, tags, cron, isScheduleActive, parameters }
            # parameters = {config: {org_slug, tasks}}
            # tasks = list of
            #    {seq, slug, type, timeout, orgtask__uuid, connection_id, airbyte_server_block}
            parameters = deployment["parameters"]
            # logger.info(parameters)
            for task in parameters["config"]["tasks"]:
                if task["connection_id"] == connection_id:
                    logger.info(f"deleting task {task['slug']} from deployment")
                    parameters["config"]["tasks"].remove(task)
            # logger.info(parameters)
            payload = PrefectDataFlowUpdateSchema3(deployment_params=parameters)
            prefect_service.update_dataflow_v1(
                dataflow_orgtask.dataflow.deployment_id, payload
            )
            logger.info(
                "updated deployment %s", dataflow_orgtask.dataflow.deployment_name
            )

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
    result = airbyte_service.get_jobs_for_connection(connection_id, limit, offset)
    res["totalSyncs"] = result["totalJobCount"]
    if len(result["jobs"]) == 0:
        return [], None

    for job in result["jobs"]:
        job_info = airbyte_service.parse_job_info(job)

        res["history"].append(job_info)

    return res, None


def update_destination(
    org: Org, destination_id: str, payload: AirbyteDestinationUpdate
):
    """updates an airbyte destination and dbt cli profile if credentials changed"""
    destination = airbyte_service.update_destination(
        destination_id, payload.name, payload.config, payload.destinationDefId
    )
    logger.info("updated destination having id " + destination["destinationId"])
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if warehouse.name != payload.name:
        warehouse.name = payload.name
        warehouse.save()

    dbt_credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    if warehouse.wtype == "postgres":
        dbt_credentials = update_dict_but_not_stars(payload.config)
        # i've forgotten why we have this here, airbyte sends "database" - RC
        if "dbname" in dbt_credentials:
            dbt_credentials["database"] = dbt_credentials["dbname"]

    elif warehouse.wtype == "bigquery":
        dbt_credentials = json.loads(payload.config["credentials_json"])
    elif warehouse.wtype == "snowflake":
        if (
            "credentials" in payload.config
            and "password" in payload.config["credentials"]
            and isinstance(payload.config["credentials"]["password"], str)
            and len(payload.config["credentials"]["password"]) > 0
            and list(set(payload.config["credentials"]["password"])) != "*"
        ):
            dbt_credentials["credentials"]["password"] = payload.config["credentials"][
                "password"
            ]

    else:
        return None, "unknown warehouse type " + warehouse.wtype

    secretsmanager.update_warehouse_credentials(warehouse, dbt_credentials)

    cli_profile_block = OrgPrefectBlockv1.objects.filter(
        org=org, block_type=DBTCLIPROFILE
    ).first()

    if cli_profile_block:
        logger.info(f"Updating the cli profile block : {cli_profile_block.block_name}")
        prefect_service.update_dbt_cli_profile_block(
            block_name=cli_profile_block.block_name,
            wtype=warehouse.wtype,
            credentials=dbt_credentials,
            bqlocation=(
                payload.config["dataset_location"]
                if "dataset_location" in payload.config
                else None
            ),
        )
        logger.info(
            f"Successfully updated the cli profile block : {cli_profile_block.block_name}"
        )

    return destination, None


def get_demo_whitelisted_source_config(type: str):
    """Returns the config of whitelisted source based on type"""
    ret_src = None
    for src in DEMO_WHITELIST_SOURCES:
        if src["type"] == type:
            ret_src = src
            break

    if not ret_src:
        return ret_src, "source not found"

    return ret_src["config"], None


def delete_source(org: Org, source_id: str):
    """deletes an airbyte source and related connections"""
    connections = airbyte_service.get_connections(org.airbyte_workspace_id)[
        "connections"
    ]
    connections_of_source = [
        conn["connectionId"] for conn in connections if conn["sourceId"] == source_id
    ]

    # Fetch org tasks and deployments mapped to each connection_id
    org_tasks = OrgTask.objects.filter(
        org=org, connection_id__in=connections_of_source
    ).all()

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


def get_connection_catalog(org: Org, connection_id: str):
    """
    Get the catalog diff of a connection.
    """
    try:
        connection = airbyte_service.get_connection_catalog(connection_id)
        schema_change = connection.get("schemaChange")

        if not schema_change or schema_change not in ["breaking", "non_breaking"]:
            OrgSchemaChange.objects.filter(connection_id=connection_id).delete()
        else:
            OrgSchemaChange.objects.update_or_create(
                connection_id=connection_id,
                defaults={"change_type": schema_change, "org": org},
            )

        res = {
            "name": connection["name"],
            "connectionId": connection["connectionId"],
            "catalogId": connection["catalogId"],
            "syncCatalog": connection["syncCatalog"],
            "schemaChange": schema_change,
            "catalogDiff": connection["catalogDiff"],
        }
        return res, None
    except Exception as e:
        logger.error(f"Error getting catalog for connection {connection_id}: {e}")
        return None, f"Error getting catalog for connection {connection_id}: {e}"


def update_connection_schema(
    org: Org, connection_id: str, payload: AirbyteConnectionSchemaUpdate
):
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
    org_schema_change = OrgSchemaChange.objects.filter(org=org)

    if org_schema_change is None:
        return None, "No schema change found"

    schema_changes = list(org_schema_change.values())
    return schema_changes, None
