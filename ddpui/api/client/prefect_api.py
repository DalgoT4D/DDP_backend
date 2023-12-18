import os
from pathlib import Path
from datetime import datetime
import yaml

from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from django.utils.text import slugify

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.ddpprefect import (
    DBTCORE,
    SHELLOPERATION,
    DBTCLIPROFILE,
    SECRET,
    AIRBYTECONNECTION,
    AIRBYTESERVER,
)
from ddpui.models.org import (
    OrgPrefectBlock,
    OrgWarehouse,
    OrgDataFlow,
    OrgDataFlowv1,
    OrgPrefectBlockv1,
)
from ddpui.models.orgjobs import BlockLock, DataflowBlock
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import Task, DataflowOrgTask, OrgTask
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    PrefectDbtTaskSetup,
    PrefectDataFlowCreateSchema,
    PrefectDataFlowCreateSchema2,
    PrefectDataFlowCreateSchema3,
    PrefectFlowRunSchema,
    PrefectDataFlowUpdateSchema,
    PrefectDataFlowUpdateSchema2,
    PrefectDataFlowUpdateSchema3,
    PrefectShellSetup,
    PrefectSecretBlockCreate,
    PrefectShellTaskSetup,
    PrefectDataFlowCreateSchema4,
)

from ddpui.utils.deploymentblocks import write_dataflowblocks
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_GITPULL,
    TRANSFORM_TASKS_SEQ,
    AIRBYTE_SYNC_TIMEOUT,
    TASK_AIRBYTESYNC,
)
from ddpui.utils.prefectlogs import parse_prefect_logs
from ddpui.utils.helpers import generate_hash_id

prefectapi = NinjaAPI(urls_namespace="prefect")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


@prefectapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@prefectapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@prefectapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.info(exc)
    return Response({"detail": "something went wrong"}, status=500)


@prefectapi.post("/flows/", auth=auth.CanManagePipelines())
def post_prefect_dataflow(request, payload: PrefectDataFlowCreateSchema):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if payload.name in [None, ""]:
        raise HttpError(400, "must provide a name for the flow")

    name_components = [orguser.org.slug]

    # check if pipeline has airbyte syncs
    if len(payload.connectionBlocks) > 0:
        name_components.append("airbyte")

    # check if pipeline has dbt transformation
    dbt_blocks = []
    if payload.dbtTransform == "yes":
        name_components.append("dbt")
        for dbt_block in OrgPrefectBlock.objects.filter(
            org=orguser.org, block_type__in=[DBTCORE, SHELLOPERATION]
        ).order_by("seq"):
            dbt_blocks.append(
                {
                    "blockName": dbt_block.block_name,
                    "seq": dbt_block.seq,
                    "blockType": dbt_block.block_type,
                }
            )

    # fetch all deployment names to compute a unique one
    deployment_names = []
    for orgdataflow in OrgDataFlow.objects.filter(org=orguser.org):
        deployment_names.append(orgdataflow.deployment_name)

    # deployment name should be unique
    name_index = 0
    base_deployment_name = "-".join(name_components + ["deployment"])
    deployment_name = base_deployment_name
    while deployment_name in deployment_names:
        name_index += 1
        deployment_name = base_deployment_name + f"-{name_index}"

    flow_name = "-".join(name_components + ["flow"])

    try:
        res = prefect_service.create_dataflow(
            PrefectDataFlowCreateSchema2(
                deployment_name=deployment_name,
                flow_name=flow_name,
                orgslug=orguser.org.slug,
                connection_blocks=payload.connectionBlocks,
                dbt_blocks=dbt_blocks,
                cron=payload.cron,
            )
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to create a dataflow") from error

    org_data_flow = OrgDataFlow.objects.create(
        org=orguser.org,
        name=payload.name,
        deployment_name=res["deployment"]["name"],
        deployment_id=res["deployment"]["id"],
        cron=payload.cron,
        dataflow_type="orchestrate",
    )

    write_dataflowblocks(org_data_flow)

    return {
        "deploymentId": org_data_flow.deployment_id,
        "name": org_data_flow.name,
        "cron": org_data_flow.cron,
    }


@prefectapi.get("/flows/", auth=auth.CanManagePipelines())
def get_prefect_dataflows(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = OrgDataFlow.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
    ).all()

    deployment_ids = [flow.deployment_id for flow in org_data_flows]

    # dictionary to hold {"id": status}
    is_deployment_active = {}

    # setting active/inactive status based on if the schedule is set or not
    for deployment in prefect_service.get_filtered_deployments(
        orguser.org.slug, deployment_ids
    ):
        is_deployment_active[deployment["deploymentId"]] = (
            deployment["isScheduleActive"]
            if "isScheduleActive" in deployment
            else False
        )

    res = []

    for flow in org_data_flows:
        block_ids = DataflowBlock.objects.filter(dataflow=flow).values("opb__block_id")
        # if there is one there will typically be several - a sync,
        # a git-run, a git-test... we return the userinfo only for the first one
        lock = BlockLock.objects.filter(
            opb__block_id__in=[x["opb__block_id"] for x in block_ids]
        ).first()
        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "lastRun": prefect_service.get_last_flow_run_by_deployment_id(
                    flow.deployment_id
                ),
                "status": is_deployment_active[flow.deployment_id]
                if flow.deployment_id in is_deployment_active
                else False,
                "lock": {
                    "lockedBy": lock.locked_by.user.email,
                    "lockedAt": lock.locked_at,
                }
                if lock
                else None,
            }
        )

    return res


@prefectapi.put("/flows/{deployment_id}", auth=auth.CanManagePipelines())
def put_prefect_dataflow(request, deployment_id, payload: PrefectDataFlowUpdateSchema):
    """Edit the data flow / prefect deployment. For now only the schedules can be edited"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flow = OrgDataFlow.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if org_data_flow is None:
        raise HttpError(404, "Pipeline not found")

    # check if pipeline has dbt transformation
    dbt_blocks = []
    if payload.dbtTransform == "yes":
        for dbt_block in OrgPrefectBlock.objects.filter(
            org=orguser.org, block_type__in=[DBTCORE, SHELLOPERATION]
        ).order_by("seq"):
            dbt_blocks.append(
                {
                    "blockName": dbt_block.block_name,
                    "seq": dbt_block.seq,
                    "blockType": dbt_block.block_type,
                }
            )

    prefect_service.update_dataflow(
        deployment_id,
        PrefectDataFlowUpdateSchema2(
            dbt_blocks=dbt_blocks,
            connection_blocks=payload.connectionBlocks,
            cron=payload.cron,
        ),
    )

    org_data_flow.cron = payload.cron if payload.cron else None
    org_data_flow.name = payload.name
    org_data_flow.save()

    return {"success": 1}


@prefectapi.get("/flows/{deployment_id}", auth=auth.CanManagePipelines())
def get_prefect_dataflow(request, deployment_id):
    """Fetch details of prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # remove the org data flow
    org_data_flow = OrgDataFlow.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if org_data_flow is None:
        raise HttpError(404, "flow does not exist")

    try:
        res = prefect_service.get_deployment(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to get deploymenet from prefect-proxy") from error

    if "parameters" in res and "airbyte_blocks" in res["parameters"]:
        for airbyte_block in res["parameters"]["airbyte_blocks"]:
            conn = airbyte_service.get_connection(
                orguser.org.airbyte_workspace_id, airbyte_block["connectionId"]
            )
            airbyte_block["name"] = conn["name"]

    # differentiate between deploymentName and name
    res["deploymentName"] = res["name"]
    res["name"] = org_data_flow.name

    return res


@prefectapi.delete("/flows/{deployment_id}", auth=auth.CanManagePipelines())
def delete_prefect_dataflow(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    prefect_service.delete_deployment_by_id(deployment_id)

    # remove the org data flow
    org_data_flow = OrgDataFlow.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if org_data_flow:
        org_data_flow.delete()

    return {"success": 1}


@prefectapi.post("/flows/{deployment_id}/flow_run", auth=auth.CanManagePipelines())
def post_prefect_dataflow_quick_run(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    locks = prefect_service.lock_blocks_for_deployment(deployment_id, orguser)

    try:
        res = prefect_service.create_deployment_flow_run(deployment_id)
    except Exception as error:
        logger.exception(error)
        for blocklock in locks:
            logger.info("deleting BlockLock %s", blocklock.opb.block_name)
            blocklock.delete()
        raise HttpError(400, "failed to start a run") from error

    for blocklock in locks:
        blocklock.flow_run_id = res["flow_run_id"]
        blocklock.save()

    return res


@prefectapi.post(
    "/flows/{deployment_id}/set_schedule/{status}", auth=auth.CanManagePipelines()
)
def post_deployment_set_schedule(request, deployment_id, status):
    """Set deployment schedule to active / inactive"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if (
        (status is None)
        or (isinstance(status, str) is not True)
        or (status not in ["active", "inactive"])
    ):
        raise HttpError(422, "incorrect status value")

    try:
        prefect_service.set_deployment_schedule(deployment_id, status)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to change flow state") from error
    return {"success": 1}


@prefectapi.post("/flows/airbyte_sync/", auth=auth.CanManagePipelines())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Run airbyte sync flow in prefect"""
    orguser: OrgUser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if payload.flowName is None:
        payload.flowName = f"{orguser.org.name}-airbytesync"
    if payload.flowRunName is None:
        now = timezone.as_ist(datetime.now())
        payload.flowRunName = f"{now.isoformat()}"
    try:
        result = prefect_service.run_airbyte_connection_sync(payload)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to run sync") from error
    return result


@prefectapi.post("/flows/dbt_run/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_run_flow(
    request, payload: PrefectDbtCore
):  # pylint: disable=unused-argument
    """Run dbt flow in prefect"""
    orguser: OrgUser = request.orguser

    orgprefectblock = OrgPrefectBlock.objects.filter(
        org=orguser.org, block_name=payload.blockName
    ).first()
    if orgprefectblock is None:
        logger.error("block name %s not found", payload.blockName)
        raise HttpError(400, "block name not found")

    blocklock = BlockLock.objects.filter(opb=orgprefectblock).first()
    if blocklock:
        raise HttpError(
            400, f"{blocklock.locked_by.user.email} is running this operation"
        )

    try:
        blocklock = BlockLock.objects.create(opb=orgprefectblock, locked_by=orguser)
    except Exception as error:
        raise HttpError(400, "someone else is running this operation") from error

    dbt = orguser.org.dbt
    profile_file = Path(dbt.project_dir) / "dbtrepo/profiles/profiles.yml"
    if os.path.exists(profile_file):
        os.unlink(profile_file)

    if payload.flowName is None:
        payload.flowName = f"{orguser.org.name}-dbt"
    if payload.flowRunName is None:
        now = timezone.as_ist(datetime.now())
        payload.flowRunName = f"{now.isoformat()}"

    # save into some table
    try:
        result = prefect_service.run_dbt_core_sync(payload)
    except Exception as error:
        blocklock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to run dbt") from error

    blocklock.delete()
    logger.info("released lock on block %s", payload.blockName)
    return result


@prefectapi.post("/blocks/dbt/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_block(request):
    """Create five prefect dbt core blocks:
    - dbt clean
    - dbt deps
    - dbt run
    - dbt test
    - dbt docs generate
    and Create one shell block to do git pull
    - git pull
    for a ddp-dbt-profile
    """
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    if orguser.org.dbt.dbt_venv is None:
        orguser.org.dbt.dbt_venv = os.getenv("DBT_VENV")
        orguser.org.dbt.save()

    dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
    if not dbt_env_dir.exists():
        raise HttpError(400, "create the dbt env first")

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
    project_dir = str(dbtrepodir)
    dbt_project_filename = str(dbtrepodir / "dbt_project.yml")

    if not os.path.exists(dbt_project_filename):
        raise HttpError(400, dbt_project_filename + " is missing")

    # create the git pull shell block
    try:
        gitrepo_access_token = secretsmanager.retrieve_github_token(orguser.org.dbt)
        gitrepo_url = orguser.org.dbt.gitrepo_url
        command = "git pull"

        # make sure this key is always present in the env of git pull shell command
        shell_env = {"secret-git-pull-url-block": ""}

        if gitrepo_access_token is not None:
            gitrepo_url = gitrepo_url.replace(
                "github.com", "oauth2:" + gitrepo_access_token + "@github.com"
            )

            # store the git oauth endpoint with token in a prefect secret block
            secret_block = PrefectSecretBlockCreate(
                block_name=f"{orguser.org.slug}-git-pull-url",
                secret=gitrepo_url,
            )
            block_response = prefect_service.create_secret_block(secret_block)

            # store the prefect secret block in the git pull shell command
            shell_env["secret-git-pull-url-block"] = block_response["block_name"]

        block_name = f"{orguser.org.slug}-" f"{slugify(command)}"
        shell_cmd = PrefectShellSetup(
            blockname=block_name,
            commands=[command],
            workingDir=project_dir,
            env=shell_env,
        )
        block_response = prefect_service.create_shell_block(shell_cmd)

        # store prefect shell block in database
        shellprefectblock = OrgPrefectBlock(
            org=orguser.org,
            block_type=SHELLOPERATION,
            block_id=block_response["block_id"],
            block_name=block_response["block_name"],
            display_name=block_name,
            seq=0,
            command=slugify(command),
        )
        shellprefectblock.save()

    except Exception as error:
        logger.exception(error)
        raise HttpError(400, str(error)) from error

    with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
        dbt_project = yaml.safe_load(dbt_project_file)
        if "profile" not in dbt_project:
            raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

    profile_name = dbt_project["profile"]
    target = orguser.org.dbt.default_schema
    logger.info("profile_name=%s target=%s", profile_name, target)

    # get the bigquery location if warehouse is bq
    bqlocation = None
    if warehouse.wtype == "bigquery":
        destination = airbyte_service.get_destination(
            orguser.org.airbyte_workspace_id, warehouse.airbyte_destination_id
        )
        if destination.get("connectionConfiguration"):
            bqlocation = destination["connectionConfiguration"]["dataset_location"]

    block_names = []
    for sequence_number, command in enumerate(
        ["clean", "deps", "run", "test", "docs generate"]
    ):
        block_name = (
            f"{orguser.org.slug}-"
            f"{slugify(profile_name)}-"
            f"{slugify(target)}-"
            f"{slugify(command)}"
        )
        block_data = PrefectDbtCoreSetup(
            block_name=block_name,
            profiles_dir=f"{project_dir}/profiles/",
            project_dir=project_dir,
            working_dir=project_dir,
            env={},
            commands=[f"{dbt_binary} {command} --target {target}"],
        )

        try:
            block_response = prefect_service.create_dbt_core_block(
                block_data,
                profile_name,
                f"{orguser.org.slug}_{profile_name}",
                target,
                warehouse.wtype,
                credentials,
                bqlocation,
            )
        except Exception as error:
            logger.exception(error)
            raise HttpError(400, str(error)) from error

        coreprefectblock = OrgPrefectBlock(
            org=orguser.org,
            block_type=DBTCORE,
            block_id=block_response["block_id"],
            block_name=block_response["block_name"],
            display_name=block_name,
            seq=sequence_number
            + 1,  # shell command would be at zero, dbt commands starts from 1
            command=slugify(command),
            dbt_target_schema=target,
        )

        # cleaned name from the prefect-proxy
        block_name = block_response["block_name"]

        # for the command dbt run create a deployment
        if command == "run":
            # create deployment
            dataflow = prefect_service.create_dataflow(
                PrefectDataFlowCreateSchema2(
                    deployment_name=f"manual-run-{block_name}",
                    flow_name=f"manual-run-{block_name}",
                    orgslug=orguser.org.slug,
                    connection_blocks=[],
                    dbt_blocks=[
                        {"blockName": block_name, "blockType": DBTCORE, "seq": 0}
                    ],
                )
            )

            # store deployment record in django db
            existing_dataflow = OrgDataFlow.objects.filter(
                deployment_id=dataflow["deployment"]["id"]
            ).first()
            if existing_dataflow:
                existing_dataflow.delete()

            manual_run_dataflow = OrgDataFlow.objects.create(
                org=orguser.org,
                name=f"manual-run-{block_name}",
                deployment_name=dataflow["deployment"]["name"],
                deployment_id=dataflow["deployment"]["id"],
                dataflow_type="manual",
            )
            write_dataflowblocks(manual_run_dataflow)

        coreprefectblock.save()
        block_names.append(block_name)

    return {"success": 1, "block_names": block_names}


@prefectapi.get("/blocks/dbt/", auth=auth.CanManagePipelines())
def get_prefect_dbt_run_blocks(request):
    """Fetch all prefect dbt run blocks for an organization"""
    orguser: OrgUser = request.orguser

    blocks = []

    for prefect_block in OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=DBTCORE
    ):
        # is the block currently locked?
        lock = BlockLock.objects.filter(opb=prefect_block).first()
        block = {
            "blockType": prefect_block.block_type,
            "blockId": prefect_block.block_id,
            "blockName": prefect_block.block_name,
            "action": prefect_block.command,
            "target": prefect_block.dbt_target_schema,
            "deploymentId": None,
            "lock": {
                "lockedBy": lock.locked_by.user.email,
                "lockedAt": lock.locked_at,
            }
            if lock
            else None,
        }

        # fetch the manual deploymentId for the dbt run block
        if prefect_block.block_type == DBTCORE and prefect_block.command == "run":
            dataflow = OrgDataFlow.objects.filter(
                org=orguser.org, cron=None, connection_id=None
            ).first()
            block["deploymentId"] = dataflow.deployment_id if dataflow else None

        blocks.append(block)

    return blocks


@prefectapi.delete("/blocks/dbt/", auth=auth.CanManagePipelines())
def delete_prefect_dbt_run_block(request):
    """Delete prefect dbt run block for an organization"""
    orguser: OrgUser = request.orguser

    org_dbt_blocks = OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type__in=[DBTCORE, SHELLOPERATION]
    ).all()

    for dbt_block in org_dbt_blocks:
        # Delete block in prefect
        try:
            if dbt_block.block_type == DBTCORE:
                prefect_service.delete_dbt_core_block(dbt_block.block_id)

            if dbt_block.block_type == SHELLOPERATION:
                prefect_service.delete_shell_block(dbt_block.block_id)

        except Exception as error:
            logger.exception(error)
            # may have deleted the block via the prefect ui, continue

        # Delete block row from database
        dbt_block.delete()

        # For the run block, also delete the manual deployment for this
        if dbt_block.command == "run":
            while True:
                dataflow = OrgDataFlow.objects.filter(
                    org=orguser.org, cron=None, connection_id=None
                ).first()
                if dataflow:
                    logger.info("deleting manual deployment for dbt run")
                    # do this in try catch because it can fail & throw error
                    try:
                        prefect_service.delete_deployment_by_id(dataflow.deployment_id)
                    except Exception:  # skipcq: PYL-W0703
                        logger.exception("could not delete prefect deployment")
                        continue

                    # delete manual dbt run deployment
                    dataflow.delete()
                    logger.info("FINISHED deleting manual deployment for dbt run")
                else:
                    break

    return {"success": 1}


# =================================================================================================


@prefectapi.get("/flow_runs/{flow_run_id}/logs", auth=auth.CanManagePipelines())
def get_flow_runs_logs(
    request, flow_run_id, offset: int = 0
):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    try:
        result = prefect_service.get_flow_run_logs(flow_run_id, offset)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return result


@prefectapi.get("/flow_runs/{flow_run_id}/logsummary", auth=auth.CanManagePipelines())
def get_flow_runs_logsummary(request, flow_run_id):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    try:
        connection_info = {
            "host": os.getenv("PREFECT_HOST"),
            "port": os.getenv("PREFECT_PORT"),
            "database": os.getenv("PREFECT_DB"),
            "user": os.getenv("PREFECT_USER"),
            "password": os.getenv("PREFECT_PASSWORD"),
        }
        result = parse_prefect_logs(connection_info, flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return result


@prefectapi.get(
    "/flow_runs/{flow_run_id}",
    auth=auth.CanManagePipelines(),
    response=PrefectFlowRunSchema,
)
def get_flow_run_by_id(request, flow_run_id):
    # pylint: disable=unused-argument
    """fetch a flow run from prefect"""
    try:
        flow_run = prefect_service.get_flow_run(flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to retrieve logs") from error
    return flow_run


@prefectapi.get(
    "/flows/{deployment_id}/flow_runs/history", auth=auth.CanManagePipelines()
)
def get_prefect_flow_runs_log_history(
    request, deployment_id, limit: int = 0, fetchlogs=True
):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    flow_runs = prefect_service.get_flow_runs_by_deployment_id(
        deployment_id, limit=limit
    )

    if fetchlogs:
        for flow_run in flow_runs:
            logs_dict = prefect_service.get_flow_run_logs(flow_run["id"], 0)
            flow_run["logs"] = (
                logs_dict["logs"]["logs"] if "logs" in logs_dict["logs"] else []
            )

    return flow_runs


# =================================================================================================
# new apis to go away from the block architecture


@prefectapi.post("/tasks/{orgtask_id}/run/", auth=auth.CanManagePipelines())
def post_run_prefect_org_task(request, orgtask_id):  # pylint: disable=unused-argument
    """
    Run dbt task & git pull in prefect. All tasks without a deployment.
    Basically short running tasks
    Can run
        - git pull
        - dbt deps
        - dbt clean
        - dbt test
    """
    orguser: OrgUser = request.orguser

    org_task = OrgTask.objects.filter(org=orguser.org, id=orgtask_id).first()

    if org_task is None:
        raise HttpError(400, "task not found")

    if org_task.task.type not in ["dbt", "git"]:
        raise HttpError(400, "task not supported")

    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
    project_dir = str(dbtrepodir)

    # TODO: add task lock logic

    if org_task.task.slug == TASK_GITPULL:
        shell_env = {"secret-git-pull-url-block": ""}

        gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
        ).first()

        if gitpull_secret_block is not None:
            shell_env["secret-git-pull-url-block"] = gitpull_secret_block.block_name

        payload = PrefectShellTaskSetup(
            commands=["git pull"],
            working_dir=project_dir,
            env=shell_env,
            slug=org_task.task.slug,
            type=SHELLOPERATION,
        )

        if payload.flow_name is None:
            payload.flow_name = f"{orguser.org.name}-gitpull"
        if payload.flow_run_name is None:
            now = timezone.as_ist(datetime.now())
            payload.flow_run_name = f"{now.isoformat()}"

        try:
            result = prefect_service.run_shell_task_sync(payload)
        except Exception as error:
            logger.exception(error)
            raise HttpError(
                400, f"failed to run the shell task {org_task.task.slug}"
            ) from error
    else:
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
        target = orguser.org.dbt.default_schema

        # fetch the cli profile block
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()

        if cli_profile_block is None:
            raise HttpError(400, "dbt cli profile block not found")

        payload = PrefectDbtTaskSetup(
            slug=org_task.task.slug,
            commands=[f"{dbt_binary} {org_task.task.command} --target {target}"],
            env={},
            working_dir=project_dir,
            profiles_dir=f"{project_dir}/profiles/",
            project_dir=project_dir,
            cli_profile_block=cli_profile_block.block_name,
            cli_args=[],
            type=DBTCORE,
        )

        if payload.flow_name is None:
            payload.flow_name = f"{orguser.org.name}-{org_task.task.slug}"
        if payload.flow_run_name is None:
            now = timezone.as_ist(datetime.now())
            payload.flow_run_name = f"{now.isoformat()}"

        try:
            result = prefect_service.run_dbt_task_sync(payload)
        except Exception as error:
            logger.exception(error)
            raise HttpError(400, "failed to run dbt") from error

    return result


@prefectapi.post("/v1/flows/{deployment_id}/flow_run/", auth=auth.CanManagePipelines())
def post_run_prefect_org_deployment_task(request, deployment_id):
    """
    Run deployment based task.
    Can run
        - airbtye sync
        - dbt run
    """
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    dataflow_orgtask = DataflowOrgTask.objects.filter(
        dataflow__deployment_id=deployment_id
    ).first()

    if dataflow_orgtask is None:
        raise HttpError(400, "no org task mapped to the deployment")

    # TODO: add task lock logic
    try:
        res = prefect_service.create_deployment_flow_run(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error

    return res


@prefectapi.post("/tasks/transform/", auth=auth.CanManagePipelines())
def post_prefect_transformation_tasks(request):
    """
    - Create a git pull url secret block
    - Create a dbt cli profile block
    - Create dbt tasks
        - git pull
        - dbt deps
        - dbt clean
        - dbt run
        - dbt test
    """
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    if orguser.org.dbt.dbt_venv is None:
        orguser.org.dbt.dbt_venv = os.getenv("DBT_VENV")
        orguser.org.dbt.save()

    dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
    if not dbt_env_dir.exists():
        raise HttpError(400, "create the dbt env first")

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
    project_dir = str(dbtrepodir)
    dbt_project_filename = str(dbtrepodir / "dbt_project.yml")

    if not os.path.exists(dbt_project_filename):
        raise HttpError(400, dbt_project_filename + " is missing")

    # create a secret block to save the github endpoint url along with token
    try:
        gitrepo_access_token = secretsmanager.retrieve_github_token(orguser.org.dbt)
        gitrepo_url = orguser.org.dbt.gitrepo_url

        if gitrepo_access_token is not None and gitrepo_access_token != "":
            gitrepo_url = gitrepo_url.replace(
                "github.com", "oauth2:" + gitrepo_access_token + "@github.com"
            )

            # store the git oauth endpoint with token in a prefect secret block
            secret_block = PrefectSecretBlockCreate(
                block_name=f"{orguser.org.slug}-git-pull-url",
                secret=gitrepo_url,
            )
            block_response = prefect_service.create_secret_block(secret_block)

            # store secret block name block_response["block_name"] in orgdbt
            OrgPrefectBlockv1.objects.create(
                org=orguser.org,
                block_type=SECRET,
                block_id=block_response["block_id"],
                block_name=block_response["block_name"],
            )

    except Exception as error:
        logger.exception(error)
        raise HttpError(400, str(error)) from error

    with open(dbt_project_filename, "r", encoding="utf-8") as dbt_project_file:
        dbt_project = yaml.safe_load(dbt_project_file)
        if "profile" not in dbt_project:
            raise HttpError(400, "could not find 'profile:' in dbt_project.yml")

    profile_name = dbt_project["profile"]
    target = orguser.org.dbt.default_schema
    logger.info("profile_name=%s target=%s", profile_name, target)

    # get the dataset location if warehouse type is bigquery
    bqlocation = None
    if warehouse.wtype == "bigquery":
        destination = airbyte_service.get_destination(
            orguser.org.airbyte_workspace_id, warehouse.airbyte_destination_id
        )
        if destination.get("connectionConfiguration"):
            bqlocation = destination["connectionConfiguration"]["dataset_location"]

    # create a dbt cli profile block
    try:
        cli_block_name = f"{orguser.org.slug}_{profile_name}"
        cli_block_response = prefect_service.create_dbt_cli_profile_block(
            cli_block_name,
            profile_name,
            target,
            warehouse.wtype,
            bqlocation,
            credentials,
        )

        # save the cli profile block in django db
        OrgPrefectBlockv1.objects.create(
            org=orguser.org,
            block_type=DBTCLIPROFILE,
            block_id=cli_block_response["block_id"],
            block_name=cli_block_response["block_name"],
        )

    except Exception as error:
        logger.exception(error)
        raise HttpError(400, str(error)) from error

    # create org tasks for the transformation page
    for task in Task.objects.filter(type__in=["dbt", "git"]).all():
        org_task = OrgTask.objects.create(org=orguser.org, task=task)

        if task.slug == TASK_DBTRUN:
            # create deployment
            hash_code = generate_hash_id(8)
            deployment_name = f"manual-{orguser.org.slug}-{task.slug}-{hash_code}"
            dataflow = prefect_service.create_dataflow_v1(
                PrefectDataFlowCreateSchema3(
                    deployment_name=deployment_name,
                    flow_name=deployment_name,
                    orgslug=orguser.org.slug,
                    deployment_params={
                        "config": {
                            "tasks": [
                                {
                                    "slug": task.slug,
                                    "type": DBTCORE,
                                    "seq": 1,
                                    "commands": [
                                        f"{dbt_binary} {task.command} --target {target}"
                                    ],
                                    "env": {},
                                    "working_dir": project_dir,
                                    "profiles_dir": f"{project_dir}/profiles/",
                                    "project_dir": project_dir,
                                    "cli_profile_block": cli_block_response[
                                        "block_name"
                                    ],
                                    "cli_args": [],
                                }
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
                org=orguser.org,
                name=deployment_name,
                deployment_name=dataflow["deployment"]["name"],
                deployment_id=dataflow["deployment"]["id"],
                dataflow_type="manual",
            )

            DataflowOrgTask.objects.create(
                dataflow=new_dataflow,
                orgtask=org_task,
            )

    return {"success": 1}


@prefectapi.get("/tasks/transform/", auth=auth.CanManagePipelines())
def get_prefect_transformation_tasks(request):
    """Fetch all dbt tasks for an org"""
    orguser: OrgUser = request.orguser

    org_tasks = []

    for org_task in (
        OrgTask.objects.filter(
            org=orguser.org,
            task__type__in=["git", "dbt"],
        )
        .order_by("task__id")
        .all()
    ):
        # TODO: add task locking logic here later
        org_tasks.append(
            {
                "label": org_task.task.label,
                "slug": org_task.task.slug,
                "id": org_task.id,
                "deploymentId": None,
            }
        )

        # fetch the manual deploymentId for the dbt run task
        if org_task.task.slug == TASK_DBTRUN:
            dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()
            org_tasks[-1]["deploymentId"] = (
                dataflow_orgtask.dataflow.deployment_id if dataflow_orgtask else None
            )

    return org_tasks


@prefectapi.delete("/tasks/transform/", auth=auth.CanManagePipelines())
def delete_prefect_transformation_tasks(request):
    """delete tasks and related objects for an org"""
    orguser: OrgUser = request.orguser

    secret_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org,
        block_type=SECRET,
    ).first()
    if secret_block:
        logger.info("deleting secret block %s", secret_block.block_name)
        prefect_service.delete_secret_block(secret_block.block_id)
        secret_block.delete()

    cli_profile_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org,
        block_type=DBTCLIPROFILE,
    ).first()
    if cli_profile_block:
        logger.info("deleting cli profile block %s", cli_profile_block.block_name)
        prefect_service.delete_dbt_cli_profile_block(cli_profile_block.block_id)
        cli_profile_block.delete()

    org_tasks = OrgTask.objects.filter(org=orguser.org).all()

    for org_task in org_tasks:
        if org_task.task.slug == TASK_DBTRUN:
            dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()
            if dataflow_orgtask:
                # delete the manual deployment for this
                dataflow = dataflow_orgtask.dataflow
                logger.info("deleting manual deployment for dbt run")
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


@prefectapi.post("/v1/flows/", auth=auth.CanManagePipelines())
def post_prefect_dataflow_v1(request, payload: PrefectDataFlowCreateSchema4):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    if payload.name in [None, ""]:
        raise HttpError(400, "must provide a name for the flow")

    seq = 0  # global sequence for all tasks
    tasks = []
    map_org_tasks = []  # map org tasks to dataflow

    # check if pipeline has airbyte syncs
    if len(payload.connections) > 0:
        org_server_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=AIRBYTESERVER
        ).first()
        if not org_server_block:
            raise HttpError(400, "airbyte server block not found")

        # push sync tasks to pipeline
        payload.connections.sort(key=lambda conn: conn.seq)
        for connection in payload.connections:
            logger.info(connection)
            org_task = OrgTask.objects.filter(
                org=orguser.org, connection_id=connection.id
            ).first()
            if org_task is None:
                logger.info(
                    f"connection id {connection.id} not found in org tasks; ignoring this airbyte sync"
                )
                continue
            # map this org task to dataflow
            map_org_tasks.append(org_task)

            logger.info(
                f"connection id {connection.id} found in org tasks; pushing to pipeline"
            )
            seq += 1
            task_config = {
                "seq": seq,
                "slug": org_task.task.slug,
                "type": AIRBYTECONNECTION,
                "airbyte_server_block": org_server_block.block_name,
                "connection_id": connection.id,
                "timeout": AIRBYTE_SYNC_TIMEOUT,
            }
            tasks.append(task_config)

    logger.info(f"Pipline has {seq} airbyte syncs")

    # check if pipeline has dbt transformation
    if payload.dbtTransform == "yes":
        logger.info(f"Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
        dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
        project_dir = str(dbtrepodir)
        target = orguser.org.dbt.default_schema

        # dbt cli profile block
        cli_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_block:
            raise HttpError(400, "dbt cli profile not found")

        # push dbt pipeline tasks
        for org_task in OrgTask.objects.filter(task__type__in=["dbt", "git"]).all():
            logger.info(
                f"found transform task {org_task.task.slug}; pushing to pipeline"
            )
            # map this org task to dataflow
            map_org_tasks.append(org_task)

            dbt_core_task_setup = PrefectDbtTaskSetup(
                seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                slug=org_task.task.slug,
                commands=[f"{dbt_binary} {org_task.task.command} --target {target}"],
                type=DBTCORE,
                env={},
                working_dir=project_dir,
                profiles_dir=f"{project_dir}/profiles/",
                project_dir=project_dir,
                cli_profile_block=cli_block.block_name,
                cli_args=[],
            )

            task_config = dict(dbt_core_task_setup)

            # update task_config its a git pull task
            if org_task.task.slug == TASK_GITPULL:
                shell_env = {"secret-git-pull-url-block": ""}

                gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                    org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
                ).first()

                if gitpull_secret_block is not None:
                    shell_env[
                        "secret-git-pull-url-block"
                    ] = gitpull_secret_block.block_name

                shell_task_setup = PrefectShellTaskSetup(
                    commands=["git pull"],
                    working_dir=project_dir,
                    env=shell_env,
                    slug=org_task.task.slug,
                    type=SHELLOPERATION,
                    seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                )

                task_config = dict(shell_task_setup)

            tasks.append(task_config)
        logger.info(f"Dbt tasks pushed to the pipeline")

    # create deployment
    try:
        hash_code = generate_hash_id(8)
        deployment_name = f"pipeline-{orguser.org.slug}-{hash_code}"
        dataflow = prefect_service.create_dataflow_v1(
            PrefectDataFlowCreateSchema3(
                deployment_name=deployment_name,
                flow_name=deployment_name,
                orgslug=orguser.org.slug,
                deployment_params={"config": {"tasks": tasks}},
                cron=payload.cron,
            )
        )
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to create a pipeline") from error

    org_dataflow = OrgDataFlowv1.objects.create(
        org=orguser.org,
        name=payload.name,
        deployment_name=dataflow["deployment"]["name"],
        deployment_id=dataflow["deployment"]["id"],
        cron=payload.cron,
        dataflow_type="orchestrate",
    )

    for org_task in map_org_tasks:
        DataflowOrgTask.objects.create(dataflow=org_dataflow, orgtask=org_task)

    return {
        "deploymentId": org_dataflow.deployment_id,
        "name": org_dataflow.name,
        "cron": org_dataflow.cron,
    }


@prefectapi.get("/v1/flows/", auth=auth.CanManagePipelines())
def get_prefect_dataflows_v1(request):
    """Fetch all flows/pipelines created in an organization"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = OrgDataFlowv1.objects.filter(
        org=orguser.org, dataflow_type="orchestrate"
    ).all()

    deployment_ids = [flow.deployment_id for flow in org_data_flows]

    # dictionary to hold {"id": status}
    is_deployment_active = {}

    # setting active/inactive status based on if the schedule is set or not
    for deployment in prefect_service.get_filtered_deployments(
        orguser.org.slug, deployment_ids
    ):
        is_deployment_active[deployment["deploymentId"]] = (
            deployment["isScheduleActive"]
            if "isScheduleActive" in deployment
            else False
        )

    res = []

    for flow in org_data_flows:
        # block_ids = DataflowBlock.objects.filter(dataflow=flow).values("opb__block_id")
        # # if there is one there will typically be several - a sync,
        # # a git-run, a git-test... we return the userinfo only for the first one
        # lock = BlockLock.objects.filter(
        #     opb__block_id__in=[x["opb__block_id"] for x in block_ids]
        # ).first()

        # TODO: task lock logic
        lock = None
        res.append(
            {
                "name": flow.name,
                "deploymentId": flow.deployment_id,
                "cron": flow.cron,
                "deploymentName": flow.deployment_name,
                "lastRun": prefect_service.get_last_flow_run_by_deployment_id(
                    flow.deployment_id
                ),
                "status": is_deployment_active[flow.deployment_id]
                if flow.deployment_id in is_deployment_active
                else False,
                "lock": {
                    "lockedBy": lock.locked_by.user.email,
                    "lockedAt": lock.locked_at,
                }
                if lock
                else None,
            }
        )

    return res


@prefectapi.get("/v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
def get_prefect_dataflow_v1(request, deployment_id):
    """Fetch details of prefect deployment"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # remove the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if org_data_flow is None:
        raise HttpError(404, "pipeline does not exist")

    try:
        deployment = prefect_service.get_deployment(deployment_id)
        logger.info(deployment)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to get deploymenet from prefect-proxy") from error

    connections = [
        {"id": task["connection_id"], "seq": task["seq"]}
        for task in deployment["parameters"]["config"]["tasks"]
        if task["type"] == AIRBYTECONNECTION
    ]

    has_transform = (
        len(
            [
                task
                for task in deployment["parameters"]["config"]["tasks"]
                if task["type"] in [DBTCORE, SHELLOPERATION]
            ]
        )
        > 0
    )

    # differentiate between deploymentName and name
    deployment["deploymentName"] = deployment["name"]
    deployment["name"] = org_data_flow.name

    return {
        "name": org_data_flow.name,
        "deploymentName": deployment["deploymentName"],
        "cron": deployment["cron"],
        "connections": connections,
        "dbtTransform": "yes" if has_transform else "no",
        "isScheduleActive": deployment["isScheduleActive"],
    }


@prefectapi.delete("/v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
def delete_prefect_dataflow_v1(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # remove the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if not org_data_flow:
        raise HttpError(404, "pipeline not found")

    prefect_service.delete_deployment_by_id(deployment_id)

    org_data_flow.delete()

    return {"success": 1}


@prefectapi.put("/v1/flows/{deployment_id}", auth=auth.CanManagePipelines())
def put_prefect_dataflow_v1(
    request, deployment_id, payload: PrefectDataFlowUpdateSchema3
):
    """Edit the data flow / prefect deployment. For now only the schedules can be edited"""
    orguser: OrgUser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    # the org data flow
    org_data_flow = OrgDataFlowv1.objects.filter(
        org=orguser.org, deployment_id=deployment_id
    ).first()

    if not org_data_flow:
        raise HttpError(404, "pipeline not found")

    seq = 0  # global sequence for all tasks
    tasks = []
    map_org_tasks = []  # map org tasks to dataflow

    # check if pipeline has airbyte syncs
    if len(payload.connections) > 0:
        org_server_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=AIRBYTESERVER
        ).first()
        if not org_server_block:
            raise HttpError(400, "airbyte server block not found")

        # delete all airbyte sync DataflowOrgTask
        DataflowOrgTask.objects.filter(
            dataflow=org_data_flow, orgtask__task__type="airbyte"
        ).delete()

        # push sync tasks to pipeline
        payload.connections.sort(key=lambda conn: conn.seq)
        for connection in payload.connections:
            logger.info(connection)
            org_task = OrgTask.objects.filter(
                org=orguser.org, connection_id=connection.id
            ).first()
            if org_task is None:
                logger.info(
                    f"connection id {connection.id} not found in org tasks; ignoring this airbyte sync"
                )
                continue
            # map this org task to dataflow
            map_org_tasks.append(org_task)

            logger.info(
                f"connection id {connection.id} found in org tasks; pushing to pipeline"
            )
            seq += 1
            task_config = {
                "seq": seq,
                "slug": org_task.task.slug,
                "type": AIRBYTECONNECTION,
                "airbyte_server_block": org_server_block.block_name,
                "connection_id": connection.id,
                "timeout": AIRBYTE_SYNC_TIMEOUT,
            }
            tasks.append(task_config)

    logger.info(f"Pipline has {seq} airbyte syncs")

    # check if pipeline has dbt transformation
    if payload.dbtTransform == "yes":
        logger.info(f"Dbt tasks being pushed to the pipeline")

        # dbt params
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
        dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug / "dbtrepo"
        project_dir = str(dbtrepodir)
        target = orguser.org.dbt.default_schema

        # dbt cli profile block
        cli_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()
        if not cli_block:
            raise HttpError(400, "dbt cli profile not found")

        # delete all transform related DataflowOrgTask
        DataflowOrgTask.objects.filter(
            dataflow=org_data_flow, orgtask__task__type__in=["dbt", "git"]
        ).delete()

        # push dbt pipeline tasks
        for org_task in OrgTask.objects.filter(task__type__in=["dbt", "git"]).all():
            logger.info(
                f"found transform task {org_task.task.slug}; pushing to pipeline"
            )
            # map this org task to dataflow
            map_org_tasks.append(org_task)

            dbt_core_task_setup = PrefectDbtTaskSetup(
                seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                slug=org_task.task.slug,
                commands=[f"{dbt_binary} {org_task.task.command} --target {target}"],
                type=DBTCORE,
                env={},
                working_dir=project_dir,
                profiles_dir=f"{project_dir}/profiles/",
                project_dir=project_dir,
                cli_profile_block=cli_block.block_name,
                cli_args=[],
            )

            task_config = dict(dbt_core_task_setup)

            # update task_config its a git pull task
            if org_task.task.slug == TASK_GITPULL:
                shell_env = {"secret-git-pull-url-block": ""}

                gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
                    org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
                ).first()

                if gitpull_secret_block is not None:
                    shell_env[
                        "secret-git-pull-url-block"
                    ] = gitpull_secret_block.block_name

                shell_task_setup = PrefectShellTaskSetup(
                    commands=["git pull"],
                    working_dir=project_dir,
                    env=shell_env,
                    slug=org_task.task.slug,
                    type=SHELLOPERATION,
                    seq=TRANSFORM_TASKS_SEQ[org_task.task.slug] + seq,
                )

                task_config = dict(shell_task_setup)

            tasks.append(task_config)
        logger.info(f"Dbt tasks pushed to the pipeline")

    # update deployment
    payload.deployment_params = {"config": {"tasks": tasks}}
    try:
        prefect_service.update_dataflow_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to update a pipeline") from error

    for org_task in map_org_tasks:
        DataflowOrgTask.objects.create(dataflow=org_data_flow, orgtask=org_task)

    org_data_flow.cron = payload.cron if payload.cron else None
    org_data_flow.name = payload.name
    org_data_flow.save()

    return {"success": 1}
