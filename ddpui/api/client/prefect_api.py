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

from ddpui.ddpprefect import DBTCORE, SHELLOPERATION
from ddpui.models.org import OrgPrefectBlock, OrgWarehouse, OrgDataFlow
from ddpui.models.org_user import OrgUser
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    PrefectDataFlowCreateSchema,
    PrefectDataFlowCreateSchema2,
    PrefectFlowRunSchema,
    PrefectDataFlowUpdateSchema,
    PrefectShellSetup,
)

from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone

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
    request, exc: Exception
):  # pylint: disable=unused-argument # skipcq PYL-W0613
    """Handle any other exception raised in the apis"""
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
            org=orguser.org, block_type=DBTCORE
        ).order_by("seq"):
            dbt_blocks.append({"blockName": dbt_block.block_name, "seq": dbt_block.seq})

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
    )

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

    org_data_flows = (
        OrgDataFlow.objects.filter(org=orguser.org).exclude(cron=None).all()
    )

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

    prefect_service.update_dataflow(deployment_id, payload)

    org_data_flow.cron = payload.cron
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

    try:
        res = prefect_service.create_deployment_flow_run(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error
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
        logger.exception(error)
        raise HttpError(400, "failed to run dbt") from error
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

    dbt_env_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(dbt_env_dir):
        raise HttpError(400, "create the dbt env first")

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    project_dir = str(dbt_env_dir / "dbtrepo")
    dbt_project_filename = str(dbt_env_dir / "dbtrepo/dbt_project.yml")

    if not os.path.exists(dbt_project_filename):
        raise HttpError(400, dbt_project_filename + " is missing")

    # create the git pull shell block
    try:
        command = "git pull"
        block_name = f"{orguser.org.slug}-" f"{slugify(command)}"
        shell_cmd = PrefectShellSetup(
            blockname=block_name,
            commands=[command],
            workingDir=project_dir,
            env={},
        )
        block_response = prefect_service.create_shell_block(shell_cmd)

        # store prefect shell block in database
        shellprefectblock = OrgPrefectBlock(
            org=orguser.org,
            block_type=SHELLOPERATION,
            block_id=block_response["block_id"],
            block_name=block_response["block_name"],
            display_name=block_name,
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
            seq=sequence_number,
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
                    dbt_blocks=[{"blockName": block_name, "seq": 0}],
                )
            )

            # store deployment record in django db
            existing_dataflow = OrgDataFlow.objects.filter(
                deployment_id=dataflow["deployment"]["id"]
            ).first()
            if existing_dataflow:
                existing_dataflow.delete()

            OrgDataFlow.objects.create(
                org=orguser.org,
                name=f"manual-run-{block_name}",
                deployment_name=dataflow["deployment"]["name"],
                deployment_id=dataflow["deployment"]["id"],
            )

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
        block = {
            "blockType": prefect_block.block_type,
            "blockId": prefect_block.block_id,
            "blockName": prefect_block.block_name,
            "action": prefect_block.command,
            "target": prefect_block.dbt_target_schema,
            "deploymentId": None,
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
        org=orguser.org, block_type=DBTCORE
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
def get_prefect_flow_runs_log_history(request, deployment_id):
    # pylint: disable=unused-argument
    """Fetch all flow runs for the deployment and the logs for each flow run"""
    flow_runs = prefect_service.get_flow_runs_by_deployment_id(deployment_id, limit=0)

    for flow_run in flow_runs:
        logs_dict = prefect_service.get_flow_run_logs(flow_run["id"], 0)
        flow_run["logs"] = (
            logs_dict["logs"]["logs"] if "logs" in logs_dict["logs"] else []
        )

    return flow_runs
