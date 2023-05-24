import os
from pathlib import Path
from datetime import datetime

from ninja import NinjaAPI
from ninja.errors import HttpError

# from ninja.errors import ValidationError
# from ninja.responses import Response
from django.utils.text import slugify

# from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect import prefect_service

from ddpui.ddpprefect import DBTCORE
from ddpui.models.org import OrgPrefectBlock, OrgWarehouse, OrgDataFlow
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    PrefectDbtRun,
    PrefectDataFlowCreateSchema,
    PrefectDataFlowCreateSchema2,
)

from ddpui.utils.ddp_logger import logger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone

prefectapi = NinjaAPI(urls_namespace="prefect")
# http://127.0.0.1:8000/api/docs


# @prefectapi.exception_handler(ValidationError)
# def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
#     """Handle any ninja validation errors raised in the apis"""
#     return Response({"error": exc.errors}, status=422)


# @prefectapi.exception_handler(PydanticValidationError)
# def pydantic_validation_error_handler(
#     request, exc: PydanticValidationError
# ):  # pylint: disable=unused-argument
#     """Handle any pydantic errors raised in the apis"""
#     return Response({"error": exc.errors()}, status=422)


# @prefectapi.exception_handler(HttpError)
# def ninja_http_error_handler(
#     request, exc: HttpError
# ):  # pylint: disable=unused-argument
#     """Handle any http errors raised in the apis"""
#     return Response({"error": " ".join(exc.args)}, status=exc.status_code)


# @prefectapi.exception_handler(Exception)
# def ninja_default_error_handler(
#     request, exc: Exception
# ):  # pylint: disable=unused-argument
#     """Handle any other exception raised in the apis"""
#     raise exc
#     # return Response({"error": " ".join(exc.args)}, status=500)


@prefectapi.post("/flows/", auth=auth.CanManagePipelines())
def post_prefect_dataflow(request, payload: PrefectDataFlowCreateSchema):
    """Create a prefect deployment i.e. a ddp dataflow"""
    orguser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

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
        ):
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
    orguser = request.orguser

    if orguser.org is None:
        raise HttpError(400, "register an organization first")

    org_data_flows = OrgDataFlow.objects.filter(org=orguser.org).all()

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
            }
        )

    return res


@prefectapi.delete("/flows/{deployment_id}", auth=auth.CanManagePipelines())
def delete_prefect_dataflow(request, deployment_id):
    """Delete a prefect deployment along with its org data flow"""
    orguser = request.orguser

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
    res = prefect_service.create_deployment_flow_run(deployment_id)
    return res


@prefectapi.post("/flows/airbyte_sync/", auth=auth.CanManagePipelines())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Run airbyte sync flow in prefect"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    if payload.flowName is None:
        payload.flowName = f"{orguser.org.name}-airbytesync"
    if payload.flowRunName is None:
        now = timezone.as_ist(datetime.now())
        payload.flowRunName = f"{now.isoformat()}"
    return prefect_service.run_airbyte_connection_sync(payload)


@prefectapi.post("/flows/dbt_run/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_run_flow(
    request, payload: PrefectDbtCore
):  # pylint: disable=unused-argument
    """Run dbt flow in prefect"""
    orguser = request.orguser

    dbt = orguser.org.dbt
    profile_file = Path(dbt.project_dir) / "dbtrepo/profiles/profiles.yml"
    if os.path.exists(profile_file):
        os.unlink(profile_file)

    if payload.flowName is None:
        payload.flowName = f"{orguser.org.name}-airbytesync"
    if payload.flowRunName is None:
        now = timezone.as_ist(datetime.now())
        payload.flowRunName = f"{now.isoformat()}"

    # save into some table

    return prefect_service.run_dbt_core_sync(payload)


@prefectapi.post("/blocks/dbt/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_block(request, payload: PrefectDbtRun):
    """Create three prefect dbt core blocks:
    - dbt run
    - dbt test
    - dbt docs generate
    for a ddp-dbt-profile
    """
    orguser = request.orguser
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

    target = (
        payload.profile.target_configs_schema
        if payload.profile.target_configs_schema
        else orguser.org.dbt.default_schema
    )

    block_names = []
    sequence_number = 0
    for command in ["docs generate", "run", "test"]:
        block_name = f"{orguser.org.slug}-{slugify(payload.profile.name)}-{slugify(target)}-{slugify(command)}"

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
                payload.profile,
                target,
                warehouse.wtype,
                credentials,
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
        )

        coreprefectblock.save()
        block_names.append(block_response["block_name"])

    return {"success": 1, "block_names": block_names}


@prefectapi.get("/blocks/dbt/", auth=auth.CanManagePipelines())
def get_prefect_dbt_run_blocks(request):
    """Fetch all prefect dbt run blocks for an organization"""
    orguser = request.orguser

    return [
        {
            "blockType": prefect_block.block_type,
            "blockId": prefect_block.block_id,
            "blockName": prefect_block.block_name,
        }
        for prefect_block in OrgPrefectBlock.objects.filter(
            org=orguser.org, block_type=DBTCORE
        )
    ]


@prefectapi.delete("/blocks/dbt/", auth=auth.CanManagePipelines())
def delete_prefect_dbt_run_block(request):
    """Delete prefect dbt run block for an organization"""
    orguser = request.orguser

    # blocks = prefect_service.get_blocks(prefect_service.DBTCORE, orguser.org.slug)
    org_dbt_blocks = OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=DBTCORE
    ).all()

    for dbt_block in org_dbt_blocks:
        # Delete block in prefect
        try:
            prefect_service.delete_dbt_core_block(dbt_block.block_id)
        except Exception as error:
            logger.exception(error)
            # may have deleted the block via the prefect ui, continue

        # Delete block row from database
        dbt_block.delete()

    return {"success": 1}


@prefectapi.get("/flow_runs/{flow_run_id}/logs", auth=auth.CanManagePipelines())
def get_flow_runs_logs(
    request, flow_run_id, offset: int = 0
):  # pylint: disable=unused-argument
    """return the logs from a flow-run"""
    return prefect_service.get_flow_run_logs(flow_run_id, offset)
