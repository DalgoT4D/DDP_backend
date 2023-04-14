import os
from pathlib import Path

from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.org_prefect_block import OrgPrefectBlock
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,  # DbtProfile,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    PrefectDbtRun,
)

prefectapi = NinjaAPI(urls_namespace="prefect")
# http://127.0.0.1:8000/api/docs


@prefectapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """Handle any ninja validation errors raised in the apis"""
    return Response({"error": exc.errors}, status=422)


@prefectapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """Handle any pydantic errors raised in the apis"""
    return Response({"error": exc.errors()}, status=422)


@prefectapi.exception_handler(HttpError)
def ninja_http_error_handler(
    request, exc: HttpError
):  # pylint: disable=unused-argument
    """Handle any http errors raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=exc.status_code)


@prefectapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"error": " ".join(exc.args)}, status=500)


@prefectapi.post("/prefect/flows/airbyte_sync/", auth=auth.CanManagePipelines())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Run airbyte sync flow in prefect"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return prefect_service.run_airbyte_connection_prefect_flow(payload.blockname)


@prefectapi.post("/prefect/flows/dbt_run/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_run_flow(
    request, payload: PrefectDbtCore
):  # pylint: disable=unused-argument
    """Run dbt flow in prefect"""
    return prefect_service.run_dbtcore_prefect_flow(payload.blockname)


@prefectapi.post("/prefect/blocks/dbt_run/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_block(request, payload: PrefectDbtRun):
    """Create prefect dbt core block"""
    orguser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    dbt_env_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(dbt_env_dir):
        raise HttpError(400, "create the dbt env first")

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    project_dir = str(dbt_env_dir / "dbtrepo")

    block_data = PrefectDbtCoreSetup(
        blockname=payload.dbt_blockname,
        profiles_dir=f"{project_dir}/profiles/",
        project_dir=project_dir,
        working_dir=project_dir,
        env={},
        commands=[f"{dbt_binary} run --target {payload.profile.target}"],
    )

    block = prefect_service.create_dbt_core_block(
        block_data, payload.profile, payload.credentials
    )

    cpb = OrgPrefectBlock(
        org=orguser.org,
        blocktype=block["block_type"]["name"],
        blockid=block["id"],
        blockname=block["name"],
    )
    cpb.save()

    return block


@prefectapi.get("/prefect/blocks/dbt_run/", auth=auth.CanManagePipelines())
def get_prefect_dbt_run_blocks(request):
    """Fetch all prefect dbt run blocks for an organization"""
    orguser = request.orguser

    return [
        {
            "blocktype": x.blocktype,
            "blockid": x.blockid,
            "blockname": x.blockname,
        }
        for x in OrgPrefectBlock.objects.filter(
            org=orguser.org, blocktype=prefect_service.DBTCORE
        )
    ]


@prefectapi.delete("/prefect/blocks/dbt_run/{block_id}", auth=auth.CanManagePipelines())
def delete_prefect_dbt_run_block(request, block_id):
    """Delete prefect dbt run block for an organization"""
    orguser = request.orguser
    # don't bother checking for orguser.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=orguser.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}


@prefectapi.post("/prefect/blocks/dbt_test/", auth=auth.CanManagePipelines())
def post_prefect_dbt_test_block(request, payload: PrefectDbtRun):
    """Create prefect dbt test block for an organization"""
    orguser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    project_dir = project_dir / "dbtrepo"
    dbt_binary = project_dir / "venv/bin/dbt"

    block_data = PrefectDbtCoreSetup(
        blockname=payload.dbt_blockname,
        profiles_dir=f"{project_dir}/profiles/",
        project_dir=project_dir,
        working_dir=project_dir,
        env={},
        commands=[f"{dbt_binary} test --target {payload.target}"],
    )

    block = prefect_service.create_dbt_core_block(
        block_data, payload.profile, payload.credentials
    )

    cpb = OrgPrefectBlock(
        org=orguser.org,
        blocktype=block["block_type"]["name"],
        blockid=block["id"],
        blockname=block["name"],
    )
    cpb.save()

    return block


@prefectapi.delete(
    "/prefect/blocks/dbt_test/{block_id}", auth=auth.CanManagePipelines()
)
def delete_prefect_dbt_test_block(request, block_id):
    """Delete dbt test block for an organization"""
    orguser = request.orguser
    # don't bother checking for orguser.org.dbt

    prefect_service.delete_dbt_core_block(block_id)
    cpb = OrgPrefectBlock.objects.filter(org=orguser.org, blockid=block_id).first()
    if cpb:
        cpb.delete()

    return {"success": 1}
