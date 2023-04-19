import os
from pathlib import Path

from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from django.utils.text import slugify
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import OrgPrefectBlock, OrgWarehouse
from ddpui.ddpprefect.schema import (
    PrefectAirbyteSync,  # DbtProfile,
    PrefectDbtCore,
    PrefectDbtCoreSetup,
    PrefectDbtRun,
)
from ddpui.utils.ddp_logger import logger
from ddpui.utils import secretsmanager

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
    raise exc
    # return Response({"error": " ".join(exc.args)}, status=500)


@prefectapi.post("/flows/airbyte_sync/", auth=auth.CanManagePipelines())
def post_prefect_airbyte_sync_flow(request, payload: PrefectAirbyteSync):
    """Run airbyte sync flow in prefect"""
    orguser = request.orguser
    if orguser.org.airbyte_workspace_id is None:
        raise HttpError(400, "create an airbyte workspace first")

    return prefect_service.run_airbyte_connection_prefect_flow(payload.blockName)


@prefectapi.post("/flows/dbt_run/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_run_flow(
    request, payload: PrefectDbtCore
):  # pylint: disable=unused-argument
    """Run dbt flow in prefect"""
    return prefect_service.run_dbtcore_prefect_flow(payload.blockName)


@prefectapi.post("/blocks/dbt/", auth=auth.CanManagePipelines())
def post_prefect_dbt_core_block(request, payload: PrefectDbtRun):
    """Create prefect dbt core block"""
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

    sequence_number = 0
    for command in ["docs generate", "run", "test"]:
        block_name = f"{orguser.org.slug}-{slugify(command)}"

        block_data = PrefectDbtCoreSetup(
            block_name=block_name,
            profiles_dir=f"{project_dir}/profiles/",
            project_dir=project_dir,
            working_dir=project_dir,
            env={},
            commands=[f"{dbt_binary} {command} --target {payload.profile.target}"],
        )

        block = prefect_service.create_dbt_core_block(
            block_data,
            payload.profile,
            warehouse.wtype,
            credentials,
        )

        coreprefectblock = OrgPrefectBlock(
            org=orguser.org,
            block_type=block["block_type"]["name"],
            block_id=block["id"],
            block_name=block["name"],
            display_name=block["name"],
            seq=sequence_number,
        )

        coreprefectblock.save()

    return {"success": 1}


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
            org=orguser.org, block_type=prefect_service.DBTCORE
        )
    ]


@prefectapi.delete("/blocks/dbt/", auth=auth.CanManagePipelines())
def delete_prefect_dbt_run_block(request):
    """Delete prefect dbt run block for an organization"""
    orguser = request.orguser

    # blocks = prefect_service.get_blocks(prefect_service.DBTCORE, orguser.org.slug)
    org_dbt_blocks = OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=prefect_service.DBTCORE
    ).all()

    for dbt_block in org_dbt_blocks:
        # Delete block in prefect
        try:
            prefect_service.delete_dbt_core_block(dbt_block.block_id)
        except Exception as error:
            logger.error(error)
            # may have deleted the block via the prefect ui, continue

        # Delete block row from database
        dbt_block.delete()

    return {"success": 1}
