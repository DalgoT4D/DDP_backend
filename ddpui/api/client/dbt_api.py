import os
from pathlib import Path
from redis import Redis
from uuid import uuid4

from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.ddpprefect.schema import OrgDbtSchema, OrgDbtGitHub, OrgDbtTarget
from ddpui.models.org_user import OrgUserResponse, OrgUser
from ddpui.models.org import OrgPrefectBlock
from ddpui.ddpprefect import DBTCORE
from ddpui.utils.helpers import runcmd
from ddpui.utils.dbtdocs import create_single_html
from ddpui.celeryworkers.tasks import (
    setup_dbtworkspace,
    clone_github_repo,
    update_dbt_core_block_schema_task,
)
from ddpui.ddpdbt import dbt_service
from ddpui.utils.custom_logger import CustomLogger

dbtapi = NinjaAPI(urls_namespace="dbt")
logger = CustomLogger("ddpui")


@dbtapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@dbtapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@dbtapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    return Response({"detail": "something went wrong"}, status=500)


@dbtapi.post("/workspace/", auth=auth.CanManagePipelines())
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is not None:
        org.dbt.delete()
        org.dbt = None
        org.save()

    task = setup_dbtworkspace.delay(org.id, payload.dict())

    return {"task_id": task.id}


@dbtapi.put("/github/", auth=auth.CanManagePipelines())
def put_dbt_github(request, payload: OrgDbtGitHub):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    org.dbt.gitrepo_url = payload.gitrepoUrl
    org.dbt.gitrepo_access_token_secret = payload.gitrepoAccessToken
    org.dbt.save()

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    task = clone_github_repo.delay(
        org.dbt.gitrepo_url, org.dbt.gitrepo_access_token_secret, str(project_dir), None
    )

    return {"task_id": task.id}


@dbtapi.put("/schema/", auth=auth.CanManagePipelines())
def put_dbt_schema(request, payload: OrgDbtTarget):
    """Update the target_configs.schema for all dbt-core-op blocks"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    org.dbt.default_schema = payload.target_configs_schema
    org.dbt.save()
    logger.info("updated orgdbt")

    for dbtblock in OrgPrefectBlock.objects.filter(org=org, block_type=DBTCORE):
        logger.info(
            "updating schema of %s to %s",
            dbtblock.block_name,
            org.dbt.default_schema,
        )
        update_dbt_core_block_schema_task.delay(
            dbtblock.block_name, org.dbt.default_schema
        )


@dbtapi.delete("/workspace/", response=OrgUserResponse, auth=auth.CanManagePipelines())
def dbt_delete(request):
    """Delete the dbt workspace and project repo created"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    dbt_service.delete_dbt_workspace(orguser.org)

    return OrgUserResponse.from_orguser(orguser)


@dbtapi.get("/dbt_workspace", auth=auth.CanManagePipelines())
def get_dbt_workspace(request):
    """return details of the dbt workspace for this org"""
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        return {"error": "no dbt workspace has been configured"}

    return {
        "gitrepo_url": orguser.org.dbt.gitrepo_url,
        "target_type": orguser.org.dbt.target_type,
        "default_schema": orguser.org.dbt.default_schema,
    }


@dbtapi.post("/git_pull/", auth=auth.CanManagePipelines())
def post_dbt_git_pull(request):
    """Pull the dbt repo from github for the organization"""
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    try:
        runcmd("git pull", project_dir / "dbtrepo")
    except Exception as error:
        raise HttpError(
            500, f"git pull failed in {str(project_dir / 'dbtrepo')}"
        ) from error

    return {"success": True}


@dbtapi.post("/makedocs/", auth=auth.CanManagePipelines())
def post_dbt_makedocs(request):
    """prepare the dbt docs single html"""
    orguser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / orguser.org.slug
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    repo_dir = project_dir / "dbtrepo"
    if not os.path.exists(repo_dir / "target"):
        raise HttpError(400, "run dbt docs generate first")

    # passing the repo_dir to create_single_html is considered a security
    # risk by deepsource (PTC-W6004) so we pass only the slug
    html = create_single_html(orguser.org.slug)
    htmlfilename = str(repo_dir / "dbtdocs.html")
    with open(htmlfilename, "w", encoding="utf-8") as indexfile:
        indexfile.write(html)
        indexfile.close()

    redis = Redis()
    token = uuid4()
    redis_key = f"dbtdocs-{token.hex}"
    redis.set(redis_key, htmlfilename.encode("utf-8"))
    redis.expire(redis_key, 3600 * 24)

    return {"token": token.hex}
