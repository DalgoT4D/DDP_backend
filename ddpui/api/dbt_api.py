import os
from pathlib import Path
from uuid import uuid4

from ninja import NinjaAPI
from ninja.errors import HttpError, ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from ddpui import auth
from ddpui.auth import has_permission
from ddpui.celeryworkers.tasks import (
    clone_github_repo,
    run_dbt_commands,
    setup_dbtworkspace,
)
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.taskprogress import TaskProgress
from ddpui.ddpdbt import dbt_service
from ddpui.ddpprefect import DBTCLIPROFILE, prefect_service
from ddpui.ddpprefect.schema import OrgDbtGitHub, OrgDbtSchema, OrgDbtTarget
from ddpui.models.org import OrgPrefectBlockv1, Org
from ddpui.models.org_user import OrgUser, OrgUserResponse
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.dbtdocs import create_single_html
from ddpui.utils.helpers import runcmd
from ddpui.utils.orguserhelpers import from_orguser
from ddpui.utils.redis_client import RedisClient

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


@dbtapi.post("/workspace/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_workspace"])
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is not None:
        org.dbt.delete()
        org.dbt = None
        org.save()

    repo_exists = dbt_service.check_repo_exists(
        payload.gitrepoUrl, payload.gitrepoAccessToken
    )

    if not repo_exists:
        raise HttpError(400, "Github repository does not exist")

    task = setup_dbtworkspace.delay(org.id, payload.dict())

    return {"task_id": task.id}


@dbtapi.put("/github/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_dbt_workspace"])
def put_dbt_github(request, payload: OrgDbtGitHub):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is None:
        raise HttpError(400, "Create a dbt workspace first")

    repo_exists = dbt_service.check_repo_exists(
        payload.gitrepoUrl, payload.gitrepoAccessToken
    )

    if not repo_exists:
        raise HttpError(400, "Github repository does not exist")

    org.dbt.gitrepo_url = payload.gitrepoUrl
    org.dbt.gitrepo_access_token_secret = payload.gitrepoAccessToken
    org.dbt.save()

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    task = clone_github_repo.delay(
        org.slug,
        org.dbt.gitrepo_url,
        org.dbt.gitrepo_access_token_secret,
        str(project_dir),
        None,
    )

    return {"task_id": task.id}


@dbtapi.delete(
    "/workspace/", response=OrgUserResponse, auth=auth.CustomAuthMiddleware()
)
@has_permission(["can_delete_dbt_workspace"])
def dbt_delete(request):
    """Delete the dbt workspace and project repo created"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    dbt_service.delete_dbt_workspace(orguser.org)

    return from_orguser(orguser)


@dbtapi.get("/dbt_workspace", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
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


@dbtapi.post("/git_pull/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_run_orgtask"])
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


@dbtapi.post("/makedocs/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_docs"])
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

    redis = RedisClient.get_instance()
    token = uuid4()
    redis_key = f"dbtdocs-{token.hex}"
    redis.set(redis_key, htmlfilename.encode("utf-8"))
    redis.expire(redis_key, 3600 * 24)

    return {"token": token.hex}


@dbtapi.put("/v1/schema/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_dbt_workspace"])
def put_dbt_schema_v1(request, payload: OrgDbtTarget):
    """Update the target_configs.schema for the dbt cli profile block"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    org.dbt.default_schema = payload.target_configs_schema
    org.dbt.save()
    logger.info("updated orgdbt")

    cli_profile_block = OrgPrefectBlockv1.objects.filter(
        org=orguser.org, block_type=DBTCLIPROFILE
    ).first()

    if cli_profile_block:
        logger.info(
            f"Updating the cli profile block's schema : {cli_profile_block.block_name}"
        )
        prefect_service.update_dbt_cli_profile_block(
            block_name=cli_profile_block.block_name,
            target=payload.target_configs_schema,
        )
        logger.info(
            f"Successfully updated the cli profile block's schema : {cli_profile_block.block_name}"
        )

    return {"success": 1}


@dbtapi.get("/dbt_transform/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def get_transform_type(request):
    """find the transform type"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    if org.dbt is None:
        transform_type = None
    else:
        transform_type = org.dbt.transform_type

    return {"transform_type": transform_type}


@dbtapi.post("/run_dbt_via_celery/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_dbt_workspace"])
def post_run_dbt_commands(request):
    """Run dbt commands via celery"""
    orguser: OrgUser = request.orguser
    org: Org = orguser.org

    task_id = str(uuid4())

    taskprogress = TaskProgress(
        task_id, f"{TaskProgressHashPrefix.RUNDBTCMDS}-{org.slug}"
    )

    taskprogress.add({"message": "Added dbt commands in queue", "status": "queued"})

    # executes clean, deps, run
    run_dbt_commands.delay(orguser.id, task_id)

    return {"task_id": task_id}


@dbtapi.post("/fetch-elementary-report/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_fetch_elementary_report(request):
    """prepare the dbt docs single html"""
    orguser: OrgUser = request.orguser
    error, result = dbt_service.fetch_elementary_report(orguser.org)
    if error:
        raise HttpError(400, error)

    return result


@dbtapi.post("/refresh-elementary-report/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_refresh_elementary_report(request):
    """prepare the dbt docs single html"""
    orguser: OrgUser = request.orguser
    error, result = dbt_service.refresh_elementary_report(orguser.org)
    if error:
        raise HttpError(400, error)

    return result
