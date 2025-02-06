import os
from pathlib import Path
from uuid import uuid4

from ninja import Router
from ninja.errors import HttpError

from ddpui import auth
from ddpui.auth import has_permission
from ddpui.celeryworkers.tasks import (
    clone_github_repo,
    run_dbt_commands,
    setup_dbtworkspace,
)
from ddpui.models.tasks import (
    TaskProgressHashPrefix,
    TaskLock,
    OrgTask,
)
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_DBTCLEAN,
    TASK_DBTDEPS,
)
from ddpui.ddpdbt import dbt_service, elementary_service
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET, prefect_service
from ddpui.ddpprefect.schema import OrgDbtGitHub, OrgDbtSchema, OrgDbtTarget, PrefectSecretBlockEdit
from ddpui.models.org import OrgPrefectBlockv1, Org
from ddpui.models.org_user import OrgUser, OrgUserResponse
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.dbtdocs import create_single_html
from ddpui.utils.helpers import runcmd
from ddpui.utils.orguserhelpers import from_orguser
from ddpui.utils.redis_client import RedisClient
from ddpui.schemas.org_task_schema import TaskParameters

dbt_router = Router()
logger = CustomLogger("ddpui")


@dbt_router.post("/workspace/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_workspace"])
def post_dbt_workspace(request, payload: OrgDbtSchema):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is not None:
        org.dbt.delete()
        org.dbt = None
        org.save()

    repo_exists = dbt_service.check_repo_exists(payload.gitrepoUrl, payload.gitrepoAccessToken)

    if not repo_exists:
        raise HttpError(400, "Github repository does not exist")

    task = setup_dbtworkspace.delay(org.id, payload.dict())

    return {"task_id": task.id}


@dbt_router.put("/github/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_dbt_workspace"])
def put_dbt_github(request, payload: OrgDbtGitHub):
    """Setup the client git repo and install a virtual env inside it to run dbt"""
    orguser: OrgUser = request.orguser
    org = orguser.org
    if org.dbt is None:
        raise HttpError(400, "Create a dbt workspace first")

    repo_exists = dbt_service.check_repo_exists(payload.gitrepoUrl, payload.gitrepoAccessToken)

    if not repo_exists:
        raise HttpError(400, "Github repository does not exist")

    org.dbt.gitrepo_url = payload.gitrepoUrl
    org.dbt.gitrepo_access_token_secret = payload.gitrepoAccessToken
    org.dbt.save()

    # ignore if token is *******
    if set(payload.gitrepoAccessToken.strip()) == set("*"):
        pass

    # if token is empty, delete the secret block
    elif payload.gitrepoAccessToken in [None, ""]:
        block_name = f"{orguser.org.slug}-git-pull-url"
        secret_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=SECRET, block_name=block_name
        ).first()
        if secret_block:
            prefect_service.delete_secret_block(secret_block.block_id)
            secret_block.delete()

    # else create / update the secret block
    else:
        gitrepo_url = payload.gitrepoUrl.replace(
            "github.com", "oauth2:" + payload.gitrepoAccessToken + "@github.com"
        )

        # update the git oauth endpoint with token in the prefect secret block
        secret_block_edit_params = PrefectSecretBlockEdit(
            block_name=f"{orguser.org.slug}-git-pull-url",
            secret=gitrepo_url,
        )

        response = prefect_service.upsert_secret_block(secret_block_edit_params)
        if not OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=SECRET, block_name=secret_block_edit_params.block_name
        ).exists():
            OrgPrefectBlockv1.objects.create(
                org=orguser.org,
                block_type=SECRET,
                block_name=secret_block_edit_params.block_name,
                block_id=response["block_id"],
            )

    org_dir = DbtProjectManager.get_org_dir(org)

    task = clone_github_repo.delay(
        org.slug,
        org.dbt.gitrepo_url,
        org.dbt.gitrepo_access_token_secret,
        org_dir,
        None,
    )

    return {"task_id": task.id}


@dbt_router.delete("/workspace/", response=OrgUserResponse, auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_dbt_workspace"])
def dbt_delete(request):
    """Delete the dbt workspace and project repo created"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    dbt_service.delete_dbt_workspace(orguser.org)

    return from_orguser(orguser)


@dbt_router.get("/dbt_workspace", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def get_dbt_workspace(request):
    """return details of the dbt workspace for this org"""
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        return {"error": "no dbt workspace has been configured"}

    secret_block_exists = OrgPrefectBlockv1.objects.filter(
        org=orguser.org, block_type=SECRET, block_name=f"{orguser.org.slug}-git-pull-url"
    ).exists()

    return {
        "gitrepo_url": orguser.org.dbt.gitrepo_url,
        "gitrepo_access_token": "*********" if secret_block_exists else None,
        "target_type": orguser.org.dbt.target_type,
        "default_schema": orguser.org.dbt.default_schema,
    }


@dbt_router.post("/git_pull/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_run_orgtask"])
def post_dbt_git_pull(request):
    """Pull the dbt repo from github for the organization"""
    orguser: OrgUser = request.orguser
    orgdbt = orguser.org.dbt
    if orgdbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
    if not os.path.exists(project_dir):
        raise HttpError(400, "create the dbt env first")

    try:
        runcmd("git pull", project_dir)
    except Exception as error:
        raise HttpError(500, f"git pull failed in {str(project_dir)}") from error

    return {"success": True}


@dbt_router.post("/makedocs/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_dbt_docs"])
def post_dbt_makedocs(request):
    """prepare the dbt docs single html"""
    orguser: OrgUser = request.orguser
    orgdbt = orguser.org.dbt
    if orgdbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    project_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt))
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


@dbt_router.put("/v1/schema/", auth=auth.CustomAuthMiddleware())
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
        logger.info(f"Updating the cli profile block's schema : {cli_profile_block.block_name}")
        prefect_service.update_dbt_cli_profile_block(
            block_name=cli_profile_block.block_name,
            target=payload.target_configs_schema,
        )
        logger.info(
            f"Successfully updated the cli profile block's schema : {cli_profile_block.block_name}"
        )

    return {"success": 1}


@dbt_router.get("/dbt_transform/", auth=auth.CustomAuthMiddleware())
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


@dbt_router.post("/run_dbt_via_celery/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_edit_dbt_workspace"])
def post_run_dbt_commands(request, payload: TaskParameters = None):
    """Run dbt commands via celery"""
    orguser: OrgUser = request.orguser
    org: Org = orguser.org

    task_id = str(uuid4())

    taskprogress = TaskProgress(task_id, f"{TaskProgressHashPrefix.RUNDBTCMDS}-{org.slug}")

    taskprogress.add({"message": "Added dbt commands in queue", "status": "queued"})

    try:
        task_locks: list[TaskLock] = []

        # acquire locks for clean, deps and run
        org_tasks = OrgTask.objects.filter(
            org=org,
            task__slug__in=[TASK_DBTCLEAN, TASK_DBTDEPS, TASK_DBTRUN],
            generated_by="system",
        ).all()
        for org_task in org_tasks:
            task_lock = TaskLock.objects.create(
                orgtask=org_task, locked_by=orguser, celery_task_id=task_id
            )
            task_locks.append(task_lock)

        # executes clean, deps, run
        run_dbt_commands.delay(org.id, task_id, payload.dict() if payload else None)

    finally:
        # clear all locks
        for lock in task_locks:
            lock.delete()

    return {"task_id": task_id}


@dbt_router.post("/fetch-elementary-report/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_fetch_elementary_report(request):
    """prepare the dbt docs single html"""
    orguser: OrgUser = request.orguser
    error, result = elementary_service.fetch_elementary_report(orguser.org)
    if error:
        raise HttpError(400, error)

    return result


@dbt_router.post("/v1/refresh-elementary-report/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_refresh_elementary_report_via_prefect(request):
    """prepare the dbt docs single html via prefect deployment"""
    orguser: OrgUser = request.orguser
    return elementary_service.refresh_elementary_report_via_prefect(orguser)


@dbt_router.get("/elementary-setup-status", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def get_elementary_setup_status(request):
    """prepare the dbt docs single html"""
    orguser: OrgUser = request.orguser
    result = elementary_service.elementary_setup_status(orguser.org)
    if "error" in result:
        raise HttpError(400, result["error"])

    return result


@dbt_router.get("/check-dbt-files", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def get_check_dbt_files(request):
    """checks that the dbt project is set up for elementary"""
    orguser: OrgUser = request.orguser
    error, result = elementary_service.check_dbt_files(orguser.org)
    if error:
        raise HttpError(400, error)

    return result


@dbt_router.post("/create-elementary-tracking-tables/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_create_elementary_tracking_tables(request):
    """prepare the dbt docs single html via prefect deployment"""
    orguser: OrgUser = request.orguser
    result = elementary_service.create_elementary_tracking_tables(orguser.org)
    if "error" in result:
        raise HttpError(400, result["error"])

    return result


@dbt_router.post("/create-elementary-profile/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_create_elementary_profile(request):
    """prepare the dbt docs single html via prefect deployment"""
    orguser: OrgUser = request.orguser
    result = elementary_service.create_elementary_profile(orguser.org)
    if "error" in result:
        raise HttpError(400, result["error"])

    return result


@dbt_router.post("/create-edr-deployment/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_dbt_workspace"])
def post_create_edr_sendreport_dataflow(request):
    """prepare the dbt docs single html via prefect deployment"""
    orguser: OrgUser = request.orguser

    org_task = get_edr_send_report_task(orguser.org)
    if org_task is None:
        logger.info("creating OrgTask for edr-send-report for %s", orguser.org.slug)
        org_task = get_edr_send_report_task(orguser.org, create=True)

    result = elementary_service.create_edr_sendreport_dataflow(orguser.org, org_task, "0 0 * * *")
    if "error" in result:
        raise HttpError(400, result["error"])

    return result
