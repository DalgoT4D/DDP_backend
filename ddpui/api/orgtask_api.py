import uuid
import os
from pathlib import Path
from datetime import datetime
import yaml

from ninja import NinjaAPI
from ninja.errors import HttpError
from ninja.errors import ValidationError

from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError

from django.forms.models import model_to_dict
from django.db.models import Prefetch
from ddpui import auth
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service

from ddpui.ddpprefect import (
    DBTCLIPROFILE,
    SECRET,
)
from ddpui.models.org import (
    OrgWarehouse,
    OrgPrefectBlockv1,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import (
    DataflowOrgTask,
    OrgTask,
    TaskLock,
    Task,
    OrgTaskGeneratedBy,
)
from ddpui.ddpprefect.schema import (
    PrefectSecretBlockCreate,
)
from ddpui.schemas.org_task_schema import CreateOrgTaskPayload, TaskParameters
from ddpui.core.dbtfunctions import gather_dbt_project_params
from ddpui.core.orgtaskfunctions import (
    create_default_transform_tasks,
    create_prefect_deployment_for_dbtcore_task,
    delete_orgtask,
    fetch_orgtask_lock,
    fetch_orgtask_lock_v1,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone
from ddpui.utils.helpers import map_airbyte_keys_to_postgres_keys
from ddpui.utils.constants import (
    TASK_DBTRUN,
    TASK_GITPULL,
    TRANSFORM_TASKS_SEQ,
    TASK_GENERATE_EDR,
    TASK_SEED,
)
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.core.pipelinefunctions import (
    setup_dbt_core_task_config,
    setup_git_pull_shell_task_config,
    setup_edr_send_report_task_config,
)
from ddpui.auth import has_permission

orgtaskapi = NinjaAPI(urls_namespace="orgtask")
# http://127.0.0.1:8000/api/docs


logger = CustomLogger("ddpui")


@orgtaskapi.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@orgtaskapi.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    print(exc)
    return Response({"detail": exc.errors()}, status=500)


@orgtaskapi.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception  # skipcq PYL-W0613
):  # pylint: disable=unused-argument
    """Handle any other exception raised in the apis"""
    logger.info(exc)
    return Response({"detail": "something went wrong"}, status=500)


@orgtaskapi.post("/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_orgtask"])
def post_orgtask(request, payload: CreateOrgTaskPayload):
    """Create a custom client org task (dbt or git). If base task is dbt run create a deployment"""
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    task = Task.objects.filter(slug=payload.task_slug).first()

    if task is None:
        raise HttpError(404, "task not found")

    parameters = {}
    if payload.flags and len(payload.flags) > 0:
        parameters["flags"] = payload.flags

    if payload.options and len(payload.options.keys()) > 0:
        parameters["options"] = payload.options

    # create a deployment if the task type is run
    orgtask = OrgTask.objects.create(
        org=orguser.org,
        task=task,
        parameters=parameters,
        generated_by="client",
        uuid=uuid.uuid4(),
    )

    dataflow = None
    if task.slug in [TASK_DBTRUN, TASK_SEED]:
        dbt_project_params, error = gather_dbt_project_params(orguser.org)
        if error:
            raise HttpError(400, error)

        # fetch the cli profile block
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()

        if cli_profile_block is None:
            raise HttpError(400, "dbt cli profile block not found")

        dataflow = create_prefect_deployment_for_dbtcore_task(
            orgtask, cli_profile_block, dbt_project_params
        )

    return {
        **model_to_dict(orgtask, fields=["parameters"]),
        "task_slug": orgtask.task.slug,
        "dataflow": (
            {**model_to_dict(dataflow, exclude=["id", "org"])} if dataflow else None
        ),
    }


@orgtaskapi.post("transform/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_create_orgtask"])
def post_system_transformation_tasks(request):
    """
    - Create a git pull url secret block
    - Create a dbt cli profile block
    - Create dbt tasks
        - git pull
        - dbt deps
        - dbt clean
        - dbt run
        - dbt test
        - dbt docs generate
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

    dbt_project_params, error = gather_dbt_project_params(orguser.org)
    if error:
        raise HttpError(400, error)
    dbt_project_filename = str(dbt_project_params.dbt_repo_dir / "dbt_project.yml")

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
    # target = orguser.org.dbt.default_schema
    logger.info("profile_name=%s target=%s", profile_name, dbt_project_params.target)

    # get the dataset location if warehouse type is bigquery
    bqlocation = None
    if warehouse.wtype == "bigquery":
        destination = airbyte_service.get_destination(
            orguser.org.airbyte_workspace_id, warehouse.airbyte_destination_id
        )
        if destination.get("connectionConfiguration"):
            bqlocation = destination["connectionConfiguration"]["dataset_location"]

    # map airbyte keys to postgres keys
    if warehouse.wtype == "postgres":
        credentials = map_airbyte_keys_to_postgres_keys(credentials)

    # create a dbt cli profile block
    try:
        cli_block_name = f"{orguser.org.slug}-{profile_name}"

        cli_block_response = prefect_service.create_dbt_cli_profile_block(
            cli_block_name,
            profile_name,
            dbt_project_params.target,
            warehouse.wtype,
            bqlocation,
            credentials,
        )

        # save the cli profile block in django db
        cli_profile_block = OrgPrefectBlockv1.objects.create(
            org=orguser.org,
            block_type=DBTCLIPROFILE,
            block_id=cli_block_response["block_id"],
            block_name=cli_block_response["block_name"],
        )

    except Exception as error:
        logger.exception(error)
        raise HttpError(400, str(error)) from error

    # create org tasks for the transformation page
    _, error = create_default_transform_tasks(
        orguser.org, cli_profile_block, dbt_project_params
    )
    if error:
        raise HttpError(400, error)

    return {"success": 1}


@orgtaskapi.get("elementary-lock/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_orgtasks"])
def get_elemetary_task_lock(request):
    """Check if the elementary report generation task is underway"""
    org = request.orguser.org
    org_task = get_edr_send_report_task(org)
    return fetch_orgtask_lock(org_task)


@orgtaskapi.get("transform/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_orgtasks"])
def get_prefect_transformation_tasks(request):
    """Fetch all dbt tasks for an org; client or system"""
    orguser: OrgUser = request.orguser

    org_tasks = (
        OrgTask.objects.filter(
            org=orguser.org,
            task__type__in=["git", "dbt"],
        )
        .order_by("-generated_by")
        .select_related("task")
    )

    all_org_task_ids = org_tasks.values_list("id", flat=True)
    all_org_task_locks = TaskLock.objects.filter(orgtask_id__in=all_org_task_ids)

    all_dataflow_orgtasks = DataflowOrgTask.objects.filter(
        orgtask_id__in=all_org_task_ids, dataflow__dataflow_type="manual"
    ).select_related("dataflow")

    res = []

    for org_task in org_tasks:
        # git pull               : "git" + " " + "pull"
        # dbt run --full-refresh : "dbt" + " " + "run --full-refresh"
        command = org_task.task.type + " " + org_task.get_task_parameters()

        lock = None
        all_locks = [
            lock for lock in all_org_task_locks if lock.orgtask_id == org_task.id
        ]
        if len(all_locks) > 0:
            lock = all_locks[0]

        res.append(
            {
                "label": org_task.task.label,
                "slug": org_task.task.slug,
                "id": org_task.id,
                "uuid": org_task.uuid,
                "deploymentId": None,
                "lock": fetch_orgtask_lock_v1(org_task, lock),
                "command": command,
                "generated_by": org_task.generated_by,
                "seq": TRANSFORM_TASKS_SEQ[org_task.task.slug],
            }
        )

        # fetch the manual deploymentId for the long running dbt tasks
        dataflow_orgtasks = [
            dfot for dfot in all_dataflow_orgtasks if dfot.orgtask_id == org_task.id
        ]
        res[-1]["deploymentId"] = (
            dataflow_orgtasks[0].dataflow.deployment_id
            if len(dataflow_orgtasks) > 0
            else None
        )

    return res


@orgtaskapi.delete("transform/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_orgtask"])
def delete_system_transformation_tasks(request):
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

    for org_task in OrgTask.objects.filter(org=orguser.org, task__is_system=True).all():
        _, error = delete_orgtask(org_task)

        if error:
            logger.info(
                f"Failed deleting orgtask with id {org_task.id} of type {org_task.task.slug}. Skipping and continuing to next task deletion"
            )
            continue


@orgtaskapi.post("{orgtask_uuid}/run/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_run_orgtask"])
def post_run_prefect_org_task(
    request, orgtask_uuid, payload: TaskParameters = None
):  # pylint: disable=unused-argument
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

    try:
        uuid.UUID(str(orgtask_uuid))
    except ValueError:
        raise HttpError(400, "invalid input type")

    org_task = OrgTask.objects.filter(org=orguser.org, uuid=orgtask_uuid).first()

    if org_task is None:
        raise HttpError(400, "task not found")

    if org_task.task.type not in ["dbt", "git", "edr"]:
        raise HttpError(400, "task not supported")

    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    dbt_project_params, error = gather_dbt_project_params(orguser.org)

    # check if the task is locked
    task_lock = TaskLock.objects.filter(orgtask=org_task).first()
    if task_lock:
        raise HttpError(
            400, f"{task_lock.locked_by.user.email} is running this operation"
        )

    # lock the task
    task_lock = TaskLock.objects.create(orgtask=org_task, locked_by=orguser)

    if org_task.task.slug == TASK_GITPULL:
        gitpull_secret_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=SECRET, block_name__contains="git-pull"
        ).first()

        task_config = setup_git_pull_shell_task_config(
            org_task,
            dbt_project_params.project_dir,
            gitpull_secret_block,
        )

        if task_config.flow_name is None:
            task_config.flow_name = f"{orguser.org.name}-gitpull"
        if task_config.flow_run_name is None:
            now = timezone.as_ist(datetime.now())
            task_config.flow_run_name = f"{now.isoformat()}"

        try:
            result = prefect_service.run_shell_task_sync(task_config)
        except Exception as error:
            task_lock.delete()
            logger.exception(error)
            raise HttpError(
                400, f"failed to run the shell task {org_task.task.slug}"
            ) from error

    elif org_task.task.slug == TASK_GENERATE_EDR:
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        task_config = setup_edr_send_report_task_config(
            org_task, dbt_project_params.project_dir, dbt_env_dir
        )

        if task_config.flow_name is None:
            task_config.flow_name = f"{orguser.org.name}-edr-send-report"
        if task_config.flow_run_name is None:
            now = timezone.as_ist(datetime.now())
            task_config.flow_run_name = f"{now.isoformat()}"

        try:
            result = prefect_service.run_shell_task_sync(task_config)
        except Exception as error:
            task_lock.delete()
            logger.exception(error)
            raise HttpError(
                400, f"failed to run the shell task {org_task.task.slug}"
            ) from error

    else:
        dbt_env_dir = Path(orguser.org.dbt.dbt_venv)
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        # fetch the cli profile block
        cli_profile_block = OrgPrefectBlockv1.objects.filter(
            org=orguser.org, block_type=DBTCLIPROFILE
        ).first()

        if cli_profile_block is None:
            raise HttpError(400, "dbt cli profile block not found")

        # save orgtask params to memory and not db
        if payload:
            org_task.parameters = dict(payload)

        task_config = setup_dbt_core_task_config(
            org_task, cli_profile_block, dbt_project_params
        )

        if task_config.flow_name is None:
            task_config.flow_name = f"{orguser.org.name}-{org_task.task.slug}"
        if task_config.flow_run_name is None:
            now = timezone.as_ist(datetime.now())
            task_config.flow_run_name = f"{now.isoformat()}"

        try:
            result = prefect_service.run_dbt_task_sync(task_config)
        except Exception as error:
            task_lock.delete()
            logger.exception(error)
            raise HttpError(400, "failed to run dbt") from error

    # release the lock
    task_lock.delete()
    logger.info("released lock on task %s", org_task.task.slug)

    return result


@orgtaskapi.delete("{orgtask_uuid}/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_delete_orgtask"])
def post_delete_orgtask(request, orgtask_uuid):  # pylint: disable=unused-argument
    """Delete client generated orgtask"""

    orguser: OrgUser = request.orguser

    try:
        uuid.UUID(str(orgtask_uuid))
    except ValueError:
        raise HttpError(400, "invalid input type")

    org_task = OrgTask.objects.filter(org=orguser.org, uuid=orgtask_uuid).first()

    if org_task is None:
        raise HttpError(400, "task not found")

    if org_task.task.type not in ["dbt", "git", "edr"]:
        raise HttpError(400, "task not supported")

    if orguser.org.dbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    if org_task.generated_by == OrgTaskGeneratedBy.SYSTEM:
        raise HttpError(400, "cannot delete system generated tasks")

    # check if the task is locked
    task_lock = TaskLock.objects.filter(orgtask=org_task).first()
    if task_lock:
        raise HttpError(
            400,
            f"Cannot delete, {task_lock.locked_by.user.email} is running this operation",
        )

    # make sure the org task is not part of a orchestrate pipeline
    if (
        DataflowOrgTask.objects.filter(
            orgtask=org_task, dataflow__dataflow_type="orchestrate"
        ).count()
        > 0
    ):
        raise HttpError(403, "Cannot delete the orgtask since its part of a pipeline")

    _, error = delete_orgtask(org_task)

    if error:
        logger.info(
            f"Failed deleting orgtask with id {org_task.id} of type {org_task.task.slug}. Skipping and continuing to next task deletion"
        )
        raise HttpError(400, error)

    return {"success": 1}
