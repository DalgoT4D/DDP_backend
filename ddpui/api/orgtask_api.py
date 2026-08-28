import uuid
from datetime import datetime

from ninja import Router
from ninja.errors import HttpError

from django.forms.models import model_to_dict
from ddpui.ddpprefect import prefect_service
from ddpui.ddpdbt import dbthelpers

from ddpui.ddpprefect import SECRET
from ddpui.models.org import (
    Org,
    OrgWarehouse,
    OrgPrefectBlockv1,
    OrgDataFlowv1,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import (
    DataflowOrgTask,
    OrgTask,
    TaskLock,
    Task,
    OrgTaskGeneratedBy,
    TaskType,
)
from ddpui.ddpprefect.schema import (
    PrefectSecretBlockEdit,
)
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.schemas.org_task_schema import CreateOrgTaskPayload, TaskParameters

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.orgtaskfunctions import (
    create_default_transform_tasks,
    create_prefect_deployment_for_dbtcore_task,
    delete_orgtask,
    fetch_orgtask_lock_v1,
)
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import timezone
from ddpui.utils.constants import (
    TRANSFORM_TASKS_SEQ,
    TASK_GENERATE_EDR,
    LONG_RUNNING_TASKS,
    DEFAULT_TRANSFORM_TASKS_IN_PIPELINE,
    TRANSFORM_TASKS_DEPENDENCIES,
)
from ddpui.core.orgtaskfunctions import get_edr_send_report_task
from ddpui.core.pipelinefunctions import (
    fetch_pipeline_lock_v1,
)
from ddpui.auth import has_permission
from ddpui.core.audit_log_service import create_audit_log
from ddpui.models.audit_log import AuditLogAction, AuditLogResourceType

orgtask_router = Router()
logger = CustomLogger("ddpui")


@orgtask_router.post("/")
@has_permission(["can_create_orgtask"])
def post_orgtask(request, payload: CreateOrgTaskPayload):
    """Create a custom client org task (dbt or git). If base task is dbt run create a deployment"""
    orguser: OrgUser = request.orguser
    orgdbt = orguser.org.dbt
    if orgdbt is None:
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
        dbt=orgdbt,
    )

    dataflow = None
    if task.slug in LONG_RUNNING_TASKS:
        # For dbt-cli
        if task.type == TaskType.DBT:
            dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(
                orguser.org, orgdbt
            )

            dataflow = create_prefect_deployment_for_dbtcore_task(orgtask, dbt_project_params)

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.DBT,
        resource_id=str(orgtask.uuid),
        action=AuditLogAction.CREATE,
        resource_fields={"name": orgtask.task.slug},
    )

    return {
        **model_to_dict(orgtask, fields=["parameters"]),
        "task_slug": orgtask.task.slug,
        "dataflow": ({**model_to_dict(dataflow, exclude=["id", "org"])} if dataflow else None),
    }


@orgtask_router.post("transform/")
@has_permission(["can_create_orgtask"])
def post_system_transformation_tasks(request):
    """
    - Create a git pull url secret block
    - Create a dbt cli profile block
    - Create all the system transform tasks
        - git pull
        - dbt deps
        - dbt clean
        - dbt run
        - dbt test
    """
    orguser: OrgUser = request.orguser
    if orguser.org.dbt is None:
        raise HttpError(400, "create a dbt workspace first")

    org = orguser.org

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        raise HttpError(400, "need to set up a warehouse first")
    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    if org.dbt.dbt_venv is None:
        org.dbt.dbt_venv = DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH
        org.dbt.save()

    # create a secret block to save the github endpoint url along with token
    try:
        gitrepo_access_token = None
        if org.dbt.gitrepo_access_token_secret:
            gitrepo_access_token = secretsmanager.retrieve_github_pat(
                org.dbt.gitrepo_access_token_secret
            )
        gitrepo_url = org.dbt.gitrepo_url

        if gitrepo_access_token is not None and gitrepo_access_token != "":
            gitrepo_url = gitrepo_url.replace(
                "github.com", "oauth2:" + gitrepo_access_token + "@github.com"
            )

            # store the git oauth endpoint with token in a prefect secret block
            secret_block = PrefectSecretBlockEdit(
                block_name=f"{org.slug}-git-pull-url",
                secret=gitrepo_url,
            )
            block_response = prefect_service.upsert_secret_block(secret_block)
            # prefect-proxy sanitizes the block name; always store what it persisted
            stored_block_name = block_response["block_name"]

            if not OrgPrefectBlockv1.objects.filter(
                org=org, block_type=SECRET, block_name=stored_block_name
            ).exists():
                OrgPrefectBlockv1.objects.create(
                    org=org,
                    block_type=SECRET,
                    block_name=stored_block_name,
                    block_id=block_response["block_id"],
                )

    except Exception as error:
        logger.exception(error)
        raise HttpError(400, str(error)) from error

    dbt_project_params: DbtProjectParams = DbtProjectManager.gather_dbt_project_params(org, org.dbt)

    # create org tasks for the transformation page
    _, error = create_default_transform_tasks(org, dbt_project_params)
    if error:
        raise HttpError(400, error)

    create_audit_log(
        org=org,
        orguser=orguser,
        resource_type=AuditLogResourceType.DBT,
        resource_id=str(org.dbt.id),
        action=AuditLogAction.CREATE,
    )

    return {"success": 1}


@orgtask_router.get("elementary-lock/")
@has_permission(["can_view_orgtasks"])
def get_elemetary_task_lock(request):
    """Check if the elementary report generation task is underway"""
    org: Org = request.orguser.org
    org_task = get_edr_send_report_task(org)
    lock = TaskLock.objects.filter(orgtask=org_task).first()
    return fetch_orgtask_lock_v1(org_task, lock)


@orgtask_router.get("transform/")
@has_permission(["can_view_orgtasks"])
def get_prefect_transformation_tasks(request, include_edr: bool = False):
    """Fetch manual (Transform-tab) dbt deployments for an org. Each response
    row represents one runnable deployment; the "primary" orgtask (the last
    one in the chain by seq, ignoring auto-managed dependencies) determines
    the label/slug/command/lock/uuid shown.

    `include_edr=true` opts the generate-edr deployment into the response —
    used by the pipeline form picker. Defaults to False so the Transform tab
    and other callers don't see EDR."""
    orguser: OrgUser = request.orguser

    auto_managed_task_slugs = set(TRANSFORM_TASKS_DEPENDENCIES)

    dataflows = OrgDataFlowv1.objects.filter(
        org=orguser.org,
        dataflow_type="manual",
    ).prefetch_related("datafloworgtasks__orgtask__task")

    primaries = []
    for dataflow in dataflows:
        primary_dfot = None
        for dfot in sorted(
            dataflow.datafloworgtasks.all(),
            key=lambda d: d.seq,
            reverse=True,
        ):
            slug = dfot.orgtask.task.slug
            if slug in TRANSFORM_TASKS_SEQ and slug not in auto_managed_task_slugs:
                primary_dfot = dfot
                break
        if primary_dfot is None:
            continue
        if primary_dfot.orgtask.task.slug == TASK_GENERATE_EDR and not include_edr:
            continue
        primaries.append((dataflow, primary_dfot.orgtask))

    # gather all orgtask ids across all chained deployments so a lock held on
    # any chain orgtask (including shared prep tasks locked by other dataflows)
    # surfaces on the deployment — matches orchestrated pipeline lock behavior
    all_orgtask_ids = set()
    for dataflow, _ in primaries:
        for dfot in dataflow.datafloworgtasks.all():
            all_orgtask_ids.add(dfot.orgtask_id)

    all_locks = list(TaskLock.objects.filter(orgtask_id__in=all_orgtask_ids))

    res = []
    for dataflow, primary in primaries:
        command = None
        if primary.task.type != TaskType.EDR:
            command = primary.task.type + " " + primary.get_task_parameters()

        chain_ids = {dfot.orgtask_id for dfot in dataflow.datafloworgtasks.all()}
        matching_locks = [lock for lock in all_locks if lock.orgtask_id in chain_ids]
        lock = matching_locks[0] if matching_locks else None

        res.append(
            {
                "label": primary.task.label,
                "slug": primary.task.slug,
                "id": primary.id,
                "uuid": primary.uuid,
                "deploymentId": dataflow.deployment_id,
                "lock": fetch_pipeline_lock_v1(dataflow, lock),
                "command": command,
                "generated_by": primary.generated_by,
                "seq": TRANSFORM_TASKS_SEQ[primary.task.slug],
                "pipeline_default": primary.task.slug in DEFAULT_TRANSFORM_TASKS_IN_PIPELINE,
            }
        )

    return sorted(res, key=lambda x: x["seq"])


@orgtask_router.delete("transform/")
@has_permission(["can_delete_orgtask"])
def delete_system_transformation_tasks(request):
    """delete tasks and related objects for an org"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    secret_block = OrgPrefectBlockv1.objects.filter(
        org=org,
        block_type=SECRET,
    ).first()
    if secret_block:
        logger.info("deleting secret block %s", secret_block.block_name)
        prefect_service.delete_secret_block(secret_block.block_id)
        secret_block.delete()

    orgdbt = org.dbt
    if orgdbt is None:
        raise HttpError(400, "dbt is not configured for this client")

    for org_task in OrgTask.objects.filter(dbt=orgdbt, task__is_system=True).all():
        _, error = delete_orgtask(org_task)

        if error:
            logger.info(
                f"Failed deleting orgtask with id {org_task.id} of type {org_task.task.slug}. Skipping and continuing to next task deletion"
            )
            continue

    return {"success": 1}


@orgtask_router.delete("{orgtask_uuid}/")
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

    if org_task.task.type not in [TaskType.DBT, TaskType.GIT, TaskType.EDR]:
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

    task_uuid = str(org_task.uuid)
    task_slug = org_task.task.slug

    _, error = delete_orgtask(org_task)

    if error:
        logger.info(
            f"Failed deleting orgtask with id {org_task.id} of type {task_slug}. Skipping and continuing to next task deletion"
        )
        raise HttpError(400, error)

    create_audit_log(
        org=orguser.org,
        orguser=orguser,
        resource_type=AuditLogResourceType.DBT,
        resource_id=task_uuid,
        action=AuditLogAction.DELETE,
        resource_fields={"name": task_slug},
    )

    return {"success": 1}
