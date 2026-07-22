"""Rebuild the template org's Prefect orchestrate deployments (pipelines) on the trial org.

`OrgTask.connection_id`, prefect block names, and `deployment_id` are all fresh-per-org — so a
template `OrgDataFlowv1(dataflow_type="orchestrate")` row is never id-copied. Instead, for each
template orchestrate dataflow, this module reconstructs a `PrefectDataFlowCreateSchema4` payload
that references the TRIAL org's own connections/OrgTasks and hands it to
`PipelineService.create_pipeline`, which mints a fresh prefect deployment + `OrgDataFlowv1` +
`DataflowOrgTask` rows on the trial org.

Payload shape (`ddpui/ddpprefect/schema.py`), confirmed by reading `PipelineService` before
writing this module:
- `connections`: `PrefectFlowAirbyteConnection2(id, seq)` — `id` is an Airbyte **connection id**
  (not an OrgTask id); `PipelineService._build_sync_tasks` resolves it via
  `OrgTask.objects.filter(org=org, connection_id=connection.id, task__slug=TASK_AIRBYTESYNC)`.
- `transformTasks`: `PrefectDataFlowOrgTasks(uuid, seq)` — `uuid` is an **OrgTask.uuid**;
  `PipelineService._build_transform_tasks` resolves it via
  `OrgTask.objects.filter(uuid=transform_task.uuid, org=org)` and requires that OrgTask to
  already exist on `org`. git/dbt-clean/dbt-deps tasks are the exception: those are auto-managed
  and auto-created on demand by `PipelineService._get_or_create_*`, so this module never puts
  them in the payload at all — including them would just make `_build_transform_tasks` log a
  warning and skip them.

Gap this module papers over: `_step_dbt` (P4) copies the template's dbt DAG rows
(OrgDbtModel/CanvasNode/etc.) but — same as any org that hasn't opened the Transform page yet —
does NOT create the org's transform OrgTasks (git-pull/dbt-clean/dbt-deps/dbt-run); those are
normally minted by `create_default_transform_tasks` (`ddpui/core/orgtaskfunctions.py`) the first
time a user hits that page (`orgtask_api.py`). So the trial org has no `dbt-run` OrgTask yet when
this step runs, and `dbt-run` (unlike git-pull/dbt-clean/dbt-deps) is NOT one of the tasks
`PipelineService` mints on demand. This module therefore mints the trial org's equivalent
transform OrgTask itself — get-or-create by task slug, mirroring the exact
`OrgTask.objects.get_or_create(org=org, task=task, dbt=orgdbt, defaults={"parameters": {}})`
pattern `PipelineService._get_or_create_dbt_clean_orgtask` (etc.) already use for the
auto-managed ones.
"""

from ddpui.core.orchestrate.pipeline_service import PipelineService
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.core.orgtaskfunctions import create_prefect_deployment_for_dbtcore_task
from ddpui.core.pipelinefunctions import pipeline_with_orgtasks
from ddpui.core.trial.exceptions import TrialCloneError
from ddpui.ddpprefect import prefect_service
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema4,
    PrefectDataFlowOrgTasks,
    PrefectDataFlowUpdateSchema3,
    PrefectFlowAirbyteConnection2,
)
from ddpui.models.org import Org, OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask, OrgTask, TaskType
from ddpui.utils.constants import LONG_RUNNING_TASKS, TASK_DBTCLEAN, TASK_DBTDEPS
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.prefect_clone")

# these are minted on demand by PipelineService._build_transform_tasks itself (and git tasks are
# filtered out there entirely) — never put them in a built payload.
_AUTO_MANAGED_TRANSFORM_SLUGS = {TASK_DBTCLEAN, TASK_DBTDEPS}


def _resolve_trial_transform_orgtask(template_orgtask: OrgTask, trial_org: Org) -> OrgTask:
    """Resolve the trial org's equivalent transform OrgTask for a template transform OrgTask,
    matched by `task` (shared system-wide `Task` FK) AND `parameters` — production supports
    multiple OrgTask rows per slug with different parameters (e.g. one dbt-run per pipeline
    with different --select), so matching on the FK alone would collapse them onto one trial
    row and the last-processed template task would clobber the earlier pipeline's flags.

    Resolution order:
    1. exact match on (task, parameters) — idempotent re-resolve;
    2. adopt a param-less row (Step 6 `create_default_transform_tasks` mints one before this
       step runs) by copying the template's parameters onto it — keeps the Transform page from
       showing a stray param-less task alongside the parameterized one;
    3. mint a fresh row (second/third parameter variant, or step ordering changed).
    """
    trial_dbt = trial_org.dbt
    if trial_dbt is None:
        raise TrialCloneError(
            f"trial org {trial_org.slug} has no dbt workspace to attach transform tasks to"
        )
    slug = template_orgtask.task.slug
    template_params = template_orgtask.parameters

    org_task = OrgTask.objects.filter(
        org=trial_org, task=template_orgtask.task, dbt=trial_dbt, parameters=template_params
    ).first()
    if org_task is not None:
        return org_task

    if template_params:
        org_task = OrgTask.objects.filter(
            org=trial_org, task=template_orgtask.task, dbt=trial_dbt, parameters={}
        ).first()
        if org_task is not None:
            org_task.parameters = template_params
            org_task.save(update_fields=["parameters"])
            logger.info(
                f"copied template parameters onto existing trial OrgTask {org_task.uuid} "
                f"(slug={slug}) for org {trial_org.slug}"
            )
            return org_task

    org_task = OrgTask.objects.create(
        org=trial_org,
        task=template_orgtask.task,
        dbt=trial_dbt,
        parameters=template_params,
    )
    logger.info(
        f"minted trial transform OrgTask {org_task.uuid} (slug={slug}) " f"for org {trial_org.slug}"
    )
    return org_task


def build_pipeline_payload(
    template_dataflow: OrgDataFlowv1, trial_org: Org, connection_map: dict
) -> PrefectDataFlowCreateSchema4:
    """Build the `PrefectDataFlowCreateSchema4` payload to recreate `template_dataflow` on
    `trial_org`: sync-task connections remapped via `connection_map`
    (`{template_connection_id: trial_connection_id}`, built by `_step_connections`/P3), transform
    tasks resolved/minted on the trial org by task slug. Never copies the template's connection
    ids, OrgTask uuids, or deployment id — those are all per-org/per-workspace.
    """
    linked = (
        DataflowOrgTask.objects.filter(dataflow=template_dataflow)
        .select_related("orgtask", "orgtask__task")
        .order_by("seq")
    )

    connections: list[PrefectFlowAirbyteConnection2] = []
    transform_tasks: list[PrefectDataFlowOrgTasks] = []

    for link in linked:
        template_orgtask = link.orgtask
        if template_orgtask.connection_id:
            trial_connection_id = connection_map.get(template_orgtask.connection_id)
            if not trial_connection_id:
                raise TrialCloneError(
                    f"no remapped trial connection for template connection "
                    f"{template_orgtask.connection_id} (dataflow {template_dataflow.name})"
                )
            connections.append(
                PrefectFlowAirbyteConnection2(id=trial_connection_id, seq=len(connections))
            )
        elif (
            template_orgtask.task.type == TaskType.DBT
            and template_orgtask.task.slug not in _AUTO_MANAGED_TRANSFORM_SLUGS
        ):
            trial_orgtask = _resolve_trial_transform_orgtask(template_orgtask, trial_org)
            transform_tasks.append(
                PrefectDataFlowOrgTasks(uuid=str(trial_orgtask.uuid), seq=len(transform_tasks))
            )
        # git-pull/git-clone and dbt-clean/dbt-deps template links are auto-managed —
        # PipelineService re-adds its own copies of these; skip them here.

    # continueOnSyncFailure lives in the template's Prefect deployment parameters, not on
    # OrgDataFlowv1 — read it the same way PipelineService.get_pipeline_details does
    # (parameters.config.continue_on_sync_failure, default False).
    template_deployment = prefect_service.get_deployment(template_dataflow.deployment_id)
    continue_on_sync_failure = (
        template_deployment.get("parameters", {})
        .get("config", {})
        .get("continue_on_sync_failure", False)
    )

    return PrefectDataFlowCreateSchema4(
        name=template_dataflow.name,
        connections=connections,
        cron=template_dataflow.cron or "",
        transformTasks=transform_tasks,
        continueOnSyncFailure=continue_on_sync_failure,
    )


def _rebake_manual_deployment_params(
    dataflow: OrgDataFlowv1, org: Org, cli_block, dbt_project_params
) -> None:
    """Rebuild a manual (Transform-page) deployment's baked `deployment_params` from its linked
    OrgTasks' CURRENT parameters and push them to Prefect.

    `create_default_transform_tasks` (Step 5) bakes `org_task.get_task_parameters()` into each
    LONG_RUNNING manual deployment at creation time — while the OrgTask still has `parameters={}`.
    Step 6's dataflow pass then copies the template's parameters (e.g. `dbt run --select X`) onto
    that OrgTask, leaving the already-created deployment running bare `dbt run`. This re-runs the
    exact task-config build the creation path used (`pipeline_with_orgtasks` over the deployment's
    own git→clean→deps→primary chain) and updates the deployment in place.
    """
    links = (
        DataflowOrgTask.objects.filter(dataflow=dataflow)
        .select_related("orgtask", "orgtask__task")
        .order_by("seq")
    )
    chain = [link.orgtask for link in links]
    task_configs, err = pipeline_with_orgtasks(
        org,
        chain,
        cli_block=cli_block,
        dbt_project_params=dbt_project_params,
        gitrepo_url=org.dbt.gitrepo_url if org.dbt else None,
    )
    if err:
        raise TrialCloneError(
            f"failed to rebuild task configs for manual deployment "
            f"{dataflow.deployment_name}: {err}"
        )
    prefect_service.update_dataflow_v1(
        dataflow.deployment_id,
        PrefectDataFlowUpdateSchema3(
            deployment_params={"config": {"tasks": task_configs, "org_slug": org.slug}},
            cron=dataflow.cron,
        ),
    )
    logger.info(f"rebaked params for manual deployment {dataflow.deployment_name}")


def sync_transform_tasks_and_deployments(template: Org, trial_org: Org) -> dict:
    """Step 6b — run AFTER `clone_orchestrate_dataflows`. Closes two gaps the dataflow pass
    leaves:

    (a) STANDALONE template transform OrgTasks: the dataflow pass only sees OrgTasks linked into
        an orchestrate dataflow via `DataflowOrgTask` — a template transform task living outside
        every pipeline (e.g. a client-created `dbt run --select X` used only from the Transform
        page) would otherwise never reach the trial. Resolved through the same
        `_resolve_trial_transform_orgtask` (idempotent on the ones the dataflow pass already
        handled).

    (b) MANUAL deployment params: ensures every LONG_RUNNING trial transform OrgTask has a manual
        deployment whose baked params match the OrgTask's — creating deployments for freshly
        minted parameter-variant rows (Step 5 only creates them for its own param-less rows) and
        rebaking deployments created before the template's parameters were copied on (see
        `_rebake_manual_deployment_params`).

    Returns counts for the run manifest.
    """
    trial_dbt = trial_org.dbt
    if trial_dbt is None or trial_dbt.cli_profile_block is None:
        raise TrialCloneError(
            f"trial org {trial_org.slug} has no dbt workspace/cli profile block; "
            "step 5 must run before transform-task sync"
        )

    standalone_seen = 0
    for template_orgtask in (
        OrgTask.objects.filter(org=template, task__type=TaskType.DBT)
        .exclude(task__slug__in=_AUTO_MANAGED_TRANSFORM_SLUGS)
        .select_related("task")
    ):
        _resolve_trial_transform_orgtask(template_orgtask, trial_org)
        standalone_seen += 1

    cli_block = trial_dbt.cli_profile_block
    dbt_project_params = DbtProjectManager.gather_dbt_project_params(trial_org, trial_dbt)
    deployments_created = 0
    deployments_rebaked = 0
    for org_task in OrgTask.objects.filter(
        org=trial_org, task__type=TaskType.DBT, task__slug__in=LONG_RUNNING_TASKS
    ).select_related("task"):
        manual_link = (
            DataflowOrgTask.objects.filter(orgtask=org_task, dataflow__dataflow_type="manual")
            .select_related("dataflow")
            .first()
        )
        if manual_link is None:
            create_prefect_deployment_for_dbtcore_task(org_task, cli_block, dbt_project_params)
            deployments_created += 1
        elif org_task.parameters:
            _rebake_manual_deployment_params(
                manual_link.dataflow, trial_org, cli_block, dbt_project_params
            )
            deployments_rebaked += 1

    return {
        "transform_orgtasks_synced": standalone_seen,
        "manual_deployments_created": deployments_created,
        "manual_deployments_rebaked": deployments_rebaked,
    }


def clone_orchestrate_dataflows(template: Org, trial_org: Org, connection_map: dict) -> list[str]:
    """Rebuild every template orchestrate `OrgDataFlowv1` on `trial_org` via
    `PipelineService.create_pipeline`. Returns the list of newly-minted deployment ids."""
    deployment_ids: list[str] = []
    template_dataflows = OrgDataFlowv1.objects.filter(org=template, dataflow_type="orchestrate")
    for dataflow in template_dataflows:
        payload = build_pipeline_payload(dataflow, trial_org, connection_map)
        result = PipelineService.create_pipeline(trial_org, payload)
        deployment_ids.append(result["deploymentId"])
        logger.info(
            f"recreated orchestrate pipeline '{dataflow.name}' on trial org {trial_org.slug}: "
            f"deployment_id={result['deploymentId']}"
        )
    return deployment_ids
