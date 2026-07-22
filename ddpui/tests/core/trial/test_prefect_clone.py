"""Tests for ddpui/core/trial/prefect_clone.py — rebuilding template orchestrate pipelines
on the trial org (Step 7 / P5)."""

from unittest.mock import patch

import pytest

from ddpui.core.trial import prefect_clone
from ddpui.core.trial.exceptions import TrialCloneError
from ddpui.models.org import Org, OrgDbt, OrgDataFlowv1
from ddpui.models.tasks import DataflowOrgTask, OrgTask, Task, TaskType

pytestmark = pytest.mark.django_db


def _make_orgdbt(slug: str) -> OrgDbt:
    return OrgDbt.objects.create(
        gitrepo_url=f"https://github.com/dalgo/{slug}.git",
        project_dir=f"test_project_dir_{slug}",
        dbt_venv="test_venv",
        target_type="postgres",
        default_schema="default_schema",
        transform_type="github",
    )


def _make_orgs():
    template = Org.objects.create(name="tmpl-pf", slug="tmpl-pf")
    trial_org = Org.objects.create(name="Trial pf", slug="trial-pf")
    trial_org.dbt = _make_orgdbt("trial-pf")
    trial_org.save()
    return template, trial_org


def _make_template_dataflow_with_tasks(template: Org):
    """One template orchestrate OrgDataFlowv1 with a sync OrgTask (connection_id set) and a
    dbt-run OrgTask (no connection_id), both linked via DataflowOrgTask."""
    sync_task = Task.objects.create(
        type=TaskType.AIRBYTE, slug="airbyte-sync", label="AIRBYTE sync"
    )
    dbt_task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-run", label="DBT run", command="run"
    )

    sync_orgtask = OrgTask.objects.create(org=template, task=sync_task, connection_id="tmpl-conn-1")
    dbt_orgtask = OrgTask.objects.create(org=template, task=dbt_task, dbt=template.dbt)

    dataflow = OrgDataFlowv1.objects.create(
        org=template,
        name="daily-sync",
        deployment_name="tmpl-deployment",
        deployment_id="tmpl-dep-1",
        cron="0 1 * * *",
        dataflow_type="orchestrate",
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=sync_orgtask, seq=0)
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=dbt_orgtask, seq=1)
    return dataflow, sync_orgtask, dbt_orgtask


@patch("ddpui.core.trial.prefect_clone.prefect_service")
def test_build_pipeline_payload_remaps_connection_and_resolves_transform_task(
    mock_prefect_service,
):
    mock_prefect_service.get_deployment.return_value = {"parameters": {}}
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf")
    template.save()
    dataflow, sync_orgtask, dbt_orgtask = _make_template_dataflow_with_tasks(template)

    connection_map = {"tmpl-conn-1": "trial-conn-1"}

    payload = prefect_clone.build_pipeline_payload(dataflow, trial_org, connection_map)

    assert payload.name == "daily-sync"
    assert payload.cron == "0 1 * * *"
    assert len(payload.connections) == 1
    assert payload.connections[0].id == "trial-conn-1"
    assert payload.connections[0].seq == 0

    # the trial org's dbt-run OrgTask gets minted (get-or-create by task slug) since P4 never
    # creates it, and its uuid (not the template's) is what's referenced.
    trial_dbt_orgtask = OrgTask.objects.get(org=trial_org, task__slug="dbt-run")
    assert trial_dbt_orgtask.uuid != dbt_orgtask.uuid
    assert len(payload.transformTasks) == 1
    assert payload.transformTasks[0].uuid == str(trial_dbt_orgtask.uuid)
    assert payload.transformTasks[0].seq == 0


def test_resolve_transform_orgtask_copies_template_parameters_onto_existing():
    """Step 6 (create_default_transform_tasks) mints the trial dbt-run OrgTask param-less before
    Step 7 runs, so get_or_create finds it and its `defaults` never apply — resolve must copy the
    template's parameters onto the existing row, else the cloned deployment drops --select etc."""
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf")
    template.save()
    dbt_task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-run", label="DBT run", command="run"
    )
    template_orgtask = OrgTask.objects.create(
        org=template, task=dbt_task, dbt=template.dbt, parameters={"flags": ["--select", "foo"]}
    )
    # simulate Step 6 having already minted the trial org's dbt-run OrgTask, param-less
    existing = OrgTask.objects.create(
        org=trial_org, task=dbt_task, dbt=trial_org.dbt, parameters={}
    )

    resolved = prefect_clone._resolve_trial_transform_orgtask(template_orgtask, trial_org)

    assert resolved.id == existing.id  # reused the existing row, not a fresh mint
    existing.refresh_from_db()
    assert existing.parameters == {"flags": ["--select", "foo"]}


def test_resolve_transform_orgtask_keeps_parameterized_variants_distinct():
    """Production supports multiple OrgTask rows per task slug with different `parameters`
    (e.g. a dbt-run per pipeline with different --select). Resolving must NOT collapse them
    onto one trial row (last-processed template task would clobber the earlier pipeline's
    params) — each distinct template `parameters` maps to its own trial OrgTask."""
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf")
    template.save()
    dbt_task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-run", label="DBT run", command="run"
    )
    template_a = OrgTask.objects.create(
        org=template, task=dbt_task, dbt=template.dbt, parameters={"flags": ["--select", "a"]}
    )
    template_b = OrgTask.objects.create(
        org=template, task=dbt_task, dbt=template.dbt, parameters={"flags": ["--select", "b"]}
    )
    # Step 6 pre-seeded one param-less row
    OrgTask.objects.create(org=trial_org, task=dbt_task, dbt=trial_org.dbt, parameters={})

    resolved_a = prefect_clone._resolve_trial_transform_orgtask(template_a, trial_org)
    resolved_b = prefect_clone._resolve_trial_transform_orgtask(template_b, trial_org)

    assert resolved_a.id != resolved_b.id
    resolved_a.refresh_from_db()
    resolved_b.refresh_from_db()
    assert resolved_a.parameters == {"flags": ["--select", "a"]}
    assert resolved_b.parameters == {"flags": ["--select", "b"]}
    assert OrgTask.objects.filter(org=trial_org, task=dbt_task).count() == 2

    # resolving again is idempotent — no third row
    again_a = prefect_clone._resolve_trial_transform_orgtask(template_a, trial_org)
    assert again_a.id == resolved_a.id
    assert OrgTask.objects.filter(org=trial_org, task=dbt_task).count() == 2


def test_build_pipeline_payload_raises_when_connection_not_remapped():
    template, trial_org = _make_orgs()
    dataflow, sync_orgtask, dbt_orgtask = _make_template_dataflow_with_tasks(template)

    # TrialCloneError subclasses RuntimeError, so callers catching RuntimeError still work —
    # but the feature's own exception type is what the module raises.
    with pytest.raises(TrialCloneError, match="no remapped trial connection"):
        prefect_clone.build_pipeline_payload(dataflow, trial_org, connection_map={})


@patch("ddpui.core.trial.prefect_clone.prefect_service")
def test_build_pipeline_payload_skips_auto_managed_dbt_tasks(mock_prefect_service):
    mock_prefect_service.get_deployment.return_value = {"parameters": {}}
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf2")
    template.save()

    dbt_clean_task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-clean", label="DBT clean", command="clean"
    )
    dbt_clean_orgtask = OrgTask.objects.create(org=template, task=dbt_clean_task, dbt=template.dbt)
    dataflow = OrgDataFlowv1.objects.create(
        org=template,
        name="clean-only",
        deployment_name="tmpl-deployment-2",
        deployment_id="tmpl-dep-2",
        cron="0 2 * * *",
        dataflow_type="orchestrate",
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=dbt_clean_orgtask, seq=0)

    payload = prefect_clone.build_pipeline_payload(dataflow, trial_org, connection_map={})

    assert payload.connections == []
    assert payload.transformTasks == []
    assert not OrgTask.objects.filter(org=trial_org, task__slug="dbt-clean").exists()


@patch("ddpui.core.trial.prefect_clone.prefect_service")
def test_build_pipeline_payload_carries_template_continue_on_sync_failure(mock_prefect_service):
    """The template's continueOnSyncFailure lives in its Prefect deployment parameters (same
    place get_pipeline_details reads it) — the cloned pipeline must inherit it, not a hardcoded
    False."""
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf")
    template.save()
    dataflow, _, _ = _make_template_dataflow_with_tasks(template)
    mock_prefect_service.get_deployment.return_value = {
        "parameters": {"config": {"continue_on_sync_failure": True}}
    }

    payload = prefect_clone.build_pipeline_payload(
        dataflow, trial_org, connection_map={"tmpl-conn-1": "trial-conn-1"}
    )

    mock_prefect_service.get_deployment.assert_called_once_with("tmpl-dep-1")
    assert payload.continueOnSyncFailure is True


@patch("ddpui.core.trial.prefect_clone.prefect_service")
def test_build_pipeline_payload_defaults_continue_on_sync_failure_false(mock_prefect_service):
    """Deployment parameters without the config key default to False — same default as
    get_pipeline_details."""
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf")
    template.save()
    dataflow, _, _ = _make_template_dataflow_with_tasks(template)
    mock_prefect_service.get_deployment.return_value = {"parameters": {}}

    payload = prefect_clone.build_pipeline_payload(
        dataflow, trial_org, connection_map={"tmpl-conn-1": "trial-conn-1"}
    )

    assert payload.continueOnSyncFailure is False


@patch("ddpui.core.trial.prefect_clone.prefect_service")
@patch("ddpui.core.trial.prefect_clone.PipelineService")
def test_clone_orchestrate_dataflows_calls_create_pipeline_per_template_dataflow(
    mock_pipeline_service,
    mock_prefect_service,
):
    mock_prefect_service.get_deployment.return_value = {"parameters": {}}
    template, trial_org = _make_orgs()
    template.dbt = _make_orgdbt("tmpl-pf3")
    template.save()
    dataflow, sync_orgtask, dbt_orgtask = _make_template_dataflow_with_tasks(template)

    mock_pipeline_service.create_pipeline.return_value = {"deploymentId": "dep-new"}
    connection_map = {"tmpl-conn-1": "trial-conn-1"}

    deployment_ids = prefect_clone.clone_orchestrate_dataflows(template, trial_org, connection_map)

    mock_pipeline_service.create_pipeline.assert_called_once()
    called_org, called_payload = mock_pipeline_service.create_pipeline.call_args[0]
    assert called_org == trial_org
    assert called_payload.connections[0].id == "trial-conn-1"
    assert deployment_ids == ["dep-new"]


# ---------------------------------------------------------------------------
# sync_transform_tasks_and_deployments (Step 6b)
# ---------------------------------------------------------------------------


def _make_trial_with_cli_block():
    """trial org whose OrgDbt has a cli_profile_block, as step 5 guarantees before step 6."""
    from ddpui.models.org import OrgPrefectBlockv1

    template = Org.objects.create(name="tmpl-sync", slug="tmpl-sync")
    template.dbt = _make_orgdbt("tmpl-sync")
    template.save()
    trial_org = Org.objects.create(name="Trial sync", slug="trial-sync")
    trial_dbt = _make_orgdbt("trial-sync")
    cli_block = OrgPrefectBlockv1.objects.create(
        org=trial_org, block_type="dbt cli profile", block_name="trial-sync-cli", block_id="b-1"
    )
    trial_dbt.cli_profile_block = cli_block
    trial_dbt.save()
    trial_org.dbt = trial_dbt
    trial_org.save()
    return template, trial_org


@patch("ddpui.core.trial.prefect_clone.prefect_service")
@patch("ddpui.core.trial.prefect_clone.pipeline_with_orgtasks")
@patch("ddpui.core.trial.prefect_clone.create_prefect_deployment_for_dbtcore_task")
@patch("ddpui.core.trial.prefect_clone.DbtProjectManager")
def test_sync_transform_tasks_copies_standalone_params_and_fixes_deployments(
    mock_dbt_mgr, mock_create_deployment, mock_pipeline_with, mock_prefect_service
):
    """(a) a template dbt-run OrgTask with parameters that sits in NO dataflow must still reach
    the trial (adopting the param-less step-5 row); (b) the manual deployment that step 5 baked
    with empty params must be rebaked with the copied params; (c) a second parameter variant
    minted fresh must get a manual deployment created (step 5 never made one for it)."""
    template, trial_org = _make_trial_with_cli_block()
    dbt_task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-run", label="DBT run", command="run"
    )

    # template: standalone parameterized dbt-run (linked to no dataflow) + a second variant
    OrgTask.objects.create(
        org=template, task=dbt_task, dbt=template.dbt, parameters={"options": {"select": "marts"}}
    )
    OrgTask.objects.create(
        org=template, task=dbt_task, dbt=template.dbt, parameters={"options": {"select": "staging"}}
    )

    # trial: the param-less dbt-run row step 5 created, with its manual deployment already baked
    trial_run_task = OrgTask.objects.create(org=trial_org, task=dbt_task, dbt=trial_org.dbt)
    manual_flow = OrgDataFlowv1.objects.create(
        org=trial_org,
        name="manual-run",
        deployment_name="manual-trial-sync-dbt-run-abc",
        deployment_id="manual-dep-1",
        dataflow_type="manual",
    )
    DataflowOrgTask.objects.create(dataflow=manual_flow, orgtask=trial_run_task, seq=0)

    mock_pipeline_with.return_value = ([{"slug": "dbt-run", "seq": 0}], None)
    mock_dbt_mgr.gather_dbt_project_params.return_value = object()

    result = prefect_clone.sync_transform_tasks_and_deployments(template, trial_org)

    # (a) adoption: the step-5 row now carries the first template variant's params
    trial_run_task.refresh_from_db()
    assert trial_run_task.parameters == {"options": {"select": "marts"}}
    # second variant minted as its own trial row
    trial_variants = OrgTask.objects.filter(org=trial_org, task=dbt_task)
    assert trial_variants.count() == 2
    assert {str(t.parameters) for t in trial_variants} == {
        str({"options": {"select": "marts"}}),
        str({"options": {"select": "staging"}}),
    }

    # (b) existing manual deployment rebaked with the adopted params
    mock_prefect_service.update_dataflow_v1.assert_called_once()
    dep_id, update_payload = mock_prefect_service.update_dataflow_v1.call_args[0]
    assert dep_id == "manual-dep-1"
    assert update_payload.deployment_params["config"]["tasks"] == [{"slug": "dbt-run", "seq": 0}]

    # (c) fresh variant got a manual deployment created
    mock_create_deployment.assert_called_once()
    created_orgtask = mock_create_deployment.call_args[0][0]
    assert created_orgtask.parameters == {"options": {"select": "staging"}}

    assert result["manual_deployments_created"] == 1
    assert result["manual_deployments_rebaked"] == 1
    assert result["transform_orgtasks_synced"] == 2
