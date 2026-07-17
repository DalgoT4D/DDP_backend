"""Tests for ddpui/core/trial/prefect_clone.py — rebuilding template orchestrate pipelines
on the trial org (Step 7 / P5)."""

from unittest.mock import patch

import pytest

from ddpui.core.trial import prefect_clone
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


def test_build_pipeline_payload_remaps_connection_and_resolves_transform_task():
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


def test_build_pipeline_payload_raises_when_connection_not_remapped():
    template, trial_org = _make_orgs()
    dataflow, sync_orgtask, dbt_orgtask = _make_template_dataflow_with_tasks(template)

    with pytest.raises(RuntimeError, match="no remapped trial connection"):
        prefect_clone.build_pipeline_payload(dataflow, trial_org, connection_map={})


def test_build_pipeline_payload_skips_auto_managed_dbt_tasks():
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


@patch("ddpui.core.trial.prefect_clone.PipelineService")
def test_clone_orchestrate_dataflows_calls_create_pipeline_per_template_dataflow(
    mock_pipeline_service,
):
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
