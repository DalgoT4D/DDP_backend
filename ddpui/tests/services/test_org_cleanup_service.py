import pytest
from unittest.mock import patch, MagicMock
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.tasks import OrgTask, Task, OrgDataFlowv1, DataflowOrgTask
from ddpui.services.org_cleanup_service import OrgCleanupService, OrgCleanupServiceError


pytestmark = pytest.mark.django_db


def test_delete_warehouse_full_flow():
    # Setup Org
    org = Org.objects.create(name="TestOrg", airbyte_workspace_id="workspace123", slug="test-slug")

    # Setup OrgWarehouse
    warehouse = OrgWarehouse.objects.create(
        org=org, wtype="postgres", credentials="{}", airbyte_destination_id="dest123"
    )

    # Setup Task and OrgTask (airbyte type)
    task_sync = Task.objects.create(
        type="airbyte", slug="sync", label="Sync", command="sync", is_system=True
    )
    orgtask_sync = OrgTask.objects.create(
        org=org, task=task_sync, connection_id="conn123", generated_by="system"
    )

    # Setup Task and OrgTask (dbt type)
    task_dbt = Task.objects.create(
        type="dbt", slug="dbt", label="DBT", command="dbt run", is_system=True
    )
    orgtask_dbt = OrgTask.objects.create(org=org, task=task_dbt, generated_by="system")

    # Setup OrgDataFlowv1 and DataflowOrgTask (manual dataflow with airbyte tasks)
    dataflow_manual = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="manual", deployment_id="deploy1", deployment_name="df1"
    )
    DataflowOrgTask.objects.create(dataflow=dataflow_manual, orgtask=orgtask_sync)

    # Setup orchestrate pipeline with both sync and dbt tasks
    dataflow_orchestrate = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="orchestrate", deployment_id="deploy2", deployment_name="df2"
    )
    dfo_sync = DataflowOrgTask.objects.create(dataflow=dataflow_orchestrate, orgtask=orgtask_sync)
    dfo_dbt = DataflowOrgTask.objects.create(dataflow=dataflow_orchestrate, orgtask=orgtask_dbt)

    # Patch airbyte_service, secretsmanager, prefect_service
    with patch("ddpui.services.org_cleanup_service.airbyte_service") as airbyte_service, patch(
        "ddpui.services.org_cleanup_service.secretsmanager"
    ) as secretsmanager, patch(
        "ddpui.services.org_cleanup_service.prefect_service"
    ) as prefect_service:
        airbyte_service.delete_connection = MagicMock()
        airbyte_service.delete_destination = MagicMock()
        secretsmanager.delete_warehouse_credentials = MagicMock()
        prefect_service.delete_deployment_by_id = MagicMock()

        # Run service
        service = OrgCleanupService(org, dry_run=False)
        service.delete_warehouse()

        # Assert manual pipeline deleted
        prefect_service.delete_deployment_by_id.assert_any_call(dataflow_manual.deployment_id)
        assert OrgDataFlowv1.objects.filter(id=dataflow_manual.id).count() == 0

        # Assert orchestrate pipeline still exists
        assert OrgDataFlowv1.objects.filter(id=dataflow_orchestrate.id).count() == 1
        # The sync orgtask should be removed from orchestrate pipeline
        assert (
            DataflowOrgTask.objects.filter(
                dataflow=dataflow_orchestrate, orgtask=orgtask_sync
            ).count()
            == 0
        )
        # The dbt orgtask should still be present in orchestrate pipeline
        assert (
            DataflowOrgTask.objects.filter(
                dataflow=dataflow_orchestrate, orgtask=orgtask_dbt
            ).count()
            == 1
        )

        # Assert airbyte connection deleted
        airbyte_service.delete_connection.assert_called_once_with(
            org.airbyte_workspace_id, orgtask_sync.connection_id
        )
        # Assert orgtask_sync deleted
        assert OrgTask.objects.filter(id=orgtask_sync.id).count() == 0
        # Assert orgtask_dbt still exists
        assert OrgTask.objects.filter(id=orgtask_dbt.id).count() == 1

        # Assert warehouse credentials deleted before warehouse is deleted
        assert secretsmanager.delete_warehouse_credentials.call_count == 1
        # Assert airbyte destination deleted
        airbyte_service.delete_destination.assert_called_once_with(
            org.airbyte_workspace_id, warehouse.airbyte_destination_id
        )
        # Assert warehouse deleted
        assert OrgWarehouse.objects.filter(id=warehouse.id).count() == 0

    # Cleanup org
    org.delete()
    task_sync.delete()
    task_dbt.delete()


@pytest.fixture
def org_with_transform_tasks():
    org = Org.objects.create(
        name="TestOrg2", airbyte_workspace_id="workspace456", slug="test-slug2"
    )
    # Create dbt and git tasks
    task_dbt = Task.objects.create(
        type="dbt", slug="dbt", label="DBT", command="dbt run", is_system=True
    )
    task_git = Task.objects.create(
        type="git", slug="git", label="Git", command="git pull", is_system=True
    )
    orgtask_dbt = OrgTask.objects.create(org=org, task=task_dbt, generated_by="system")
    orgtask_git = OrgTask.objects.create(org=org, task=task_git, generated_by="system")
    return org, orgtask_dbt, orgtask_git


@patch("ddpui.services.org_cleanup_service.prefect_service")
@patch("ddpui.services.org_cleanup_service.secretsmanager")
@patch("ddpui.services.org_cleanup_service.DbtProjectManager")
@patch("ddpui.services.org_cleanup_service.os")
@patch("ddpui.services.org_cleanup_service.shutil")
def test_delete_transformation_layer_dry_run(
    mock_shutil,
    mock_os,
    mock_DbtProjectManager,
    mock_secretsmanager,
    mock_prefect_service,
    org_with_transform_tasks,
):
    org, orgtask_dbt, orgtask_git = org_with_transform_tasks
    service = OrgCleanupService(org, dry_run=True)
    service.delete_transformation_layer()
    # Objects should still exist
    assert OrgTask.objects.filter(id=orgtask_dbt.id).exists()
    assert OrgTask.objects.filter(id=orgtask_git.id).exists()


@patch("ddpui.services.org_cleanup_service.prefect_service")
@patch("ddpui.services.org_cleanup_service.secretsmanager")
@patch("ddpui.services.org_cleanup_service.DbtProjectManager")
@patch("ddpui.services.org_cleanup_service.os")
@patch("ddpui.services.org_cleanup_service.shutil")
def test_delete_transformation_layer_delete(
    mock_shutil,
    mock_os,
    mock_DbtProjectManager,
    mock_secretsmanager,
    mock_prefect_service,
    org_with_transform_tasks,
):
    org, orgtask_dbt, orgtask_git = org_with_transform_tasks
    # Simulate dbt workspace exists
    mock_os.path.exists.return_value = True
    service = OrgCleanupService(org, dry_run=False)
    service.delete_transformation_layer()
    # Objects should be deleted
    assert not OrgTask.objects.filter(id=orgtask_dbt.id).exists()
    assert not OrgTask.objects.filter(id=orgtask_git.id).exists()


@patch("ddpui.services.org_cleanup_service.prefect_service")
@patch("ddpui.services.org_cleanup_service.secretsmanager")
@patch("ddpui.services.org_cleanup_service.DbtProjectManager")
@patch("ddpui.services.org_cleanup_service.os")
@patch("ddpui.services.org_cleanup_service.shutil")
def test_delete_transformation_layer_task_in_orchestrate(
    mock_shutil,
    mock_os,
    mock_DbtProjectManager,
    mock_secretsmanager,
    mock_prefect_service,
    org_with_transform_tasks,
):
    org, orgtask_dbt, orgtask_git = org_with_transform_tasks
    # Create orchestrate pipeline using orgtask_dbt
    dataflow_orchestrate = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="orchestrate", deployment_id="deployX", deployment_name="dfX"
    )
    DataflowOrgTask.objects.create(dataflow=dataflow_orchestrate, orgtask=orgtask_dbt)
    service = OrgCleanupService(org, dry_run=False)
    with pytest.raises(OrgCleanupServiceError):
        service.delete_transformation_layer()


@pytest.fixture
def org_with_orchestrate_pipelines():
    org = Org.objects.create(
        name="TestOrgOrch", airbyte_workspace_id="workspace789", slug="test-slug-orch"
    )
    dataflow1 = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="orchestrate", deployment_id="dep1", deployment_name="orch1"
    )
    dataflow2 = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="orchestrate", deployment_id="dep2", deployment_name="orch2"
    )
    return org, [dataflow1, dataflow2]


@patch("ddpui.services.org_cleanup_service.prefect_service")
def test_delete_orchestrate_pipelines_dry_run(mock_prefect_service, org_with_orchestrate_pipelines):
    org, dataflows = org_with_orchestrate_pipelines
    service = OrgCleanupService(org, dry_run=True)
    service.delete_orchestrate_pipelines()
    # Should not delete any dataflows
    for df in dataflows:
        assert OrgDataFlowv1.objects.filter(id=df.id).exists()
    mock_prefect_service.delete_deployment_by_id.assert_not_called()


@patch("ddpui.services.org_cleanup_service.prefect_service")
def test_delete_orchestrate_pipelines_delete(mock_prefect_service, org_with_orchestrate_pipelines):
    org, dataflows = org_with_orchestrate_pipelines
    service = OrgCleanupService(org, dry_run=False)
    service.delete_orchestrate_pipelines()
    # Should delete all orchestrate dataflows
    for df in dataflows:
        assert not OrgDataFlowv1.objects.filter(id=df.id).exists()
        mock_prefect_service.delete_deployment_by_id.assert_any_call(df.deployment_id)


@patch("ddpui.services.org_cleanup_service.prefect_service")
def test_delete_orchestrate_pipelines_none(mock_prefect_service):
    org = Org.objects.create(
        name="TestOrgOrchNone", airbyte_workspace_id="workspace000", slug="test-slug-orch-none"
    )
    service = OrgCleanupService(org, dry_run=False)
    service.delete_orchestrate_pipelines()
    # No orchestrate pipelines, nothing should be deleted
    assert OrgDataFlowv1.objects.filter(org=org, dataflow_type="orchestrate").count() == 0
    mock_prefect_service.delete_deployment_by_id.assert_not_called()
