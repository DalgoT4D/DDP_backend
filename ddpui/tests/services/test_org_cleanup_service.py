import pytest
from unittest.mock import patch, MagicMock
from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgTask, Task, OrgDataFlowv1, DataflowOrgTask
from ddpui.services.org_cleanup_service import OrgCleanupService, OrgCleanupServiceError
from ddpui.ddpprefect import AIRBYTESERVER, DBTCLIPROFILE, SECRET


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


# ================================================================================
# Test delete_orgusers
# ================================================================================


class TestDeleteOrgusers:
    def test_dry_run_keeps_users(self):
        org = Org.objects.create(
            name="OrgUserDryRun", airbyte_workspace_id="ws-du", slug="org-du-dry"
        )
        user = User.objects.create(username="du_dry_user", email="du_dry@test.com")
        orguser = OrgUser.objects.create(user=user, org=org)

        service = OrgCleanupService(org, dry_run=True)
        service.delete_orgusers()

        assert OrgUser.objects.filter(id=orguser.id).exists()

        orguser.delete()
        user.delete()
        org.delete()

    def test_delete_orgusers(self):
        org = Org.objects.create(
            name="OrgUserDel", airbyte_workspace_id="ws-dud", slug="org-du-del"
        )
        user1 = User.objects.create(username="du_del_user1", email="du_del1@test.com")
        user2 = User.objects.create(username="du_del_user2", email="du_del2@test.com")
        ou1 = OrgUser.objects.create(user=user1, org=org)
        ou2 = OrgUser.objects.create(user=user2, org=org)

        service = OrgCleanupService(org, dry_run=False)
        service.delete_orgusers()

        assert not OrgUser.objects.filter(org=org).exists()

        user1.delete()
        user2.delete()
        org.delete()


# ================================================================================
# Test delete_edr_pipelines
# ================================================================================


class TestDeleteEdrPipelines:
    def test_dry_run(self):
        org = Org.objects.create(
            name="EdrDryRun", airbyte_workspace_id="ws-edr-dry", slug="edr-dry"
        )
        task_edr = Task.objects.create(
            type="edr", slug="edr-dry", label="EDR", command="edr", is_system=True
        )
        orgtask_edr = OrgTask.objects.create(org=org, task=task_edr, generated_by="system")
        dataflow_edr = OrgDataFlowv1.objects.create(
            org=org, dataflow_type="manual", deployment_id="edr-dep", deployment_name="edr-df"
        )
        DataflowOrgTask.objects.create(dataflow=dataflow_edr, orgtask=orgtask_edr)

        service = OrgCleanupService(org, dry_run=True)
        service.delete_edr_pipelines()

        assert OrgTask.objects.filter(id=orgtask_edr.id).exists()
        assert OrgDataFlowv1.objects.filter(id=dataflow_edr.id).exists()

        # Cleanup
        DataflowOrgTask.objects.filter(dataflow=dataflow_edr).delete()
        dataflow_edr.delete()
        orgtask_edr.delete()
        task_edr.delete()
        org.delete()

    @patch("ddpui.services.org_cleanup_service.prefect_service")
    def test_delete_edr_pipelines(self, mock_prefect_service):
        org = Org.objects.create(name="EdrDel", airbyte_workspace_id="ws-edr-del", slug="edr-del")
        task_edr = Task.objects.create(
            type="edr", slug="edr-del", label="EDR", command="edr", is_system=True
        )
        orgtask_edr = OrgTask.objects.create(org=org, task=task_edr, generated_by="system")
        dataflow_edr = OrgDataFlowv1.objects.create(
            org=org, dataflow_type="manual", deployment_id="edr-dep2", deployment_name="edr-df2"
        )
        DataflowOrgTask.objects.create(dataflow=dataflow_edr, orgtask=orgtask_edr)

        service = OrgCleanupService(org, dry_run=False)
        service.delete_edr_pipelines()

        assert not OrgTask.objects.filter(id=orgtask_edr.id).exists()
        assert not OrgDataFlowv1.objects.filter(id=dataflow_edr.id).exists()
        mock_prefect_service.delete_deployment_by_id.assert_called_once_with("edr-dep2")

        task_edr.delete()
        org.delete()

    @patch("ddpui.services.org_cleanup_service.prefect_service")
    def test_delete_edr_pipelines_prefect_error(self, mock_prefect_service):
        """Even if prefect delete fails, the dataflow and orgtasks should still be deleted."""
        org = Org.objects.create(name="EdrErr", airbyte_workspace_id="ws-edr-err", slug="edr-err")
        task_edr = Task.objects.create(
            type="edr", slug="edr-err", label="EDR", command="edr", is_system=True
        )
        orgtask_edr = OrgTask.objects.create(org=org, task=task_edr, generated_by="system")
        dataflow_edr = OrgDataFlowv1.objects.create(
            org=org, dataflow_type="manual", deployment_id="edr-dep3", deployment_name="edr-df3"
        )
        DataflowOrgTask.objects.create(dataflow=dataflow_edr, orgtask=orgtask_edr)

        mock_prefect_service.delete_deployment_by_id.side_effect = Exception("prefect down")

        service = OrgCleanupService(org, dry_run=False)
        service.delete_edr_pipelines()

        assert not OrgTask.objects.filter(id=orgtask_edr.id).exists()
        assert not OrgDataFlowv1.objects.filter(id=dataflow_edr.id).exists()

        task_edr.delete()
        org.delete()

    def test_no_edr_tasks(self):
        org = Org.objects.create(name="NoEdr", airbyte_workspace_id="ws-no-edr", slug="no-edr")
        service = OrgCleanupService(org, dry_run=False)
        service.delete_edr_pipelines()  # should not raise
        org.delete()


# ================================================================================
# Test delete_airbyte_workspace
# ================================================================================


class TestDeleteAirbyteWorkspace:
    def test_no_workspace_id(self):
        org = Org.objects.create(name="NoWsId", airbyte_workspace_id=None, slug="no-ws-id")
        service = OrgCleanupService(org, dry_run=False)
        service.delete_airbyte_workspace()  # should return early without error
        org.delete()

    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    def test_dry_run(self, mock_airbyte):
        org = Org.objects.create(name="WsDryRun", airbyte_workspace_id="ws-dry", slug="ws-dry")
        mock_airbyte.get_sources.return_value = {"sources": [{"sourceId": "src1"}]}
        mock_airbyte.get_destinations.return_value = {"destinations": [{"destinationId": "dest1"}]}

        service = OrgCleanupService(org, dry_run=True)
        service.delete_airbyte_workspace()

        mock_airbyte.delete_source.assert_not_called()
        mock_airbyte.delete_destination.assert_not_called()
        mock_airbyte.delete_workspace.assert_not_called()

        org.delete()

    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    def test_delete_workspace(self, mock_airbyte):
        org = Org.objects.create(name="WsDel", airbyte_workspace_id="ws-del", slug="ws-del")
        mock_airbyte.get_sources.return_value = {
            "sources": [{"sourceId": "src1"}, {"sourceId": "src2"}]
        }
        mock_airbyte.get_destinations.return_value = {"destinations": [{"destinationId": "dest1"}]}

        service = OrgCleanupService(org, dry_run=False)
        service.delete_airbyte_workspace()

        assert mock_airbyte.delete_source.call_count == 2
        mock_airbyte.delete_destination.assert_called_once_with("ws-del", "dest1")
        mock_airbyte.delete_workspace.assert_called_once_with("ws-del")

        org.delete()

    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    def test_delete_workspace_handles_errors(self, mock_airbyte):
        org = Org.objects.create(name="WsErr", airbyte_workspace_id="ws-err", slug="ws-err")
        mock_airbyte.get_sources.return_value = {"sources": [{"sourceId": "src1"}]}
        mock_airbyte.get_destinations.return_value = {"destinations": [{"destinationId": "dest1"}]}
        mock_airbyte.delete_source.side_effect = Exception("airbyte error")
        mock_airbyte.delete_destination.side_effect = Exception("airbyte error")
        mock_airbyte.delete_workspace.side_effect = Exception("airbyte error")

        service = OrgCleanupService(org, dry_run=False)
        # Should not raise despite errors
        service.delete_airbyte_workspace()

        org.delete()


# ================================================================================
# Test delete_warehouse dry run
# ================================================================================


class TestDeleteWarehouseDryRun:
    def test_dry_run_preserves_objects(self):
        org = Org.objects.create(name="WhDryRun", airbyte_workspace_id="ws-wh-dry", slug="wh-dry")
        warehouse = OrgWarehouse.objects.create(
            org=org, wtype="postgres", credentials="{}", airbyte_destination_id="dest-dry"
        )

        service = OrgCleanupService(org, dry_run=True)
        service.delete_warehouse()

        assert OrgWarehouse.objects.filter(id=warehouse.id).exists()

        warehouse.delete()
        org.delete()


# ================================================================================
# Test delete_orchestrate_pipelines with HttpError
# ================================================================================


class TestDeleteOrchestratePipelinesWithErrors:
    @patch("ddpui.services.org_cleanup_service.prefect_service")
    def test_prefect_http_error_handled(self, mock_prefect_service):
        """HttpError from prefect should be caught and pipeline still deleted from DB."""
        from ninja.errors import HttpError

        org = Org.objects.create(
            name="OrchErr", airbyte_workspace_id="ws-orch-err", slug="orch-err"
        )
        dataflow = OrgDataFlowv1.objects.create(
            org=org,
            dataflow_type="orchestrate",
            deployment_id="dep-err",
            deployment_name="orch-err",
        )

        mock_prefect_service.delete_deployment_by_id.side_effect = HttpError(404, "not found")

        service = OrgCleanupService(org, dry_run=False)
        service.delete_orchestrate_pipelines()

        assert not OrgDataFlowv1.objects.filter(id=dataflow.id).exists()

        org.delete()


# ================================================================================
# Test delete_org (full workflow)
# ================================================================================


class TestDeleteOrg:
    @patch("ddpui.services.org_cleanup_service.prefect_service")
    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.DbtProjectManager")
    @patch("ddpui.services.org_cleanup_service.os")
    @patch("ddpui.services.org_cleanup_service.shutil")
    def test_delete_org_dry_run(
        self,
        mock_shutil,
        mock_os,
        mock_dbt_manager,
        mock_secrets,
        mock_airbyte,
        mock_prefect,
    ):
        org = Org.objects.create(
            name="OrgFullDry",
            airbyte_workspace_id="ws-full-dry",
            slug="org-full-dry",
        )
        mock_airbyte.get_sources.return_value = {"sources": []}
        mock_airbyte.get_destinations.return_value = {"destinations": []}
        mock_dbt_manager.get_org_dir.return_value = "/tmp/fake_org_dir"
        mock_os.path.exists.return_value = False

        service = OrgCleanupService(org, dry_run=True)
        service.delete_org()

        # Org should still exist in dry run
        assert Org.objects.filter(id=org.id).exists()

        org.delete()

    @patch("ddpui.services.org_cleanup_service.prefect_service")
    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.DbtProjectManager")
    @patch("ddpui.services.org_cleanup_service.os")
    @patch("ddpui.services.org_cleanup_service.shutil")
    def test_delete_org_full(
        self,
        mock_shutil,
        mock_os,
        mock_dbt_manager,
        mock_secrets,
        mock_airbyte,
        mock_prefect,
    ):
        org = Org.objects.create(
            name="OrgFullDel",
            airbyte_workspace_id="ws-full-del",
            slug="org-full-del",
        )
        mock_airbyte.get_sources.return_value = {"sources": []}
        mock_airbyte.get_destinations.return_value = {"destinations": []}
        mock_dbt_manager.get_org_dir.return_value = "/tmp/fake_org_dir"
        mock_os.path.exists.return_value = False

        org_id = org.id
        service = OrgCleanupService(org, dry_run=False)
        service.delete_org()

        assert not Org.objects.filter(id=org_id).exists()


# ================================================================================
# Test delete_org with airbyte server blocks
# ================================================================================


class TestDeleteOrgWithBlocks:
    @patch("ddpui.services.org_cleanup_service.prefect_service")
    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.DbtProjectManager")
    @patch("ddpui.services.org_cleanup_service.os")
    @patch("ddpui.services.org_cleanup_service.shutil")
    def test_delete_org_with_airbyte_server_block(
        self,
        mock_shutil,
        mock_os,
        mock_dbt_manager,
        mock_secrets,
        mock_airbyte,
        mock_prefect,
    ):
        org = Org.objects.create(
            name="OrgBlock",
            airbyte_workspace_id="ws-block",
            slug="org-block",
        )
        block = OrgPrefectBlockv1.objects.create(
            org=org,
            block_type=AIRBYTESERVER,
            block_id="block-id-1",
            block_name="block-name-1",
        )

        mock_airbyte.get_sources.return_value = {"sources": []}
        mock_airbyte.get_destinations.return_value = {"destinations": []}
        mock_dbt_manager.get_org_dir.return_value = "/tmp/fake_org_dir"
        mock_os.path.exists.return_value = False

        org_id = org.id
        service = OrgCleanupService(org, dry_run=False)
        service.delete_org()

        assert not OrgPrefectBlockv1.objects.filter(id=block.id).exists()
        mock_prefect.prefect_delete_a_block.assert_called_once_with("block-id-1")

    @patch("ddpui.services.org_cleanup_service.prefect_service")
    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.DbtProjectManager")
    @patch("ddpui.services.org_cleanup_service.os")
    @patch("ddpui.services.org_cleanup_service.shutil")
    def test_delete_org_with_disk_directory(
        self,
        mock_shutil,
        mock_os,
        mock_dbt_manager,
        mock_secrets,
        mock_airbyte,
        mock_prefect,
    ):
        org = Org.objects.create(
            name="OrgDisk",
            airbyte_workspace_id="ws-disk",
            slug="org-disk",
        )
        mock_airbyte.get_sources.return_value = {"sources": []}
        mock_airbyte.get_destinations.return_value = {"destinations": []}
        mock_dbt_manager.get_org_dir.return_value = "/tmp/fake_org_dir"
        mock_os.path.exists.return_value = True

        service = OrgCleanupService(org, dry_run=False)
        service.delete_org()

        mock_shutil.rmtree.assert_called_once_with("/tmp/fake_org_dir")


# ================================================================================
# Test delete_warehouse with connection_id=None
# ================================================================================


class TestDeleteWarehouseEdgeCases:
    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.prefect_service")
    def test_orgtask_without_connection_id(self, mock_prefect, mock_secrets, mock_airbyte):
        """OrgTask with no connection_id should skip airbyte delete call."""
        org = Org.objects.create(
            name="WhNoConn", airbyte_workspace_id="ws-no-conn", slug="wh-no-conn"
        )
        warehouse = OrgWarehouse.objects.create(
            org=org, wtype="postgres", credentials="{}", airbyte_destination_id="dest-nc"
        )
        task = Task.objects.create(
            type="airbyte", slug="ab-no-conn", label="AB", command="sync", is_system=True
        )
        orgtask = OrgTask.objects.create(
            org=org, task=task, connection_id=None, generated_by="system"
        )

        service = OrgCleanupService(org, dry_run=False)
        service.delete_warehouse()

        mock_airbyte.delete_connection.assert_not_called()
        assert not OrgTask.objects.filter(id=orgtask.id).exists()

        task.delete()
        org.delete()

    @patch("ddpui.services.org_cleanup_service.airbyte_service")
    @patch("ddpui.services.org_cleanup_service.secretsmanager")
    @patch("ddpui.services.org_cleanup_service.prefect_service")
    def test_airbyte_delete_connection_error(self, mock_prefect, mock_secrets, mock_airbyte):
        """If delete_connection fails, should still delete orgtask."""
        org = Org.objects.create(
            name="WhConnErr", airbyte_workspace_id="ws-conn-err", slug="wh-conn-err"
        )
        warehouse = OrgWarehouse.objects.create(
            org=org, wtype="postgres", credentials="{}", airbyte_destination_id="dest-ce"
        )
        task = Task.objects.create(
            type="airbyte", slug="ab-conn-err", label="AB", command="sync", is_system=True
        )
        orgtask = OrgTask.objects.create(
            org=org, task=task, connection_id="conn-err", generated_by="system"
        )

        mock_airbyte.delete_connection.side_effect = Exception("connection gone")

        service = OrgCleanupService(org, dry_run=False)
        service.delete_warehouse()

        assert not OrgTask.objects.filter(id=orgtask.id).exists()

        task.delete()
        org.delete()
