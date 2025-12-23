import pytest
from unittest.mock import patch, MagicMock
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.tasks import OrgTask, Task, OrgDataFlowv1, DataflowOrgTask
from ddpui.services.org_cleanup_service import OrgCleanupService


pytestmark = pytest.mark.django_db


def test_delete_warehouse_full_flow():
    # Setup Org
    org = Org.objects.create(name="TestOrg", airbyte_workspace_id="workspace123", slug="test-slug")

    # Setup OrgWarehouse
    warehouse = OrgWarehouse.objects.create(
        org=org, wtype="postgres", credentials="{}", airbyte_destination_id="dest123"
    )

    # Setup Task and OrgTask (airbyte type)
    task = Task.objects.create(
        type="airbyte", slug="sync", label="Sync", command="sync", is_system=True
    )
    orgtask = OrgTask.objects.create(
        org=org, task=task, connection_id="conn123", generated_by="system"
    )

    # Setup OrgDataFlowv1 and DataflowOrgTask (manual dataflow with airbyte tasks)
    dataflow = OrgDataFlowv1.objects.create(
        org=org, dataflow_type="manual", deployment_id="deploy1", deployment_name="df1"
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)

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

        # Assert prefect_service.delete_deployment_by_id called for dataflow
        prefect_service.delete_deployment_by_id.assert_called_once_with(dataflow.deployment_id)
        # Assert dataflow deleted
        assert OrgDataFlowv1.objects.filter(id=dataflow.id).count() == 0

        # Assert airbyte connection deleted
        airbyte_service.delete_connection.assert_called_once_with(
            org.airbyte_workspace_id, orgtask.connection_id
        )
        # Assert orgtask deleted
        assert OrgTask.objects.filter(id=orgtask.id).count() == 0

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
