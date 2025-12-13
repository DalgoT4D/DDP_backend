import os
import uuid
from unittest.mock import patch, Mock
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1, OrgWarehouse
from ddpui.models.org_user import OrgUser, User
from ddpui.models.tasks import OrgTask, DataflowOrgTask, Task, TaskType
from ddpui.utils.constants import TASK_AIRBYTESYNC
from ddpui.ddpprefect import AIRBYTESERVER

from ddpui.utils.deleteorg import (
    delete_orgprefectblocks,
    delete_orgdataflows,
    delete_warehouse_v1,
    delete_orgtasks,
    delete_dbt_workspace,
    delete_airbyte_workspace_v1,
    delete_orgusers,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name", airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


@patch.multiple("ddpui.ddpdbt.dbt_service", delete_dbt_workspace=Mock())
def test_delete_dbt_workspace(org_with_workspace):
    """check that dbt_service.delete_dbt_workspace is called"""
    delete_dbt_workspace(org_with_workspace)


def test_delete_orgprefectblocks(org_with_workspace):
    """ensure that prefect_service.prefect_delete_a_block is called"""
    OrgPrefectBlockv1.objects.create(
        org=org_with_workspace,
        block_type="fake-block-type",
        block_id="FAKE-BLOCK-ID",
        block_name="fake-block-name",
    )
    assert OrgPrefectBlockv1.objects.filter(org=org_with_workspace).count() == 1
    with patch(
        "ddpui.ddpprefect.prefect_service.prefect_delete_a_block"
    ) as mock_prefect_delete_a_block:
        delete_orgprefectblocks(org_with_workspace)
        mock_prefect_delete_a_block.assert_called_once_with("FAKE-BLOCK-ID")
    assert OrgPrefectBlockv1.objects.filter(org=org_with_workspace).count() == 0


def test_delete_orgdataflows(org_with_workspace):
    """ensure that prefect_service.prefect_delete_a_block is called"""
    OrgDataFlowv1.objects.create(
        org=org_with_workspace,
        name="fake-name",
        deployment_id="fake-deployment_id",
        deployment_name="fake-deployment-name",
    )
    assert OrgDataFlowv1.objects.filter(org=org_with_workspace).count() == 1
    with patch(
        "ddpui.ddpprefect.prefect_service.delete_deployment_by_id"
    ) as mock_delete_deployment_by_id:
        delete_orgdataflows(org_with_workspace)
        mock_delete_deployment_by_id.assert_called_once_with("fake-deployment_id")
    assert OrgDataFlowv1.objects.filter(org=org_with_workspace).count() == 0


def test_delete_warehouse_v1(org_with_workspace):
    """tests delete_warehouse_v1"""
    if not Task.objects.filter(slug=TASK_AIRBYTESYNC).exists():
        Task.objects.create(slug=TASK_AIRBYTESYNC, type=TaskType.AIRBYTE, label="fake-name")
    if not OrgTask.objects.filter(org=org_with_workspace, task__slug=TASK_AIRBYTESYNC).exists():
        task = Task.objects.filter(slug=TASK_AIRBYTESYNC).first()
        OrgTask.objects.create(org=org_with_workspace, task=task)

    orgtask = OrgTask.objects.filter(org=org_with_workspace, task__slug=TASK_AIRBYTESYNC).first()
    orgtask.connection_id = "fake-connection-id"
    orgtask.save()

    org_with_workspace.airbyte_workspace_id = str(uuid.uuid4())
    org_with_workspace.save()

    dataflow = OrgDataFlowv1.objects.create(
        org=org_with_workspace,
        name="fake-name",
        deployment_id="fake-deployment_id",
        deployment_name="fake-deployment-name",
    )
    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)
    if not OrgWarehouse.objects.filter(org=org_with_workspace).exists():
        OrgWarehouse.objects.create(
            org=org_with_workspace,
            wtype="fake-wtype",
            airbyte_destination_id="fake-destination-id",
            airbyte_docker_repository="fake-docker-repository",
            airbyte_docker_image_tag="fake-docker-image-tag",
            bq_location="fake-bq-location",
        )

    with patch("ddpui.ddpprefect.prefect_service.delete_deployment_by_id") as mock_delete, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_connection"
    ) as mock_delete_connection, patch(
        "ddpui.ddpairbyte.airbyte_service.get_connections"
    ) as mock_get_connections, patch(
        "ddpui.ddpairbyte.airbyte_service.get_sources"
    ) as mock_get_sources, patch(
        "ddpui.ddpairbyte.airbyte_service.get_destinations"
    ) as mock_get_destinations, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_source"
    ) as mock_delete_source, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_destination"
    ) as mock_delete_destination, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_workspace"
    ) as mock_delete_workspace, patch(
        "ddpui.utils.secretsmanager.delete_warehouse_credentials"
    ) as mock_delete_warehouse_credentials, patch(
        "ddpui.ddpprefect.prefect_service.prefect_delete_a_block"
    ) as mock_prefect_delete_a_block:
        # == delete_warehouse_v1 ==
        mock_get_sources.return_value = {"sources": []}  # No sources to delete
        mock_get_destinations.return_value = {
            "destinations": [
                {"destinationId": "fake-destination-id"},
            ]
        }
        mock_get_connections.return_value = {
            "connections": [
                {"connectionId": "fake-connection-id"},
            ]
        }

        delete_warehouse_v1(org_with_workspace)
        mock_delete.assert_called_once_with("fake-deployment_id")
        mock_delete_connection.assert_called_with(
            org_with_workspace.airbyte_workspace_id, "fake-connection-id"
        )
        mock_delete_destination.assert_called_once_with(
            org_with_workspace.airbyte_workspace_id, "fake-destination-id"
        )
        mock_delete_warehouse_credentials.assert_called_once()

    assert (
        OrgTask.objects.filter(org=org_with_workspace, connection_id="fake-connection-id").count()
        == 0
    )
    assert OrgWarehouse.objects.filter(org=org_with_workspace).count() == 0


def test_delete_orgtasks(org_with_workspace):
    """tests delete_orgtasks"""
    delete_orgtasks(org_with_workspace)
    assert OrgTask.objects.filter(org=org_with_workspace).count() == 0


def test_delete_airbyte_workspace_v1(org_with_workspace):
    """tests delete_airbyte_workspace_v1"""

    org_with_workspace.airbyte_workspace_id = str(uuid.uuid4())
    org_with_workspace.save()

    with patch("ddpui.ddpairbyte.airbyte_service.get_sources") as mock_get_sources, patch(
        "ddpui.ddpairbyte.airbyte_service.get_destinations"
    ) as mock_get_destinations, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_source"
    ) as mock_delete_source, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_destination"
    ) as mock_delete_destination, patch(
        "ddpui.ddpairbyte.airbyte_service.delete_workspace"
    ) as mock_delete_workspace, patch(
        "ddpui.ddpprefect.prefect_service.prefect_delete_a_block"
    ) as mock_prefect_delete_a_block:
        mock_get_sources.return_value = {"sources": [{"sourceId": "fake-source-id"}]}
        mock_get_destinations.return_value = {"destinations": []}

        OrgPrefectBlockv1.objects.create(
            org=org_with_workspace,
            block_type=AIRBYTESERVER,
            block_id="FAKE-AIRBYTESERVER-BLOCK-ID",
            block_name="fake-block-name",
        )
        delete_airbyte_workspace_v1(org_with_workspace)
        mock_delete_source.assert_called_once_with(
            org_with_workspace.airbyte_workspace_id, "fake-source-id"
        )
        mock_delete_workspace.assert_called_once_with(org_with_workspace.airbyte_workspace_id)
        mock_prefect_delete_a_block.assert_called_once_with("FAKE-AIRBYTESERVER-BLOCK-ID")
        assert (
            OrgPrefectBlockv1.objects.filter(
                org=org_with_workspace, block_type=AIRBYTESERVER
            ).count()
            == 0
        )


def test_delete_orgusers(org_with_workspace):
    """ensure that orguser.user.delete is called"""
    email = "fake-email"
    tempuser = User.objects.create(email=email, username="fake-username")
    OrgUser.objects.create(user=tempuser, org=org_with_workspace)
    assert OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 1
    delete_orgusers(org_with_workspace)
    assert OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 0
