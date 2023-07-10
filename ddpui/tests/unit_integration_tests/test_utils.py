import os
from unittest.mock import patch, Mock
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgDataFlow, OrgPrefectBlock, OrgWarehouse
from ddpui.models.org_user import OrgUser, User

from ddpui.utils.deleteorg import (
    delete_prefect_deployments,
    delete_prefect_shell_blocks,
    delete_dbt_workspace,
    delete_orgusers,
    delete_airbyte_workspace,
)
from ddpui.ddpprefect import AIRBYTESERVER, AIRBYTECONNECTION, SHELLOPERATION

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


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_deployment_by_id=Mock(),
)
def test_delete_prefect_deployments(org_with_workspace):
    """
    deleting a prefect deployment should
    - invoke delete_deployment_by_id
    - delete the org-dataflow
    """
    OrgDataFlow.objects.create(org=org_with_workspace, name="org-dataflow-name")
    assert OrgDataFlow.objects.filter(org=org_with_workspace).count() == 1
    delete_prefect_deployments(org_with_workspace)
    assert OrgDataFlow.objects.filter(org=org_with_workspace).count() == 0


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_shell_block=Mock(),
)
def test_delete_prefect_shell_blocks(org_with_workspace):
    """
    deleting a prefect deployment should
    - invoke delete_deployment_by_id
    - delete the org-dataflow
    """
    OrgPrefectBlock.objects.create(org=org_with_workspace, block_type=SHELLOPERATION)
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=SHELLOPERATION
        ).count()
        == 1
    )
    delete_prefect_shell_blocks(org_with_workspace)
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=SHELLOPERATION
        ).count()
        == 0
    )


@patch.multiple("ddpui.ddpdbt.dbt_service", delete_dbt_workspace=Mock())
def test_delete_dbt_workspace(org_with_workspace):
    """check that dbt_service.delete_dbt_workspace is called"""
    delete_dbt_workspace(org_with_workspace)


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_airbyte_connection_block=Mock(),
    delete_airbyte_server_block=Mock(),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connections=Mock(
        return_value={"connections": [{"connectionId": "fake-connection-id"}]}
    ),
    delete_connection=Mock(),
    get_destinations=Mock(
        return_value={"destinations": [{"destinationId": "fake-destination-id"}]}
    ),
    delete_destination=Mock(),
    get_sources=Mock(return_value={"sources": [{"sourceId": "fake-source-id"}]}),
    delete_source=Mock(),
    delete_workspace=Mock(),
)
@patch.multiple("ddpui.utils.secretsmanager", delete_warehouse_credentials=Mock())
def test_delete_airbyte_workspace(org_with_workspace):
    """
    delete connection blocks
    delete server blocks
    delete connections
    delete destinations
    delete sources
    delete warehouse credentials
    delete workspace
    """
    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=AIRBYTECONNECTION,
        block_id="fake-conn-block-id",
        block_name="fake-conn-block-name",
    )
    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=AIRBYTESERVER,
        block_id="fake-srvr-block-id",
        block_name="fake-srvr-block-name",
    )
    OrgWarehouse.objects.create(org=org_with_workspace)
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=AIRBYTECONNECTION
        ).count()
        == 1
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=AIRBYTESERVER
        ).count()
        == 1
    )
    assert OrgWarehouse.objects.filter(org=org_with_workspace).count() == 1
    delete_airbyte_workspace(org_with_workspace)
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=AIRBYTECONNECTION
        ).count()
        == 0
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace, block_type=AIRBYTESERVER
        ).count()
        == 0
    )
    assert OrgWarehouse.objects.filter(org=org_with_workspace).count() == 0


def test_delete_orgusers(org_with_workspace):
    """ensure that orguser.user.delete is called"""
    email = "fake-email"
    tempuser = User.objects.create(email=email, username="fake-username")
    OrgUser.objects.create(user=tempuser, org=org_with_workspace)
    assert (
        OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 1
    )
    assert User.objects.filter(email=email).count() == 1
    delete_orgusers(org_with_workspace)
    assert (
        OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 0
    )
    assert User.objects.filter(email=email).count() == 0
