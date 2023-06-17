import os
import django

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.api.client.airbyte_api import (
    post_airbyte_detach_workspace,
    post_airbyte_workspace,
    get_airbyte_source_definitions,
    get_airbyte_source_definition_specifications,
)
from ddpui.ddpairbyte.schema import AirbyteWorkspaceCreate
from ddpui import ddpprefect


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    print("creating org_without_workspace")
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


def test_post_airbyte_detach_workspace_0():
    """tests /worksspace/detatch/"""

    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_detach_workspace(mock_request)
    assert str(excinfo.value) == "create an organization first"


def test_post_airbyte_detach_workspace_1(org_without_workspace):
    """tests /worksspace/detatch/"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_detach_workspace(mock_request)
    assert str(excinfo.value) == "org already has no workspace"


def test_post_airbyte_detach_workspace_2(org_with_workspace):
    """tests /worksspace/detatch/"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgPrefectBlock.objects.create(
        org=mock_orguser.org,
        block_type=ddpprefect.AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake-serverblock-name",
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=mock_orguser.org, block_type=ddpprefect.AIRBYTESERVER
        ).count()
        == 1
    )
    OrgPrefectBlock.objects.create(
        org=mock_orguser.org,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-connectionblock-id",
        block_name="fake-connectionblock-name",
    )
    OrgPrefectBlock.objects.create(
        org=mock_orguser.org,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-connectionblock-id-1",
        block_name="fake-connectionblock-name-1",
    )
    OrgPrefectBlock.objects.create(
        org=mock_orguser.org,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-connectionblock-id-2",
        block_name="fake-connectionblock-name-2",
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=mock_orguser.org, block_type=ddpprefect.AIRBYTECONNECTION
        ).count()
        == 3
    )

    # @patch("ddpui.ddpprefect.prefect_service.delete_airbyte_server_block")
    # def patched_delete_airbyte_server_block():
    #     pass

    @patch("ddpui.ddpprefect.prefect_service.delete_airbyte_connection_block")
    def mock_delete_airbyte_connection_block():
        pass

    @patch("ddpui.ddpprefect.prefect_service.delete_airbyte_server_block")
    def mock_delete_airbyte_server_block():
        pass

    post_airbyte_detach_workspace(mock_request)

    assert (
        OrgPrefectBlock.objects.filter(
            org=mock_orguser.org, block_type=ddpprefect.AIRBYTESERVER
        ).count()
        == 0
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=mock_orguser.org, block_type=ddpprefect.AIRBYTECONNECTION
        ).count()
        == 0
    )


def test_post_airbyte_workspace():
    """if the request passes the authentication check
    AND there are no airbyte server blocks for this org
    AND we can conenct to airbyte (or a mocked version of it)
    then post_airbyte_workspace must succeed
    """
    test_org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")

    mock_orguser = Mock()
    mock_orguser.org = test_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    testworkspacename = "Test Workspace"
    mock_payload = AirbyteWorkspaceCreate(name=testworkspacename)

    for serverblock in OrgPrefectBlock.objects.filter(
        org=test_org, block_type=ddpprefect.AIRBYTESERVER
    ):
        serverblock.delete()

    serverblock = OrgPrefectBlock.objects.filter(
        block_name="test-org-slug-airbyte-server"
    ).first()
    if serverblock:
        serverblock.delete()

    response = post_airbyte_workspace(mock_request, mock_payload)
    assert response.name == testworkspacename


def test_get_airbyte_source_definitions_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definitions(mock_request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_source_definitions",
    return_value={"sourceDefinitions": [1, 2, 3]},
)
def test_get_airbyte_source_definitions_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source_definitions(mock_request)

    assert len(result) == 3


def test_get_airbyte_source_definition_specifications_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_definition_specifications(mock_request, "fake-sourcedef-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_source_definition_specification",
    return_value={"srcdefspeec_key": "srcdefspeec_val"},
)
def test_get_airbyte_source_definition_specifications_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source_definition_specifications(
        mock_request, "fake-sourcedef-id"
    )

    assert result["srcdefspeec_key"] == "srcdefspeec_val"
