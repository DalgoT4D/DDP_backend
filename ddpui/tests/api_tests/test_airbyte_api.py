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
    post_airbyte_source,
    put_airbyte_source,
    post_airbyte_check_source,
    post_airbyte_check_source_for_update,
    get_airbyte_sources,
    get_airbyte_source,
    delete_airbyte_source,
)
from ddpui.ddpairbyte.schema import (
    AirbyteWorkspaceCreate,
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteSourceUpdateCheckConnection,
)
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


def test_post_airbyte_source_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceCreate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_source(mock_request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.create_source",
    return_value={"sourceId": "fake-source-id"},
)
def test_post_airbyte_source_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceCreate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    source = post_airbyte_source(mock_request, fake_payload)

    assert source["sourceId"] == "fake-source-id"


def test_put_airbyte_source_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        put_airbyte_source(mock_request, "fake-source-id", fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_source",
    return_value={"sourceId": "fake-source-id"},
)
def test_put_airbyte_source_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    source = put_airbyte_source(mock_request, "fake-source-id", fake_payload)

    assert source["sourceId"] == "fake-source-id"


def test_post_airbyte_check_source_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source(mock_request, fake_payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_source_connection",
    return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}},
)
def test_post_airbyte_check_source_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source(mock_request, fake_payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_source_connection",
    return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}},
)
def test_post_airbyte_check_source_2(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdate(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source(mock_request, fake_payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


def test_post_airbyte_check_source_for_update_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_source_for_update(
            mock_request, "fake-source-id", fake_payload
        )

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_source_connection_for_update",
    return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1]}}},
)
def test_post_airbyte_check_source_for_update_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(
        mock_request, "fake-source-id", fake_payload
    )

    assert result["status"] == "failed"
    assert len(result["logs"]) == 1


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_source_connection_for_update",
    return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2]}}},
)
def test_post_airbyte_check_source_for_update_2(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    fake_payload = AirbyteSourceUpdateCheckConnection(
        name="temp-name", sourceDefId="fake-id", config={}
    )
    result = post_airbyte_check_source_for_update(
        mock_request, "fake-source-id", fake_payload
    )

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 2


def test_get_airbyte_sources_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_sources(
            mock_request,
        )

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_sources",
    return_value={"sources": [1, 2, 3]},
)
def test_get_airbyte_sources_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_sources(
        mock_request,
    )

    assert len(result) == 3


def test_get_airbyte_source_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source(mock_request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_source",
    return_value={"fake-key": "fake-val"},
)
def test_get_airbyte_source_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source(mock_request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


def test_delete_airbyte_source_0(org_without_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_source(mock_request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connections=Mock(
        return_value={"connections": [{"sourceId": "fake-source-id-1"}]}
    ),
    delete_source=Mock(),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbye_connection_blocks=Mock(return_value=[]),
    post_prefect_blocks_bulk_delete=Mock(),
)
def test_delete_airbyte_source_1(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = delete_airbyte_source(mock_request, "fake-source-id")

    assert result["success"] == 1


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connections=Mock(
        return_value={
            "connections": [
                {"sourceId": "fake-source-id-1", "connectionId": "fake-connection-id-1"}
            ]
        }
    ),
    delete_source=Mock(),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbye_connection_blocks=Mock(
        return_value=[{"connectionId": "fake-connection-id-1", "id": "fake-block-id-1"}]
    ),
    post_prefect_blocks_bulk_delete=Mock(),
)
def test_delete_airbyte_source_2(org_with_workspace):
    """tests GET /source_definitions"""

    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-block-id-1",
        block_name="fake-block-name-1",
    )
    assert OrgPrefectBlock.objects.filter(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-block-id-1",
        block_name="fake-block-name-1",
    ).exists()

    result = delete_airbyte_source(mock_request, "fake-source-id-1")

    assert not OrgPrefectBlock.objects.filter(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-block-id-1",
        block_name="fake-block-name-1",
    ).exists()

    assert result["success"] == 1
