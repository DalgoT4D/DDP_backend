import os
import django

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlock, OrgDataFlow, OrgWarehouse
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
    get_airbyte_source_schema_catalog,
    get_airbyte_destination_definitions,
    get_airbyte_destination_definition_specifications,
    post_airbyte_destination,
    post_airbyte_connection_reset,
    post_airbyte_check_destination,
    post_airbyte_check_destination_for_update,
    put_airbyte_destination,
    get_airbyte_destinations,
    get_airbyte_destination,
    get_airbyte_connections,
    get_airbyte_connection,
    post_airbyte_connection,
    put_airbyte_connection,
    delete_airbyte_connection,
    post_airbyte_sync_connection,
    get_job_status,
)
from ddpui.ddpairbyte.schema import (
    AirbyteWorkspace,
    AirbyteWorkspaceCreate,
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdateCheckConnection,
    AirbyteDestinationUpdate,
    AirbyteConnectionCreate,
    AirbyteConnectionUpdate,
)
from ddpui import ddpprefect

pytestmark = pytest.mark.django_db


# ================================================================================
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


# ================================================================================
def test_post_airbyte_detach_workspace_without_org():
    """tests /worksspace/detatch/"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_detach_workspace(mock_request)
    assert str(excinfo.value) == "create an organization first"


def test_post_airbyte_detach_workspace_without_workspace(org_without_workspace):
    """tests /worksspace/detatch/"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_detach_workspace(mock_request)
    assert str(excinfo.value) == "org already has no workspace"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_airbyte_connection_block=Mock(),
    delete_airbyte_server_block=Mock(),
)
def test_post_airbyte_detach_workspace_success(org_with_workspace):
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


# ================================================================================
def test_post_airbyte_workspace_with_workspace(org_with_workspace):
    """if the request passes the authentication check
    AND there are no airbyte server blocks for this org
    AND we can conenct to airbyte (or a mocked version of it)
    then post_airbyte_workspace must succeed
    """
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    testworkspacename = "Test Workspace"
    mock_payload = AirbyteWorkspaceCreate(name=testworkspacename)
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_workspace(mock_request, mock_payload)
    assert str(excinfo.value) == "org already has a workspace"


@patch(
    "ddpui.ddpairbyte.airbytehelpers.setup_airbyte_workspace",
    return_value=AirbyteWorkspace(
        name="workspace-name",
        workspaceId="workspaceId",
        initialSetupComplete=True,
    ),
)
def test_post_airbyte_workspace_success(setup_airbyte_workspace, org_without_workspace):
    """if the request passes the authentication check
    AND there are no airbyte server blocks for this org
    AND we can conenct to airbyte (or a mocked version of it)
    then post_airbyte_workspace must succeed
    """
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    testworkspacename = "workspace-name"
    mock_payload = AirbyteWorkspaceCreate(name=testworkspacename)
    response = post_airbyte_workspace(mock_request, mock_payload)
    assert response.name == testworkspacename


# ================================================================================
def test_get_airbyte_source_definitions_without_airbyte_workspace(
    org_without_workspace,
):
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
def test_get_airbyte_source_definitions_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source_definitions(mock_request)

    assert len(result) == 3


# ================================================================================
def test_get_airbyte_source_definition_specifications_without_workspace(
    org_without_workspace,
):
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
    return_value={"connectionSpecification": "srcdefspeec_val"},
)
def test_get_airbyte_source_definition_specifications_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source_definition_specifications(
        mock_request, "fake-sourcedef-id"
    )

    assert result == "srcdefspeec_val"


# ================================================================================
def test_post_airbyte_source_without_workspace(org_without_workspace):
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
def test_post_airbyte_source_success(org_with_workspace):
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


# ================================================================================
def test_put_airbyte_source_without_workspace(org_without_workspace):
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
def test_put_airbyte_source_success(org_with_workspace):
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


# ================================================================================
def test_post_airbyte_check_source_with_workspace(org_without_workspace):
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
def test_post_airbyte_check_source_failure(org_with_workspace):
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
def test_post_airbyte_check_source_success(org_with_workspace):
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


# ================================================================================
def test_post_airbyte_check_source_for_update_without_workspace(org_without_workspace):
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
def test_post_airbyte_check_source_for_update_failure(org_with_workspace):
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
def test_post_airbyte_check_source_for_update_success(org_with_workspace):
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


# ================================================================================
def test_get_airbyte_sources_without_workspace(org_without_workspace):
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
def test_get_airbyte_sources_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_sources(
        mock_request,
    )

    assert len(result) == 3


# ================================================================================
def test_get_airbyte_source_without_workspace(org_without_workspace):
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
def test_get_airbyte_source_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source(mock_request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_delete_airbyte_source_without_workspace(org_without_workspace):
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


# ================================================================================
def test_get_airbyte_source_schema_catalog_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_source_schema_catalog(mock_request, "fake-source-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
    return_value={"fake-key": "fake-val"},
)
def test_get_airbyte_source_schema_catalog_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_source_schema_catalog(mock_request, "fake-source-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_definitions_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definitions(mock_request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_destination_definitions",
    return_value={"destinationDefinitions": [{"name": "dest1"}, {"name": "dest3"}]},
)
def test_get_airbyte_destination_definitions_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definitions(mock_request)

    assert len(result) == 1
    assert result[0]["name"] == "dest1"


# ================================================================================
def test_get_airbyte_destination_definition_specifications_without_workspace(
    org_without_workspace,
):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination_definition_specifications(
            mock_request, "fake-dest-def-id"
        )

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_destination_definition_specification",
    return_value={"connectionSpecification": {"fake-key": "fake-val"}},
)
def test_get_airbyte_destination_definition_specifications_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    os.environ["AIRBYTE_DESTINATION_TYPES"] = "dest1,dest2"
    result = get_airbyte_destination_definition_specifications(
        mock_request, "fake-dest-def-id"
    )

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_post_airbyte_destination_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_destination(mock_request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.create_destination",
    return_value={"destinationId": "fake-dest-id"},
)
def test_post_airbyte_destination_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_destination(mock_request, payload)

    assert result["destinationId"] == "fake-dest-id"


# ================================================================================
def test_post_airbyte_check_destination_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination(mock_request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_destination_connection",
    return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}},
)
def test_post_airbyte_check_destination_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(mock_request, payload)

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_destination_connection",
    return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}},
)
def test_post_airbyte_check_destination_failure(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationCreate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = post_airbyte_check_destination(mock_request, payload)

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_post_airbyte_check_destination_for_update_without_workspace(
    org_without_workspace,
):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_check_destination_for_update(mock_request, "fake-dest-id", payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_destination_connection_for_update",
    return_value={"jobInfo": {"succeeded": True, "logs": {"logLines": [1, 2, 3]}}},
)
def test_post_airbyte_check_destination_for_update_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(
        mock_request, "fake-dest-id", payload
    )

    assert result["status"] == "succeeded"
    assert len(result["logs"]) == 3


@patch(
    "ddpui.ddpairbyte.airbyte_service.check_destination_connection_for_update",
    return_value={"jobInfo": {"succeeded": False, "logs": {"logLines": [1, 2, 3, 4]}}},
)
def test_post_airbyte_check_destination_for_update_failure(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationUpdateCheckConnection(
        name="fake-dest-name",
        config={},
    )
    result = post_airbyte_check_destination_for_update(
        mock_request, "fake-dest-id", payload
    )

    assert result["status"] == "failed"
    assert len(result["logs"]) == 4


# ================================================================================
def test_put_airbyte_destination_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationUpdate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    with pytest.raises(HttpError) as excinfo:
        put_airbyte_destination(mock_request, "fake-dest-id", payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.update_destination",
    return_value={"destinationId": "fake-dest-id"},
)
def test_put_airbyte_destination_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteDestinationUpdate(
        name="fake-dest-name",
        destinationDefId="fake-dest-def-id",
        config={},
    )
    result = put_airbyte_destination(mock_request, "fake-dest-id", payload)

    assert result["destinationId"] == "fake-dest-id"


# ================================================================================
def test_get_airbyte_destinations_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destinations(mock_request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_destinations",
    return_value={"destinations": [{"fake-key": "fake-val"}]},
)
def test_get_airbyte_destination_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_destinations(mock_request)

    assert len(result) == 1
    assert result[0]["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_destination_without_workspace(org_without_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_destination(mock_request, "fake-dest-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch(
    "ddpui.ddpairbyte.airbyte_service.get_destination",
    return_value={"fake-key": "fake-val"},
)
def test_get_airbyte_destination_success(org_with_workspace):
    """tests GET /source_definitions"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    result = get_airbyte_destination(mock_request, "fake-dest-id")

    assert result["fake-key"] == "fake-val"


# ================================================================================
def test_get_airbyte_connections_without_workspace(org_without_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connections(mock_request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(
        return_value={
            "sourceId": "fake-source-id-1",
            "connectionId": "fake-connection-id-1",
            "destinationId": "fake-destination-id-1",
            "sourceCatalogId": "fake-source-catalog-id-1",
            "syncCatalog": "sync-catalog",
            "status": "conn-status",
        }
    ),
    get_source=Mock(return_value={"sourceName": "fake-source-name-1"}),
    get_destination=Mock(return_value={"destinationName": "fake-destination-name-1"}),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={
            "data": {"connection_id": "fake-connection-id"},
            "name": "fake-block-name",
        }
    ),
    get_last_flow_run_by_deployment_id=Mock(return_value="lastRun"),
)
def test_get_airbyte_connectionn_success(org_with_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-block-id",
        block_name="fake-block-name",
        display_name="fake-display-name",
    )

    OrgDataFlow.objects.create(
        org=org_with_workspace,
        connection_id="fake-connection-id-1",
        deployment_id="fake-deployment-id",
    )

    result = get_airbyte_connections(mock_request)
    assert len(result) == 1

    assert result[0]["name"] == "fake-display-name"
    assert result[0]["blockId"] == "fake-block-id"
    assert result[0]["blockName"] == "fake-block-name"
    assert result[0]["blockData"]["connection_id"] == "fake-connection-id"
    assert result[0]["source"]["id"] == "fake-source-id-1"
    assert result[0]["source"]["name"] == "fake-source-name-1"
    assert result[0]["destination"]["id"] == "fake-destination-id-1"
    assert result[0]["destination"]["name"] == "fake-destination-name-1"
    assert result[0]["sourceCatalogId"] == "fake-source-catalog-id-1"
    assert result[0]["syncCatalog"] == "sync-catalog"
    assert result[0]["status"] == "conn-status"
    assert result[0]["deploymentId"] == "fake-deployment-id"
    assert result[0]["lastRun"] == "lastRun"


# ================================================================================
def test_get_airbyte_connection_without_workspace(org_without_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connection(mock_request, "fake-block-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(
        return_value={
            "sourceId": "fake-source-id-1",
            "connectionId": "fake-connection-id-1",
            "destinationId": "fake-destination-id-1",
            "sourceCatalogId": "fake-source-catalog-id-1",
            "syncCatalog": "sync-catalog",
            "namespaceDefinition": "namespace-definition",
            "status": "conn-status",
        }
    ),
    get_source=Mock(return_value={"sourceName": "fake-source-name-1"}),
    get_destination=Mock(return_value={"destinationName": "fake-destination-name-1"}),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={
            "id": "fake-block-id",
            "data": {"connection_id": "fake-connection-id"},
            "name": "fake-block-name",
        }
    ),
    get_last_flow_run_by_deployment_id=Mock(return_value="lastRun"),
)
def test_get_airbyte_connection_success(org_with_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="fake-block-id",
        block_name="fake-block-name",
        display_name="fake-display-name",
    )

    OrgDataFlow.objects.create(
        org=org_with_workspace,
        connection_id="fake-connection-id-1",
        deployment_id="fake-deployment-id",
    )

    result = get_airbyte_connection(mock_request, "fake-block-id")

    assert result["name"] == "fake-display-name"
    assert result["blockId"] == "fake-block-id"
    assert result["blockName"] == "fake-block-name"
    assert result["blockData"]["connection_id"] == "fake-connection-id"
    assert result["source"]["id"] == "fake-source-id-1"
    assert result["source"]["name"] == "fake-source-name-1"
    assert result["destination"]["id"] == "fake-destination-id-1"
    assert result["destination"]["name"] == "fake-destination-name-1"
    assert result["sourceCatalogId"] == "fake-source-catalog-id-1"
    assert result["syncCatalog"] == "sync-catalog"
    assert result["status"] == "conn-status"
    assert result["deploymentId"] == "fake-deployment-id"


# ================================================================================
def test_post_airbyte_connection_without_workspace(org_without_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream-1", "stream-2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection(mock_request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_without_stream_names(org_with_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=[],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection(mock_request, payload)

    assert str(excinfo.value) == "must specify stream names"


def test_post_airbyte_connection_without_warehouse(org_with_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection(mock_request, payload)

    assert str(excinfo.value) == "need to set up a warehouse first"


@pytest.fixture
def warehouse_without_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(org=org_with_workspace)
    yield warehouse
    warehouse.delete()


@pytest.fixture
def warehouse_with_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    )
    yield warehouse
    warehouse.delete()


def test_post_airbyte_connection_without_destination_id(
    org_with_workspace, warehouse_without_destination
):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection(mock_request, payload)

    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_normalization_operation=Mock(
        return_value={"operationId": "fake-operation-id"}
    ),
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
)
def test_post_airbyte_connection_without_server_block(
    org_with_workspace, warehouse_with_destination
):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )
    with pytest.raises(Exception) as excinfo:
        post_airbyte_connection(mock_request, payload)
    assert (
        str(excinfo.value)
        == "test-org-slug has no Airbyte Server block in OrgPrefectBlock"
    )


@pytest.fixture
def airbyte_server_block(org_with_workspace):
    block = OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake ab server block",
    )
    yield block
    block.delete()


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_normalization_operation=Mock(
        return_value={"operationId": "fake-operation-id"}
    ),
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
    get_source=Mock(
        return_value={"sourceName": "source-name"},
    ),
    get_destination=Mock(
        return_value={"destinationName": "destination-name"},
    ),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={
            "name": "airbyte-connection-block-name",
            "id": "airbyte-connection-block-id",
            "data": "block-data",
        }
    ),
    create_airbyte_connection_block=Mock(return_value="fake-block_id"),
    create_dataflow=Mock(
        return_value={
            "deployment": {"id": "fake-deployment-id", "name": "fake-deployment-name"}
        }
    ),
)
def test_post_airbyte_connection_success(
    org_with_workspace, warehouse_with_destination, airbyte_server_block
):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
        normalize=False,
    )

    response = post_airbyte_connection(mock_request, payload)

    # reload the warehouse from the database
    warehouse_with_destination = OrgWarehouse.objects.filter(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    ).first()
    assert warehouse_with_destination.airbyte_norm_op_id is not None

    assert response["name"] == "conn-name"
    assert response["blockId"] == "airbyte-connection-block-id"
    assert response["blockName"] == "airbyte-connection-block-name"
    assert response["blockData"] == "block-data"
    assert response["connectionId"] == "fake-connection-id"
    assert response["source"]["id"] == "fake-source-id"
    assert response["destination"]["id"] == "fake-destination-id"
    assert response["sourceCatalogId"] == "fake-source-catalog-id"
    assert response["status"] == "running"
    assert response["deployment_id"] == "fake-deployment-id"


# ================================================================================
def test_post_airbyte_connection_reset_no_workspace(org_without_workspace):
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_without_workspace

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset(mock_request, "connection_block_id")
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_no_block(org_with_workspace):
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset(mock_request, "connection_block_id")
    assert str(excinfo.value) == "connection block not found"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(return_value={"wrong-key": "wrong-val"}),
)
def test_post_airbyte_connection_no_connection_in_block(org_with_workspace):
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    connection_block_id = "connection_block_id"

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset(mock_request, connection_block_id)
    assert str(excinfo.value) == "connection is missing from the block"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"wrong-key": "wrong-val"}}
    ),
)
def test_post_airbyte_connection_no_connection_in_block_2(org_with_workspace):
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    connection_block_id = "connection_block_id"

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset(mock_request, connection_block_id)
    assert str(excinfo.value) == "connection is missing from the block"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "the-connection-id"}}
    ),
)
def test_post_airbyte_connection_success(org_with_workspace):
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    connection_block_id = "connection_block_id"

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    reset_connection_mock = MagicMock()

    with patch(
        "ddpui.ddpairbyte.airbyte_service.reset_connection"
    ) as reset_connection_mock:
        result = post_airbyte_connection_reset(mock_request, connection_block_id)
        assert result["success"] == 1
    reset_connection_mock.assert_called_once_with("the-connection-id")


# ================================================================================
def test_put_airbyte_connection_no_workspace(org_without_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_without_workspace

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_put_airbyte_connection_no_streams(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "must specify stream names"


def test_put_airbyte_connection_no_connection_block(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "connection block not found"


def test_put_airbyte_connection_no_warehouse(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "need to set up a warehouse first"


def test_put_airbyte_connection_no_warehouse_destination(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    OrgWarehouse.objects.create(org=org_with_workspace)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(return_value={"not-data": True}),
)
def test_put_airbyte_connection_no_connection_in_block(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "connection if missing from the block"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(return_value={"data": {}}),
)
def test_put_airbyte_connection_no_connection_in_block_2(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    OrgPrefectBlock.objects.create(org=org_with_workspace, block_id=connection_block_id)

    OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection(mock_request, connection_block_id, payload)
    assert str(excinfo.value) == "connection if missing from the block"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"conn-key": "conn-val"}),
)
def test_put_airbyte_connection_without_normalization(org_with_workspace):
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    opb = OrgPrefectBlock.objects.create(
        org=org_with_workspace, block_id=connection_block_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection(mock_request, connection_block_id, payload)

    opb.refresh_from_db()
    assert opb.display_name == payload.name

    connection = {
        "conn-key": "conn-val",
        "operationIds": [],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"operationIds": [1]}),
)
def test_put_airbyte_connection_with_normalization_with_opids(org_with_workspace):
    payload = AirbyteConnectionUpdate(
        name="connection-block-name", streams=[1], normalize=True
    )
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    opb = OrgPrefectBlock.objects.create(
        org=org_with_workspace, block_id=connection_block_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection(mock_request, connection_block_id, payload)

    opb.refresh_from_db()
    assert opb.display_name == payload.name

    connection = {
        "operationIds": [1],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"operationIds": []}),
    create_normalization_operation=Mock(return_value={"operationId": "norm-op-id"}),
)
def test_put_airbyte_connection_with_normalization_without_opids(org_with_workspace):
    payload = AirbyteConnectionUpdate(
        name="connection-block-name", streams=[1], normalize=True
    )
    connection_block_id = "connection_block_id"
    mock_request = Mock()
    mock_request.orguser = Mock()
    mock_request.orguser.org = org_with_workspace

    opb = OrgPrefectBlock.objects.create(
        org=org_with_workspace, block_id=connection_block_id
    )

    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch(
        "ddpui.ddpairbyte.airbyte_service.update_connection"
    ) as update_connection_mock:
        put_airbyte_connection(mock_request, connection_block_id, payload)

    opb.refresh_from_db()
    assert opb.display_name == payload.name
    warehouse.refresh_from_db()
    assert warehouse.airbyte_norm_op_id == "norm-op-id"

    connection = {
        "operationIds": [warehouse.airbyte_norm_op_id],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        org_with_workspace.airbyte_workspace_id, payload, connection
    )


# ================================================================================
def test_delete_airbyte_connection_without_org():
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection(mock_request, "conn-block-id")

    assert str(excinfo.value) == "create an organization first"


def test_delete_airbyte_connection_without_workspace(org_without_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection(mock_request, "conn-block-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_airbyte_connection_block_by_id=Mock(
        return_value={"data": {"connection_id": "connection-id"}}
    ),
    delete_airbyte_connection_block=Mock(),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    delete_connection=Mock(),
)
def test_delete_airbyte_connection_success(org_with_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTECONNECTION,
        block_id="conn-block-id",
        block_name="conn-block-name",
    )
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace,
            block_type=ddpprefect.AIRBYTECONNECTION,
            block_id="conn-block-id",
            block_name="conn-block-name",
        ).count()
        == 1
    )
    response = delete_airbyte_connection(mock_request, "conn-block-id")
    assert response["success"] == 1
    assert (
        OrgPrefectBlock.objects.filter(
            org=org_with_workspace,
            block_type=ddpprefect.AIRBYTECONNECTION,
            block_id="conn-block-id",
            block_name="conn-block-name",
        ).count()
        == 0
    )


# ================================================================================
def test_post_airbyte_sync_connection_without_workspace(org_without_workspace):
    mock_orguser = Mock()
    mock_orguser.org = org_without_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_sync_connection(mock_request, "conn-block-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@pytest.fixture
def org_prefect_connection_block(org_with_workspace):
    block = OrgPrefectBlock.objects.create(
        org=org_with_workspace,
        block_id="connection_block_id",
        block_name="temp-conn-block-name",
        block_type=ddpprefect.AIRBYTECONNECTION,
    )
    yield block
    block.delete()


@patch.multiple(
    "ddpui.api.client.airbyte_api",
    run_airbyte_connection_sync=Mock(return_value="retval"),
)
def test_post_airbyte_sync_connection_success(
    org_with_workspace, org_prefect_connection_block
):
    mock_orguser = Mock()
    mock_orguser.org = org_with_workspace

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    response = post_airbyte_sync_connection(mock_request, "connection_block_id")
    assert response == "retval"


# ================================================================================
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_job_info=Mock(
        return_value={
            "attempts": [{"logs": {"logLines": [1, 2, 3]}}],
            "job": {"status": "completed"},
        }
    ),
)
def test_get_job_status():
    mock_request = Mock()

    result = get_job_status(mock_request, "fake-job-id")
    assert result["status"] == "completed"
    assert len(result["logs"]) == 3
