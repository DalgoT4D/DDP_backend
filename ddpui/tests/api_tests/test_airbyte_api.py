import os
import django

from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlock, OrgWarehouse
from ddpui.api.airbyte_api import (
    get_airbyte_source_definitions,
    get_airbyte_source_definition_specifications,
    post_airbyte_source,
    put_airbyte_source,
    post_airbyte_check_source,
    post_airbyte_check_source_for_update,
    get_airbyte_sources,
    get_airbyte_source,
    get_airbyte_source_schema_catalog,
    get_airbyte_destination_definitions,
    get_airbyte_destination_definition_specifications,
    post_airbyte_destination,
    post_airbyte_check_destination,
    post_airbyte_check_destination_for_update,
    get_airbyte_destinations,
    get_airbyte_destination,
    get_job_status,
)
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteSourceUpdate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationCreate,
    AirbyteDestinationUpdateCheckConnection,
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
    return_value={
        "sourceDefinitions": [
            {"name": "name1"},
            {"name": "name2"},
            {"name": "name3"},
        ]
    },
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
def test_post_airbyte_source_success(create_source, org_with_workspace):
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
def test_put_airbyte_source_success(update_source, org_with_workspace):
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
def test_post_airbyte_check_source_failure(check_source_connection, org_with_workspace):
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
def test_post_airbyte_check_source_success(check_source_connection, org_with_workspace):
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
def test_post_airbyte_check_source_for_update_failure(
    check_source_connection_for_update, org_with_workspace
):
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
def test_post_airbyte_check_source_for_update_success(
    check_source_connection_for_update, org_with_workspace
):
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
def test_get_airbyte_destinations_success(org_with_workspace):
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


# ================================================================================
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
