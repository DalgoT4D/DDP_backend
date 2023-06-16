import os
from unittest import mock
from unittest.mock import patch
import django
from pydantic import ValidationError
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ninja.errors import HttpError
from ddpui.tests.helper.test_airbyte_unit_schemas import *
from ddpui.ddpairbyte.airbyte_service import *


@pytest.fixture(scope="module")
def valid_workspace_id():
    result = create_workspace("Example Workspace")
    workspace_id = result["workspaceId"]
    return workspace_id


@pytest.fixture
def invalid_workspace_id():
    return 123


@pytest.fixture
def valid_name():
    return "Example Workspace"


@pytest.fixture
def invalid_name():
    return 123


@pytest.fixture(scope="module")
def valid_sourcedef_id(valid_workspace_id):
    source_definitions = get_source_definitions(workspace_id=valid_workspace_id)[
        "sourceDefinitions"
    ]

    for source_definition in source_definitions:
        if source_definition["name"] == "File (CSV, JSON, Excel, Feather, Parquet)":
            source_definition_id = source_definition["sourceDefinitionId"]
            break
    return source_definition_id


def mock_abreq(endpoint, data):
    return {"connectionSpecification": {"test": "data"}}


def test_abreq_success():
    endpoint = "workspaces/list"
    expected_response = {
        "workspaces": [{"workspaceId": "1", "name": "Example Workspace"}]
    }

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = expected_response

        result = abreq(endpoint)

    assert isinstance(result, dict)
    assert result == expected_response
    assert "workspaces" in result
    assert isinstance(result["workspaces"], list)


def test_abreq_connection_error():
    endpoint = "my_endpoint"

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ConnectionError(
            "Error connecting to Airbyte server"
        )

        with pytest.raises(HttpError) as excinfo:
            abreq(endpoint)

        assert excinfo.value.status_code == 500
        print(excinfo)
        assert str(excinfo.value) == "Error connecting to Airbyte server"


# def test_abreq_invalid_request_data():
#     endpoint = "workspaces/create"
#     req = {"invalid_key": "invalid_value"}

#     with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
#         mock_post.return_value.status_code = 400
#         mock_post.return_value.headers = {"Content-Type": "application/json"}
#         mock_post.return_value.json.return_value = {"error": "Invalid request data"}
#         with pytest.raises(HttpError) as excinfo:
#             abreq(endpoint, req)
#         assert excinfo.value.status_code == 400
#         assert str(excinfo.value) == "Something went wrong: Invalid request data"


def test_get_workspaces_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "workspaces": [{"workspaceId": "1", "name": "Example Workspace"}]
        }

        result = get_workspaces()
        assert isinstance(result, list)
        assert all(isinstance(workspace, dict) for workspace in result)


def test_get_workspaces_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_workspaces()
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "no workspaces found"


def test_create_workspace_with_valid_name(valid_name):
    # check if workspace is created successfully using mock_abreq
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "workspaceId": "1",
            "name": "Example Workspace",
        }

        result = create_workspace(valid_name)
        assert "workspaceId" in result
        assert isinstance(result, dict)


def test_create_workspace_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        create_workspace(123)
    assert str(excinfo.value) == "Name must be a string"


def test_create_workspace_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 400
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            create_workspace("test_workspace")
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "workspace not created"


def test_get_workspace_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "workspaceId": "test",
            "name": "Example Workspace",
        }
        result = get_workspace("test")
        assert "workspaceId" in result
        assert isinstance(result, dict)


def test_get_workspace_success_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_workspace(123)
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_workspace_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_workspace("test")
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "workspace not found"


def test_set_workspace_name_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "workspaceId": "test",
            "name": "Example Workspace",
        }
        result = set_workspace_name("test", "New Name")
        assert "workspaceId" in result
        assert isinstance(result, dict)


def test_set_workspace_name_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        set_workspace_name(123, "New Name")
    assert str(excinfo.value) == "Workspace ID must be a string"


def test_set_workspace_name_with_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        set_workspace_name("test", 123)
    assert str(excinfo.value) == "Name must be a string"


def test_set_workspace_name_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            set_workspace_name("test", "New Name")
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "workspace not found"


def test_get_source_definitions_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "sourceDefinitions": [
                {"sourceDefinitionId": "1", "name": "Example Source Definition 1"},
                {"sourceDefinitionId": "2", "name": "Example Source Definition 2"},
            ]
        }
        result = get_source_definitions("test")["sourceDefinitions"]
        assert isinstance(result, list)


def test_get_source_definitions_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source_definitions("test")["sourceDefinitions"]
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == f"Source definitions not found for workspace: test"


def test_get_source_definitions_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_source_definitions(123)["sourceDefinitions"]
    assert str(excinfo.value) == "Invalid workspace ID"


def test_get_source_definition_specification_success():
    workspace_id = "my_workspace_id"
    sourcedef_id = "my_sourcedef_id"
    expected_response = {"key": "value"}

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {"connectionSpecification": expected_response}
        result = get_source_definition_specification(workspace_id, sourcedef_id)[
            "connectionSpecification"
        ]

    assert result == expected_response
    assert isinstance(result, dict)


def test_get_source_definition_specification_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source_definition_specification("test", "1")
        assert excinfo.value.status_code == 404
        assert (
            str(excinfo.value)
            == "specification not found for source definition 1 in workspace test"
        )


def test_get_source_definition_specification_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_source_definition_specification(123, "1")
    assert str(excinfo.value) == "Invalid workspace ID"


def test_get_source_definition_specification_with_invalid_source_definition_id():
    with pytest.raises(HttpError) as excinfo:
        get_source_definition_specification("test", 123)
    assert str(excinfo.value) == "Invalid source definition ID"


def test_get_sources_success():
    workspace_id = "my_workspace_id"
    expected_response = {"sources": [{"sourceId": "1", "name": "Example Source 1"}]}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = expected_response
        result = get_sources(workspace_id)["sources"]
        assert isinstance(result, list)


def test_get_sources_failure():
    workspace_id = "my_workspace_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_sources(workspace_id)
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "sources not found for workspace"


def test_get_sources_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_sources(123)
    assert str(excinfo.value) == "Invalid workspace ID"


def test_get_source_success():
    workspace_id = "my_workspace_id"
    source_id = "1"
    expected_response = {"sourceId": "1", "name": "Example Source 1"}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = expected_response
        result = get_source(workspace_id, source_id)

        assert result == expected_response
        assert isinstance(result, dict)


def test_get_source_failure():
    workspace_id = "my_workspace_id"
    source_id = "1"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source(workspace_id, source_id)
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "source not found"


def test_get_source_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_source(123, "1")
    assert str(excinfo.value) == "Invalid workspace ID"


def test_get_source_with_invalid_source_id():
    with pytest.raises(HttpError) as excinfo:
        get_source("test", 123)
    assert str(excinfo.value) == "Invalid source ID"


def test_delete_source_success():
    workspace_id = "my_workspace_id"
    source_id = "1"
    expected_response = {"sourceId": "1", "name": "Example Source 1"}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = expected_response
        result = delete_source(workspace_id, source_id)

        assert result == expected_response
        assert isinstance(result, dict)


def test_delete_source_failure():
    workspace_id = "my_workspace_id"
    source_id = "1"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            delete_source(workspace_id, source_id)
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "source not found"


def test_delete_source_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        delete_source(123, "1")
    assert str(excinfo.value) == "Invalid workspace ID"


def test_delete_source_with_invalid_source_id():
    with pytest.raises(HttpError) as excinfo:
        delete_source("test", 123)
    assert str(excinfo.value) == "Invalid source ID"


def test_create_source_success():
    workspace_id = "my_workspace_id"
    expected_response = {
        "sourceId": "1",
        "name": "Example Source 1",
        "sourcedef_id": "1",
        "config": {"test": "test"},
    }
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = expected_response
        mock_post.return_value.headers = {"Content-Type": "application/json"}

        result = create_source(workspace_id, "Example Source 1", "1", {"test": "test"})
        assert result == expected_response
        assert isinstance(result, dict)


def test_create_source_failure():
    workspace_id = "my_workspace_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            create_source(workspace_id, "Example Source 1", "1", {"test": "test"})
        assert excinfo.value.status_code == 500
        assert str(excinfo.value) == "failed to create source"


def test_create_source_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        create_source(123, "Example Source 1", "1", {"test": "test"})
    assert str(excinfo.value) == "workspace_id must be a string"


def test_create_source_with_invalid_source_name():
    with pytest.raises(HttpError) as excinfo:
        create_source("test", 123, "1", {"test": "test"})
    assert str(excinfo.value) == "name must be a string"


def test_create_source_with_invalid_sourcedef_id():
    with pytest.raises(HttpError) as excinfo:
        create_source("test", "Example Source 1", 123, {"test": "test"})
    assert str(excinfo.value) == "sourcedef_id must be a string"


def test_create_source_with_invalid_config():
    with pytest.raises(HttpError) as excinfo:
        create_source("test", "test", "test", 123)
    assert str(excinfo.value) == "config must be a dictionary"


def test_update_source_success():
    name = "source"
    source_id = "1"
    sourcedef_id = "1"
    expected_response = {
        "sourceId": "1",
        "name": "Example Source 1",
        "config": {"test": "test"},
        "sourcedef_id": "1",
    }
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = expected_response
        mock_post.return_value.headers = {"Content-Type": "application/json"}

        result = update_source(source_id, name, {"test": "test"}, sourcedef_id)
        assert result == expected_response
        assert isinstance(result, dict)


def test_update_source_failure():
    name = "source"
    source_id = "1"
    sourcedef_id = "1"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            update_source(source_id, name, {"test": "test"}, sourcedef_id)
        assert excinfo.value.status_code == 500
        assert str(excinfo.value) == "failed to update source"


def test_update_source_with_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        update_source("1", 123, {"test": "test"}, "1")
    assert str(excinfo.value) == "name must be a string"


def test_update_source_with_invalid_source_id():
    with pytest.raises(HttpError) as excinfo:
        update_source(123, "test", {"test": "test"}, "1")
    assert str(excinfo.value) == "source_id must be a string"


def test_update_source_with_invalid_config():
    with pytest.raises(HttpError) as excinfo:
        update_source("test", "test", 123, "1")
    assert str(excinfo.value) == "config must be a dictionary"


def test_update_source_with_invalid_sourcedef_id():
    with pytest.raises(HttpError) as excinfo:
        update_source("test", "test", {"test": "test"}, 123)
    assert str(excinfo.value) == "sourcedef_id must be a string"


def test_check_source_connection_success():
    workspace_id = "my_workspace_id"
    data = AirbyteSourceCreate(
        name="my_source_name",
        sourceDefId="my_sourcedef_id",
        config={"key": "value"},
    )
    expected_response = {"status": "succeeded", "jobInfo": {}}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = expected_response
        mock_post.return_value.headers = {"Content-Type": "application/json"}

        result = check_source_connection(workspace_id, data)
        assert result == expected_response
        assert isinstance(result, dict)


def test_check_source_connection_failure():
    workspace_id = "my_workspace_id"
    data = AirbyteSourceCreate(
        name="my_source_name",
        sourceDefId="my_sourcedef_id",
        config={"key": "value"},
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = requests.Response()
        mock_response.status_code = 500
        mock_response._content = b'{"error": "failed to check source connection"}'
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            result = check_source_connection(workspace_id, data)
            assert result is None
            assert excinfo.value.status_code == 500
            assert str(excinfo.value) == "failed to check source connection"


def test_check_source_connection_with_invalid_workspace_id():
    workspace_id = 123
    data = AirbyteSourceCreate(
        name="my_source_name",
        sourceDefId="my_sourcedef_id",
        config={"key": "value"},
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            check_source_connection(workspace_id, data)
        assert str(excinfo.value) == "workspace_id must be a string"


def test_get_source_schema_catalog_success():
    workspace_id = "my_workspace_id"
    source_id = "my_source_id"
    expected_response = {"catalog": "catalog"}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = expected_response
        mock_post.return_value.headers = {"Content-Type": "application/json"}

        result = get_source_schema_catalog(workspace_id, source_id)
        assert result == expected_response
        assert isinstance(result, dict)


def test_get_source_schema_catalog_failure():
    workspace_id = "my_workspace_id"
    source_id = "my_source_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = requests.Response()
        mock_response.status_code = 500
        mock_response._content = b'{"error": "failed to get source schema catalogs"}'
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
            assert excinfo.value.status_code == 500
            assert str(excinfo.value) == "failed to get source schema catalogs"


def test_get_source_schema_catalog_with_invalid_workspace_id():
    workspace_id = 123
    source_id = "my_source_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
        assert str(excinfo.value) == "workspace_id must be a string"


def test_get_source_schema_catalog_with_invalid_source_id():
    workspace_id = "my_workspace_id"
    source_id = 123
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
        assert str(excinfo.value) == "source_id must be a string"
