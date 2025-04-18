import os
from unittest.mock import patch, Mock
import requests
import django
import pytest

from ddpui.ddpairbyte.schema import AirbyteConnectionSchemaUpdate

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ninja.errors import HttpError
from ddpui.tests.helper.test_airbyte_unit_schemas import *
from ddpui import settings
from ddpui.ddpairbyte.airbyte_service import (
    abreq,
    create_workspace,
    get_connection_catalog,
    get_source_definitions,
    get_workspaces,
    get_workspace,
    set_workspace_name,
    get_source_definition_specification,
    create_custom_source_definition,
    get_sources,
    get_source,
    delete_source,
    create_source,
    update_schema_change,
    update_source,
    AirbyteSourceCreate,
    check_source_connection,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationUpdateCheckConnection,
    check_source_connection_for_update,
    get_source_schema_catalog,
    get_destination_definitions,
    get_destination_definition_specification,
    get_destinations,
    get_destination,
    get_destination_definition,
    create_destination,
    update_destination,
    AirbyteDestinationCreate,
    check_destination_connection,
    check_destination_connection_for_update,
    get_connections,
    get_connection,
    update_connection,
    reset_connection,
    delete_connection,
    sync_connection,
    get_job_info,
    get_job_info_without_logs,
    get_jobs_for_connection,
    parse_job_info,
    get_logs_for_job,
    schema,
    create_connection,
)


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
    expected_response = {"workspaces": [{"workspaceId": "1", "name": "Example Workspace"}]}

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

        result = get_workspaces()["workspaces"]
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
                {
                    "sourceDefinitionId": "1",
                    "name": "Example Source Definition 1",
                    "dockerRepository": "docker-repo",
                },
                {
                    "sourceDefinitionId": "2",
                    "name": "Example Source Definition 2",
                    "dockerRepository": "docker-repo",
                },
            ]
        }
        result = get_source_definitions("test")["sourceDefinitions"]
        assert isinstance(result, list)
        assert len(result) == 2


def test_get_source_definitions_blacklist():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {
            "sourceDefinitions": [
                {
                    "sourceDefinitionId": "1",
                    "name": "Example Source Definition 1",
                    "dockerRepository": "docker-repo",
                },
                {
                    "sourceDefinitionId": "2",
                    "name": "Example Source Definition 2",
                    "dockerRepository": "blacklisted",
                },
            ]
        }
        settings.AIRBYTE_SOURCE_BLACKLIST = ["blacklisted"]
        result = get_source_definitions("test")["sourceDefinitions"]
        assert len(result) == 1


def test_get_source_definitions_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            get_source_definitions("test")["sourceDefinitions"]
        assert excinfo.value.status_code == 404
        assert str(excinfo.value) == "Source definitions not found for workspace: test"


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
        result = get_source_definition_specification(workspace_id, sourcedef_id)

    assert result == {"connectionSpecification": expected_response}
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


def test_create_custom_source_definition_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        create_custom_source_definition(123, "test", "test", "test", "test")
    assert str(excinfo.value) == "Invalid workspace ID"


def test_create_custom_source_definition_with_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        create_custom_source_definition("test", 123, "test", "test", "test")
    assert str(excinfo.value) == "Invalid name"


def test_create_custom_source_definition_with_invalid_docker_repository():
    with pytest.raises(HttpError) as excinfo:
        create_custom_source_definition("test", "test", 123, "test", "test")
    assert str(excinfo.value) == "Invalid docker repository"


def test_create_custom_source_definition_with_invalid_docker_image_tag():
    with pytest.raises(HttpError) as excinfo:
        create_custom_source_definition("test", "test", "test", 123, "test")
    assert str(excinfo.value) == "Invalid docker image tag"


def test_create_custom_source_definition_with_invalid_documentation_url():
    with pytest.raises(HttpError) as excinfo:
        create_custom_source_definition("test", "test", "test", "test", 123)
    assert str(excinfo.value) == "Invalid documentation URL"


def test_create_custom_source_definition_success():
    workspace_id = "my_workspace_id"
    name = "test"
    docker_repository = "test"
    docker_image_tag = "test"
    documentation_url = "test"
    expected_response = {"sourceDefinitionId": "1", "name": "test"}

    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = expected_response
        result = create_custom_source_definition(
            workspace_id, name, docker_repository, docker_image_tag, documentation_url
        )
        assert result == expected_response
        assert isinstance(result, dict)


def test_create_custom_source_definition_failure():
    workspace_id = "my_workspace_id"
    name = "test"
    docker_repository = "test"
    docker_image_tag = "test"
    documentation_url = "test"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_post.return_value.status_code = 404
        mock_post.return_value.headers = {"Content-Type": "application/json"}
        mock_post.return_value.json.return_value = {"error": "Invalid request data"}
        with pytest.raises(HttpError) as excinfo:
            create_custom_source_definition(
                workspace_id,
                name,
                docker_repository,
                docker_image_tag,
                documentation_url,
            )
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == f"Source definition not created: {name}"


def test_get_sources_success():
    workspace_id = "my_workspace_id"
    expected_response = {"sources": [{"sourceId": "1", "name": "Example Source 1"}]}

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = expected_response
        result = get_sources(workspace_id)

    assert result == expected_response
    assert isinstance(result, dict)
    assert "sources" in result
    assert isinstance(result["sources"], list)


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

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = expected_response
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
    with patch("ddpui.ddpairbyte.airbyte_service.abreq", return_value="abreq-retval"):
        response = delete_source(workspace_id, source_id)
        assert response == "abreq-retval"


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
    expected_response_srcdef = {"connectionSpecification": {"properties": {}}}
    expected_response_src = {
        "sourceId": "1",
        "name": "Example Source 1",
        "sourcedef_id": "1",
        "config": {"test": "test"},
    }
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq_:
        mock_abreq_.side_effect = [
            expected_response_srcdef,
            expected_response_src,
        ]

        result = create_source(workspace_id, "Example Source 1", "1", {"test": "test"})
        assert result == expected_response_src
        assert isinstance(result, dict)


def test_create_source_failure():
    workspace_id = "my_workspace_id"
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq_:
        mock_abreq_.side_effect = [
            {"connectionSpecification": {"properties": {}}},
            {"error": "Invalid request data"},
        ]
        with pytest.raises(HttpError) as excinfo:
            create_source(workspace_id, "Example Source 1", "1", {"test": "test"})
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "Failed to create source: Example Source 1"


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
    expected_response_srcdef = {"connectionSpecification": {"properties": {}}}
    expected_response_src = {"status": "succeeded", "jobInfo": {}}

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq_:
        mock_abreq_.side_effect = [expected_response_srcdef, expected_response_src]

        result = check_source_connection(workspace_id, data)
        assert result == expected_response_src
        assert isinstance(result, dict)


def test_check_source_connection_failure():
    workspace_id = "my_workspace_id"
    data = AirbyteSourceCreate(
        name="my_source_name",
        sourceDefId="my_sourcedef_id",
        config={"key": "value"},
    )
    expected_response_srcdef = {"connectionSpecification": {"properties": {}}}
    failed_response = {
        "status": "failed",
        "message": "Credentials are invalid",
    }

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq_:
        mock_abreq_.side_effect = [expected_response_srcdef, failed_response]

        with pytest.raises(HttpError) as excinfo:
            check_source_connection(workspace_id, data)

        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "Credentials are invalid"


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


def test_check_source_connection_for_update_success():
    source_id = "my_source_id"
    data = AirbyteSourceUpdateCheckConnection(
        name="my_source_name",
        config={"key": "value"},
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.status_code = 200
        mock_response.json.return_value = {"mykey": "myval", "jobInfo": {}}
        mock_post.return_value = mock_response
        result = check_source_connection_for_update(source_id, data)
        assert result["mykey"] == "myval"


def test_check_source_connection_for_update_failure():
    source_id = "my_source_id"
    data = AirbyteSourceUpdateCheckConnection(
        name="my_source_name",
        config={"key": "value"},
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.status_code = 500
        mock_response.json.return_value = {
            "error": "failed to check source connection",
            "status": "failed",
        }
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            result = check_source_connection_for_update(source_id, data)
            assert result is None
            assert excinfo.value.status_code == 500
            assert str(excinfo.value) == "failed to check source connection"


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


def test_get_source_schema_catalog_failure_1():
    workspace_id = "my_workspace_id"
    source_id = "my_source_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "error": "failed to get source schema catalogs",
            "message": "error-message",
        }
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "error-message"


def test_get_source_schema_catalog_failure_2():
    workspace_id = "my_workspace_id"
    source_id = "my_source_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "error": "failed to get source schema catalogs",
            "message": "error-message",
            "jobInfo": {"failureReason": {"externalMessage": "external-message"}},
        }
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "external-message"


def test_get_source_schema_catalog_failure_3():
    workspace_id = "my_workspace_id"
    source_id = "my_source_id"
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "error": "failed to get source schema catalogs",
            "message": "error-message",
            "jobInfo": {},
        }
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_source_schema_catalog(workspace_id, source_id)
        assert excinfo.value.status_code == 400
        assert str(excinfo.value) == "Failed to discover schema"


def test_get_destination_definitions_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinationDefinitions": "theDestinationDefinitions"}
        mock_post.return_value = mock_response

        response = get_destination_definitions("workspace-id")

        assert response["destinationDefinitions"] == "theDestinationDefinitions"


def test_get_destination_definitions_failure_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_destination_definitions(None)
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_destination_definitions_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"not-the-right-key": ""}
        mock_post.return_value = mock_response

        with pytest.raises(HttpError) as excinfo:
            get_destination_definitions("workspace-id")

        assert str(excinfo.value) == "destination definitions not found"


def test_get_destination_definition():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinationDefinitionId": "theDestinationDefId"}
        mock_post.return_value = mock_response

        response = get_destination_definition("workspace-id", "destination_def_id")

        assert response["destinationDefinitionId"] == "theDestinationDefId"


def test_get_destination_definition_specification_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "connectionSpecification": {
                "title": "theTitle",
            }
        }
        mock_post.return_value = mock_response

        response = get_destination_definition_specification("workspace-id", "destinationdef_id")

        assert response["connectionSpecification"] == {
            "title": "theTitle",
        }


def test_get_destination_definition_specification_success_postgres():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "connectionSpecification": {
                "title": "Postgres Destination Spec",
                "properties": {
                    "ssl_mode": {"title": ""},
                    "tunnel_method": {"title": ""},
                },
            }
        }
        mock_post.return_value = mock_response

        response = get_destination_definition_specification("workspace-id", "destinationdef_id")

        assert response["connectionSpecification"] == {
            "title": "Postgres Destination Spec",
            "properties": {
                "ssl_mode": {"title": "SSL modes* (select 'disable' if you don't know)"},
                "tunnel_method": {
                    "title": "SSH Tunnel Method* (select 'No Tunnel' if you don't know)"
                },
            },
        }


def test_get_destination_definition_specification_with_invalid_workspace():
    with pytest.raises(HttpError) as excinfo:
        get_destination_definition_specification(1, "destinationdef_id")
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_job_info():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"job": {"id": "jobid"}},
    ) as mock_abreq_:
        get_job_info("jobid")
        mock_abreq_.assert_called_once_with("jobs/get", {"id": "jobid"})


def test_get_connection_catalog_success():
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {
            "connectionId": "test-connection-id",
            "syncCatalog": {
                "streams": [
                    {"stream": {"name": "test-stream-1"}, "config": {}},
                    {"stream": {"name": "test-stream-2"}, "config": {}},
                ]
            },
        }
        catalog = get_connection_catalog("test-connection-id")
        assert catalog == {
            "connectionId": "test-connection-id",
            "syncCatalog": {
                "streams": [
                    {"stream": {"name": "test-stream-1"}, "config": {}},
                    {"stream": {"name": "test-stream-2"}, "config": {}},
                ]
            },
        }
        mock_abreq.assert_called_once_with(
            "connections/get",
            {"connectionId": "test-connection-id"},
        )


def test_get_connection_no_connection():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"no-connectionId": True},
    ):
        workspace_id = "workspace-id"
        connection_id = "connection-id"
        with pytest.raises(HttpError) as excinfo:
            get_connection(workspace_id, connection_id)
        assert str(excinfo.value) == f"Connection not found: {connection_id}"