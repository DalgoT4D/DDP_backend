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
        "jobInfo": {
            "succeeded": False,
        },
        "message": "Credentials are invalid",
    }

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq_:
        mock_abreq_.side_effect = [expected_response_srcdef, failed_response]

        with pytest.raises(HttpError) as excinfo:
            check_source_connection(workspace_id, data)

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


def test_get_destination_definition_specification_with_invalid_destinationdef():
    with pytest.raises(HttpError) as excinfo:
        get_destination_definition_specification("workspace_id", 1)
    assert str(excinfo.value) == "destinationdef_id must be a string"


def test_get_destination_definition_specification_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"wrong-key": "theConnectionSpecification"}
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_destination_definition_specification("workspace-id", "destinationdef_id")

        assert str(excinfo.value) == "Failed to get destination definition specification"


def test_get_destinations_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinations": "the-destinations"}
        mock_post.return_value = mock_response

        response = get_destinations("workspace-id")

        assert response["destinations"] == "the-destinations"


def test_get_destinations_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_destinations(1)
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_destinations_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"wrong-key": "theConnectionSpecification"}
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_destinations("workspace-id")

        assert str(excinfo.value) == "destinations not found for this workspace"


def test_get_destination_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinationId": "the-destination"}
        mock_post.return_value = mock_response

        response = get_destination("workspace-id", "destination_id")

        assert response["destinationId"] == "the-destination"


def test_get_destination_failure_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_destination(1, "destination_id")
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_destination_failure_with_invalid_destination_id():
    with pytest.raises(HttpError) as excinfo:
        get_destination("workspace_id", 2)
    assert str(excinfo.value) == "destination_id must be a string"


def test_get_destination_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"wrong-key": "theConnectionSpecification"}
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            get_destination("workspace-id", "destination_id")

        assert str(excinfo.value) == "destination not found"


def test_create_destination_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinationId": "the-destination"}
        mock_post.return_value = mock_response

        response = create_destination("workspace-id", "name", "destinationdef_id", {})

        assert response["destinationId"] == "the-destination"


def test_create_destination_with_invalid_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        create_destination(1, "name", "destinationdef_id", {})
    assert str(excinfo.value) == "workspace_id must be a string"


def test_create_destination_failure_with_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        create_destination("workspace_id", 1, "destinationdef_id", {})
    assert str(excinfo.value) == "name must be a string"


def test_create_destination_failure_with_invalid_destinationdef_id():
    with pytest.raises(HttpError) as excinfo:
        create_destination("workspace_id", "name", 1, {})
    assert str(excinfo.value) == "destinationdef_id must be a string"


def test_create_destination_failure_with_invalid_config():
    with pytest.raises(HttpError) as excinfo:
        create_destination("workspace_id", "name", "destinationdef_id", 1)
    assert str(excinfo.value) == "config must be a dict"


def test_create_destination_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"wrong-key": "theConnectionSpecification"}
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            create_destination("workspace-id", "name", "destinationdef_id", {})

        assert str(excinfo.value) == "failed to create destination"


def test_update_destination_success():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"destinationId": "the-destination"}
        mock_post.return_value = mock_response

        response = update_destination("destination_id", "name", {}, "destinationdef_id")

        assert response["destinationId"] == "the-destination"


def test_update_destination_failure_with_invalid_destination_id():
    with pytest.raises(HttpError) as excinfo:
        update_destination(1, "name", {}, "destinationdef_id")
    assert str(excinfo.value) == "destination_id must be a string"


def test_update_destination_failure_with_invalid_name():
    with pytest.raises(HttpError) as excinfo:
        update_destination("destination_id", 1, {}, "destinationdef_id")
    assert str(excinfo.value) == "name must be a string"


def test_update_destination_failure_with_invalid_config():
    with pytest.raises(HttpError) as excinfo:
        update_destination("destination_id", "name", 1, "destinationdef_id")
    assert str(excinfo.value) == "config must be a dict"


def test_update_destination_failure_with_invalid_destinationdef_id():
    with pytest.raises(HttpError) as excinfo:
        update_destination("destination_id", "name", {}, 1)
    assert str(excinfo.value) == "destinationdef_id must be a string"


def test_update_destination_failure():
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"wrong-key": "theConnectionSpecification"}
        mock_post.return_value = mock_response
        with pytest.raises(HttpError) as excinfo:
            update_destination("destination_id", "name", {}, "destinationdef_id")

        assert str(excinfo.value) == "failed to update destination"


def test_check_destination_connection_success():
    payload = AirbyteDestinationCreate(
        name="destinationname", destinationDefId="destinationdef-id", config={}
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"jobInfo": {}, "status": "succeeded"}
        mock_post.return_value = mock_response
        response = check_destination_connection("workspace_id", payload)

        assert response["status"] == "succeeded"


def test_check_destination_connection_with_invalid_workspace_id():
    payload = AirbyteDestinationCreate(
        name="destinationname", destinationDefId="destinationdef-id", config={}
    )
    with pytest.raises(HttpError) as excinfo:
        check_destination_connection(1, payload)
    assert str(excinfo.value) == "workspace_id must be a string"


def test_check_destination_connection_failure_1():
    payload = AirbyteDestinationCreate(
        name="destinationname", destinationDefId="destinationdef-id", config={}
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "status": "failed",
            "message": "Credentials are invalid",
            "jobInfo": {
                "succeeded": False,
            },
        }
        mock_post.return_value = mock_response

        with pytest.raises(HttpError) as excinfo:
            check_destination_connection("workspace_id", payload)

        assert str(excinfo.value) == "Credentials are invalid"


def test_check_destination_connection_failure_2():
    payload = AirbyteDestinationCreate(
        name="destinationname", destinationDefId="destinationdef-id", config={}
    )
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "status": "failed",
            "message": "Credentials are invalid",
            "jobInfo": {
                "succeeded": False,
            },
        }
        mock_post.return_value = mock_response

        with pytest.raises(HttpError) as excinfo:
            check_destination_connection("workspace_id", payload)

        assert str(excinfo.value) == "Credentials are invalid"


def test_check_destination_connection_for_update_success():
    payload = AirbyteDestinationUpdateCheckConnection(name="destinationname", config={})
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"jobInfo": {}, "status": "succeeded"}
        mock_post.return_value = mock_response
        response = check_destination_connection_for_update("destination_id", payload)

        assert response["status"] == "succeeded"


def test_check_destination_connection_for_update_with_invalid_destination_id():
    payload = AirbyteDestinationUpdateCheckConnection(name="destinationname", config={})
    with pytest.raises(HttpError) as excinfo:
        check_destination_connection_for_update(1, payload)
    assert str(excinfo.value) == "destination_id must be a string"


def test_check_destination_connection_for_update_failure_1():
    payload = AirbyteDestinationUpdateCheckConnection(name="destinationname", config={})
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "status": "failed",
            "message": "Credentials are invalid",
            "jobInfo": {
                "succeeded": False,
            },
        }
        mock_post.return_value = mock_response

        with pytest.raises(HttpError) as excinfo:
            check_destination_connection_for_update("destination_id", payload)

        assert str(excinfo.value) == "Credentials are invalid"


def test_check_destination_connection_for_update_failure_2():
    payload = AirbyteDestinationUpdateCheckConnection(name="destinationname", config={})
    with patch("ddpui.ddpairbyte.airbyte_service.requests.post") as mock_post:
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {
            "status": "failed",
            "message": "Credentials are invalid",
            "jobInfo": {
                "succeeded": False,
            },
        }
        mock_post.return_value = mock_response

        with pytest.raises(HttpError) as excinfo:
            check_destination_connection_for_update("destination_id", payload)

        assert str(excinfo.value) == "Credentials are invalid"


def test_get_connections_bad_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_connections(1)
    assert str(excinfo.value) == "workspace_id must be a string"


def test_get_connections_no_connections():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"no-connections": True},
    ):
        workspace_id = "workspace-id"
        with pytest.raises(HttpError) as excinfo:
            get_connections(workspace_id)
        assert str(excinfo.value) == f"connections not found for workspace: {workspace_id}"


def test_get_connections_success():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connections": "the-connections"},
    ):
        workspace_id = "workspace-id"
        result = get_connections(workspace_id)
        assert result["connections"] == "the-connections"


def test_get_connection_bad_workspace_id():
    with pytest.raises(HttpError) as excinfo:
        get_connection(1, "connection-id")
    assert str(excinfo.value) == "workspace_id must be a string"


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


def test_get_connection_success():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "the-connection-id"},
    ):
        workspace_id = "workspace-id"
        result = get_connection(workspace_id, "connection-id")
        assert result["connectionId"] == "the-connection-id"


def test_update_connection_bad_workspace_id():
    conninfo = schema.AirbyteConnectionUpdate(name="connection-name", streams=[])
    with pytest.raises(HttpError) as excinfo:
        update_connection(1, conninfo, {})
    assert str(excinfo.value) == "workspace_id must be a string"


def test_update_connection_no_streams():
    conninfo = schema.AirbyteConnectionUpdate(name="connection-name", streams=[])
    workspace_id = "workspace-id"
    with pytest.raises(HttpError) as excinfo:
        update_connection(workspace_id, conninfo, {})
    assert str(excinfo.value) == f"must specify at least one stream workspace_id={workspace_id}"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_schema_catalog=Mock(
        return_value={
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "stream-1-name",
                        },
                        "config": {},
                    }
                ]
            }
        }
    ),
)
def test_update_connection_failed_to_update():
    connection_info = schema.AirbyteConnectionUpdate(
        name="connection-name",
        streams=[
            {
                "name": "stream-1-name",
                "selected": True,
                "syncMode": "sync-mode",
                "destinationSyncMode": "destination-sync-mode",
            }
        ],
        destinationSchema=None,
    )
    workspace_id = "workspace-id"
    with patch("ddpui.ddpairbyte.airbyte_service.abreq", return_value={"no-connectionId": True}):
        with pytest.raises(HttpError) as excinfo:
            update_connection(
                workspace_id,
                connection_info,
                {"sourceId": "source-id", "syncCatalog": {"streams": []}},
            )
        assert str(excinfo.value) == "failed to update connection"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_source_schema_catalog=Mock(
        return_value={
            "catalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "stream-1-name",
                        },
                        "config": {},
                    }
                ]
            }
        }
    ),
)
def test_update_connection_success():
    connection_info = schema.AirbyteConnectionUpdate(
        name="connection-name",
        streams=[
            {
                "name": "stream-1-name",
                "selected": True,
                "syncMode": "sync-mode",
                "destinationSyncMode": "destination-sync-mode",
            }
        ],
        destinationSchema=None,
    )
    workspace_id = "workspace-id"
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ):
        res = update_connection(
            workspace_id,
            connection_info,
            {"sourceId": "source-id", "syncCatalog": {"streams": []}},
        )
        assert res["connectionId"] == "connection-id"


def test_reset_connection():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        reset_connection("connection-id")
        mock_abreq_.assert_called_once_with("connections/reset", {"connectionId": "connection-id"})


def test_delete_connection():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        delete_connection("wsid", "connection-id")
        mock_abreq_.assert_called_once_with("connections/delete", {"connectionId": "connection-id"})


def test_sync_connection():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        sync_connection("wsid", "connection-id")
        mock_abreq_.assert_called_once_with("connections/sync", {"connectionId": "connection-id"})


def test_get_job_info():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        get_job_info("jobid")
        mock_abreq_.assert_called_once_with("jobs/get_debug_info", {"id": "jobid"})


def test_get_job_without_logs():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        get_job_info_without_logs("jobid")
        mock_abreq_.assert_called_once_with("jobs/get_without_logs", {"id": "jobid"})


def test_get_jobs_for_connection():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "connection-id"},
    ) as mock_abreq_:
        get_jobs_for_connection("connection_id")
        mock_abreq_.assert_called_once_with(
            "jobs/list",
            {
                "configTypes": ["sync"],
                "configId": "connection_id",
                "pagination": {"rowOffset": 0, "pageSize": 1},
            },
        )


def test_get_jobs_for_connection_raise():
    with pytest.raises(HttpError) as excinfo:
        get_jobs_for_connection(1)
    assert str(excinfo.value) == "connection_id must be a string"


def test_parse_job_info():
    ret = parse_job_info(
        {
            "job": {"configType": "sync", "id": "job-id", "status": "job-status"},
            "attempts": [
                {"id": 1, "status": "failed", "recordsSynced": 0},
                {
                    "id": 2,
                    "status": "succeeded",
                    "createdAt": 300,
                    "endedAt": 400,
                    "recordsSynced": 10,
                    "bytesSynced": 1000,
                    "recordsEmitted": 400,
                    "totalStats": {
                        "bytesEmitted": 400,
                        "recordsCommitted": 500,
                        "recordsEmitted": 500,
                    },
                },
                {"id": 3, "status": "succeeded", "recordsSynced": 20},
            ],
        }
    )
    assert ret["job_id"] == "job-id"
    assert ret["status"] == "job-status"
    assert ret["recordsSynced"] == 10


def test_get_logs_for_job():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"logs": {"logLines": ["log-line-1", "log-line-2"]}},
    ) as mock_abreq_:
        get_logs_for_job(1)
        mock_abreq_.assert_called_once_with("attempt/get_for_job", {"jobId": 1, "attemptNumber": 0})


def test_get_logs_for_job_1():
    with patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"logs": {"logLines": ["log-line-1", "log-line-2"]}},
    ) as mock_abreq_:
        get_logs_for_job(1, 1)
        mock_abreq_.assert_called_once_with("attempt/get_for_job", {"jobId": 1, "attemptNumber": 1})


def test_get_logs_for_job_raise():
    with pytest.raises(HttpError) as excinfo:
        get_logs_for_job("str")
    assert str(excinfo.value) == "job_id must be an integer"


def test_get_logs_for_job_success():
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {"logs": {"logLines": ["log-line-1", "log-line-2"]}}
        result = get_logs_for_job(1, 0)
        assert result == ["log-line-1", "log-line-2"]


def test_get_logs_for_job_with_logType_formatted():
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {
            "logs": {"logLines": ["log-line-1", "log-line-2"]},
            "logType": "formatted",
        }
        result = get_logs_for_job(1, 0)
        assert result == ["log-line-1", "log-line-2"]


def test_get_logs_for_job_with_logType_structured():
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {
            "logs": {
                "logLines": ["log-line-1", "log-line-2"],
                "events": [{"message": "log-line-3"}, {"message": "log-line-4"}],
            },
            "logType": "structured",
        }
        result = get_logs_for_job(1, 0)
        assert result == ["log-line-3", "log-line-4"]


def test_get_connection_catalog_success():
    org = Mock()
    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
        mock_abreq.return_value = {
            "connectionId": "test-connection-id",
            "catalog": {
                "streams": [
                    {"stream": {"name": "test-stream-1"}, "config": {}},
                    {"stream": {"name": "test-stream-2"}, "config": {}},
                ]
            },
        }
        catalog = get_connection_catalog("test-connection-id")
        assert catalog == {
            "connectionId": "test-connection-id",
            "catalog": {
                "streams": [
                    {"stream": {"name": "test-stream-1"}, "config": {}},
                    {"stream": {"name": "test-stream-2"}, "config": {}},
                ]
            },
        }
        mock_abreq.assert_called_once_with(
            "web_backend/connections/get",
            {"connectionId": "test-connection-id", "withRefreshedCatalog": True},
        )


def test_get_connection_catalog_invalid_connection_id():
    org = Mock()
    with pytest.raises(HttpError) as excinfo:
        get_connection_catalog(123)
    assert str(excinfo.value) == "connection_id must be a string"


def test_update_schema_change_valid_input():
    org = Mock()
    connection_info = AirbyteConnectionSchemaUpdate(
        name="test-connection",
        connectionId="connection_id",
        sourceCatalogId="source_id",
        syncCatalog={"streams": []},
    )
    current_connection = {"connectionId": "test-connection-id", "syncCatalog": {}}

    with patch(
        "ddpui.ddpairbyte.airbyte_service.trigger_reset_and_sync_workflow"
    ) as mock_trigger_reset_and_sync_workflow:
        mock_trigger_reset_and_sync_workflow.side_effect = (
            lambda org, connection_id: None
        )  # Mock the function to do nothing

        with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq:
            mock_abreq.return_value = {"connectionId": "test-connection-id"}

            result = update_schema_change(org, connection_info, current_connection)

            assert result["connectionId"] == "test-connection-id"
            mock_abreq.assert_called_once_with("web_backend/connections/update", current_connection)


def test_update_schema_change_invalid_connection_info():
    org = Mock()
    connection_info = {}
    current_connection = {"connectionId": "test-connection-id", "syncCatalog": {}}

    with pytest.raises(HttpError) as excinfo:
        update_schema_change(org, connection_info, current_connection)
    assert (
        str(excinfo.value) == "connection_info must be an instance of AirbyteConnectionSchemaUpdate"
    )


def test_update_schema_change_invalid_current_connection():
    org = Mock()
    connection_info = AirbyteConnectionSchemaUpdate(
        name="test-connection",
        connectionId="connection_id",
        sourceCatalogId="source_id",
        syncCatalog={"streams": []},
    )
    current_connection = "invalid"

    with pytest.raises(HttpError) as excinfo:
        update_schema_change(org, connection_info, current_connection)
    assert str(excinfo.value) == "current_connection must be a dictionary"


def test_update_schema_change_missing_syncCatalog():
    org = Mock()
    connection_info = AirbyteConnectionSchemaUpdate(
        name="test-connection",
        connectionId="connection_id",
        sourceCatalogId="source_id",
        syncCatalog={"streams": []},
    )
    current_connection = {"connectionId": "test-connection-id"}

    with patch("ddpui.ddpairbyte.airbyte_service.abreq") as mock_abreq, patch(
        "ddpui.ddpairbyte.airbyte_service.trigger_reset_and_sync_workflow"
    ) as mock_trigger_reset_and_sync_workflow:
        mock_abreq.return_value = {"connectionId": "test-connection-id"}

        result = update_schema_change(org, connection_info, current_connection)

        assert result["connectionId"] == "test-connection-id"
        assert "syncCatalog" not in result
        mock_abreq.assert_called_once_with("web_backend/connections/update", current_connection)
        mock_trigger_reset_and_sync_workflow.assert_called_once_with(org, "test-connection-id")


def test_create_connection_success():
    """Test successful connection creation"""
    connection_info = schema.AirbyteConnectionCreate(
        sourceId="source-id",
        destinationId="destination-id",
        name="test-connection",
        destinationSchema=None,
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "primaryKey": ["id"],
                "cursorField": ["updated_at"],
            }
        ],
    )
    workspace_id = "workspace-id"

    with patch(
        "my_module.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "stream-1"},
                        "config": {},
                    }
                ]
            },
        },
    ), patch("my_module.abreq", return_value={"connectionId": "connection-id"}):
        res = create_connection(workspace_id, connection_info)
        assert res["connectionId"] == "connection-id"


def test_create_connection_success():
    """Test successful connection creation with required primaryKey and cursorField"""
    connection_info = schema.AirbyteConnectionCreate(
        sourceId="source-id",
        destinationId="destination-id",
        name="test-connection",
        destinationSchema=None,
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "primaryKey": ["id"],
                "cursorField": ["updated_at"],
            }
        ],
    )
    workspace_id = "workspace-id"

    with patch(
        "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "stream-1"},
                        "config": {},
                    }
                ]
            },
        },
    ), patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "test-connection-id"},
    ):
        res = create_connection(workspace_id, connection_info)
        assert res["connectionId"] == "test-connection-id"


def test_create_connection_missing_primary_key():
    """Test that primaryKey is required when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"""
    connection_info = schema.AirbyteConnectionCreate(
        sourceId="source-id",
        destinationId="destination-id",
        name="test-connection",
        destinationSchema=None,
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "cursorField": ["updated_at"],  # primaryKey is missing
            }
        ],
    )
    workspace_id = "workspace-id"

    with patch(
        "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "stream-1"},
                        "config": {},
                    }
                ]
            },
        },
    ):
        with pytest.raises(HttpError) as exc_info:
            create_connection(workspace_id, connection_info)

    assert exc_info.value.status_code == 400
    assert (
        "primaryKey is required for stream 'stream-1' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"
        in str(exc_info.value)
    )


def test_create_connection_missing_cursor_field():
    """Test that cursorField is required when syncMode is 'incremental'"""
    connection_info = schema.AirbyteConnectionCreate(
        sourceId="source-id",
        destinationId="destination-id",
        name="test-connection",
        destinationSchema=None,
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "primaryKey": ["id"],  # cursorField is missing
            }
        ],
    )
    workspace_id = "workspace-id"

    with patch(
        "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {
                "streams": [
                    {
                        "stream": {"name": "stream-1"},
                        "config": {},
                    }
                ]
            },
        },
    ):
        with pytest.raises(HttpError) as exc_info:
            create_connection(workspace_id, connection_info)

    assert exc_info.value.status_code == 400
    assert (
        "cursor is required for stream 'stream-1' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"
        in str(exc_info.value)
    )


def test_update_connection_missing_cursor_field():
    """Test that cursorField is required when syncMode is 'incremental'"""
    connection_info = schema.AirbyteConnectionUpdate(
        name="test-connection",
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "primaryKey": ["id"],  # cursorField is missing
            }
        ],
        destinationSchema=None,
    )
    workspace_id = "workspace-id"
    current_connection = {"sourceId": "source-id", "syncCatalog": {"streams": []}}

    with patch(
        "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {"streams": [{"stream": {"name": "stream-1"}, "config": {}}]},
        },
    ), patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "test-connection-id"},
    ):
        with pytest.raises(HttpError) as exc_info:
            update_connection(workspace_id, connection_info, current_connection)

    assert exc_info.value.status_code == 400
    assert (
        "cursor is required for stream 'stream-1' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"
        in str(exc_info.value)
    )


def test_update_connection_missing_primary_key():
    """Test that primaryKey is required when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"""
    connection_info = schema.AirbyteConnectionUpdate(
        name="test-connection",
        streams=[
            {
                "name": "stream-1",
                "selected": True,
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "cursorField": ["updated_at"],  # primaryKey is missing
            }
        ],
        destinationSchema=None,
    )
    workspace_id = "workspace-id"
    current_connection = {"sourceId": "source-id", "syncCatalog": {"streams": []}}

    with patch(
        "ddpui.ddpairbyte.airbyte_service.get_source_schema_catalog",
        return_value={
            "catalogId": "catalog-id",
            "catalog": {"streams": [{"stream": {"name": "stream-1"}, "config": {}}]},
        },
    ), patch(
        "ddpui.ddpairbyte.airbyte_service.abreq",
        return_value={"connectionId": "test-connection-id"},
    ):
        with pytest.raises(HttpError) as exc_info:
            update_connection(workspace_id, connection_info, current_connection)

    assert exc_info.value.status_code == 400
    assert (
        "primaryKey is required for stream 'stream-1' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'"
        in str(exc_info.value)
    )
