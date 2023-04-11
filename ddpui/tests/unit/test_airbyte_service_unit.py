import os
from unittest.mock import patch
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.ddpairbyte.airbyte_service import *


class TestAbreq:

    @patch("requests.post")
    def test_abreq(self, mock_post):
        abhost = os.getenv("AIRBYTE_SERVER_HOST")
        abport = os.getenv("AIRBYTE_SERVER_PORT")
        abver = os.getenv("AIRBYTE_SERVER_APIVER")
        token = os.getenv("AIRBYTE_API_TOKEN")

        mock_post.return_value.json.return_value = {"key": "value"}
        res = abreq("source_definitions/list_for_workspace", {"test": "data"})

        assert res == {"key": "value"}
        mock_post.assert_called_with(
            f"http://{abhost}:{abport}/api/{abver}/source_definitions/list_for_workspace",
            headers={"Authorization": f"Basic {token}"},
            json={"test": "data"},
        )

    @patch("requests.post")
    def test_abreq_failure(self, mock_post):
        mock_post.side_effect = Exception("error")

        # test the abreq function for failure
        with pytest.raises(Exception) as excinfo:
            abreq("test_endpoint", {"test_key": "test_value"})
        assert str(excinfo.value) == "error"


class TestWorkspace:
    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspaces_success(self, mock_abreq):
        mock_abreq.return_value = [{"id": "workspace-id"}, {"id": "workspace2"}]
        workspaces = get_workspaces()
        assert workspaces == [{"id": "workspace-id"}, {"id": "workspace2"}]

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspaces_failure(self, mock_abreq):
        mock_abreq.side_effect = Exception("Failed to get workspaces")
        with pytest.raises(Exception) as excinfo:
            get_workspaces()
        assert str(excinfo.value) == "Failed to get workspaces"

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspace_success(self, mock_abreq):
        mock_abreq.return_value = {"workspace": {"id": "workspace-id"}}
        workspaces = get_workspace("workspace-id")
        assert workspaces == {"workspace": {"id": "workspace-id"}}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspace_failure(self, mock_abreq):
        mock_abreq.side_effect = Exception("Failed to get workspace")
        with pytest.raises(Exception) as excinfo:
            get_workspace("workspace-id")
        assert str(excinfo.value) == "Failed to get workspace"

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_set_workspace_name(self, mock_abreq):
        mock_abreq.return_value = {"workspace": {"id": "workspace-id"}}
        set_workspace_name("workspace-id", "ngo1")
        mock_abreq.assert_called_with(
            "workspaces/update_name",
            {"workspaceId": "workspace-id", "name": "ngo1"}
        )

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_create_workspace(self, mock_abreq):
        mock_res = {"workspaceId": "new-workspace-id"}
        mock_abreq.return_value = mock_res
        res = create_workspace("new-workspace-name")
        assert res == mock_res


class TestAirbyteSource:

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_definitions_success(self, mock_abreq):
        mock_abreq.return_value = {"sourceDefinitions": [{"id": "ngo1"}, {"id": "source2"}]}

        res = get_source_definitions("workspace_id")
        assert res == [{"id": "ngo1"}, {"id": "source2"}]

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_definitions_error(self, mock_abreq):
        mock_abreq.return_value = {"error": "Test error"}

        try:
            get_source_definitions("workspace1")
            assert False, "Expected an exception to be raised"
        except Exception as e:
            assert str(e) == str({"error": "Test error"})

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_definition_specification(self, mock_abreq):
        mock_res = {"connectionSpecification": {"key": "value"}}
        mock_abreq.return_value = mock_res

        res = get_source_definition_specification("ngo1", "source_def_id")

        mock_abreq.assert_called_once_with(
            "source_definition_specifications/get",
            {"sourceDefinitionId": "source_def_id", "workspaceId": "ngo1"},
        )
        assert res == {"key": "value"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_definition_specification_error(self, mock_abreq):
        mock_res = {"status": "error"}
        mock_abreq.return_value = mock_res

        with pytest.raises(Exception):
            get_source_definition_specification("workspace-id", "sourcedef-id")

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_sources_success(self, mock_abreq):
        mock_res = {"sources": [{"sourceId": "test_source", "sourceName": "test_source_name"}]}
        mock_abreq.return_value = mock_res
        res = get_sources("workspace_id")
        assert res == [{"sourceId": "test_source", "sourceName": "test_source_name"}]

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_success(self, mock_abreq):
        mock_res = {"sourceId": "test_source", "sourceName": "source1"}
        mock_abreq.return_value = mock_res
        res = get_source("bc632205-5bd4-4f44-a123-12df60554aa5", "test_source")
        assert res == {"sourceId": "test_source", "sourceName": "source1"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_failure(self, mock_abreq):
        mock_res = {"error": "Test error"}
        mock_abreq.return_value = mock_res

        with pytest.raises(Exception) as e:
            get_source("workspace_id", "test_source")
        assert str(e.value) == str({"error": "Test error"})

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_create_source_success(self, mock_abreq):
        mock_res = {"sourceId": "source_id", "sourceName": "source1"}
        mock_abreq.return_value = mock_res

        res = create_source("workspace_id", "source1", "sourcedef_id", {"key": "value"})
        assert res == {"sourceId": "source_id", "sourceName": "source1"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_create_source_failure(self, mock_abreq):
        mock_abreq.return_value = {"error": "error message"}

        with pytest.raises(Exception) as e:
            create_source("workspace_id", "source_name", "sourcedef_id", {"key": "value"})

        assert str(e.value) == 'Failed to create source'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_schema_catalog_success(self, mock_abreq):
        mock_res = {'catalog': 'sample_catalog'}
        mock_abreq.return_value = mock_res
        res = get_source_schema_catalog('sample_workspace_id', 'sample_source_id')
        assert res['catalog'] == 'sample_catalog'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_source_schema_catalog_failure(self, mock_abreq):
        mock_res = {'error': 'error message'}
        mock_abreq.return_value = mock_res
        with pytest.raises(Exception) as excinfo:
            get_source_schema_catalog('sample_workspace_id', 'sample_source_id')
        assert str(excinfo.value) == 'Failed to get source schema catalogs'


# class TestAirbyteConnection:

#     def test_check_source_connection_success(self):
#         workspace_id = "279d9a88-77fa-4246-be58-064e4cf5c281"
#         source_id = "f8ea7db2-a821-4e7b-b563-f083a75ad79e"

#         response = check_source_connection(workspace_id, source_id)
#         assert response['status'] == 'succeeded'

#     def test_check_source_connection_failure(self):
#         workspace_id = "bc632205-5bd4-4f44-a123-12df60554aa5"
#         source_id = "6d085a9c-18b6-4a8a-ae3e-3ca7f8a968b1"

#         response = check_source_connection(workspace_id, source_id)
#         assert response['status'] == 'failed'


class TestAirbyteDestination:

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destination_definitions_success(self, mock_abreq):
        mock_abreq.return_value = {"destinationDefinitions": "sample_destination_definitions"}
        res = get_destination_definitions('sample_workspace_id')
        assert res == "sample_destination_definitions"

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destination_definitions_failure(self, mock_abreq):
        mock_abreq.return_value = {'error': 'error message'}
        with pytest.raises(Exception) as excinfo:
            get_destination_definitions('sample_workspace_id')
        assert str(excinfo.value) == 'Failed to get destination definitions'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destination_definition_specification_success(self, mock_abreq):
        mock_abreq.return_value = {"connectionSpecification": {"key": "value"}}
        res = get_destination_definition_specification('workspace1', 'destination_defi_id')
        assert res == {"key": "value"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destination_definition_specification_failure(self, mock_abreq):
        mock_abreq.return_value = {'error': 'error message'}
        with pytest.raises(Exception) as excinfo:
            get_destination_definition_specification('sample_workspace_id', 'sample_destinationdef_id')
        assert str(excinfo.value) == 'Failed to get destination definition specification'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destinations_success(self, mock_abreq):
        mock_abreq.return_value={"destinations": "sample_destinations"}
        res = get_destinations('sample_workspace_id')
        assert res == "sample_destinations"

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_destinations_failure(self, mock_abreq):
        mock_abreq.return_value={'error': 'error message'}
        with pytest.raises(Exception) as excinfo:
            get_destinations('sample_workspace_id')
        assert str(excinfo.value) == 'Failed to get destinations'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_create_destination_success(self, mock_abreq):
        mock_abreq.return_value = {'destinationId': 'test_id'}
        config = {'key': 'value'}
        res = create_destination('test_workspace', 'test_name', 'test_destdef', config)
        assert res['destinationId'] == 'test_id'

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_create_destination_failure(self, mock_abreq):
        mock_abreq.return_value = {}
        config = {"key": "value"}

        with pytest.raises(Exception):
                create_destination('test_workspace', 'test_name', 'test_destdef', config)

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_check_destination_connection(self, mock_abreq):
        mock_abreq.return_value = {"status": "success"}
        workspace_id = "test_workspace_id"
        destination_id = "test_destination_id"

        res = check_destination_connection(workspace_id, destination_id)

        assert res == {"status": "success"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_connections_success(self, mock_abreq):
        mock_abreq.return_value = {"connections": [{"id": "test_id"}]}
        workspace_id = "test_workspace_id"

        res = get_connections(workspace_id)

        assert res == [{"id": "test_id"}]

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_connections_failure(self, mock_abreq):
        mock_abreq.return_value = {}
        workspace_id = "test_workspace_id"

        with pytest.raises(Exception):
            get_connections(workspace_id)

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_connection_success(self, mock_abreq):
        mock_abreq.return_value = {"connectionId": "test_id"}
        workspace_id = "test_workspace_id"
        connection_id = "test_connection_id"

        res = get_connection(workspace_id, connection_id)

        assert res == {"connectionId": "test_id"}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_connection_failure(self, mock_abreq):
        mock_abreq.return_value = {}
        workspace_id = "test_workspace_id"
        connection_id = "test_connection_id"

        with pytest.raises(Exception):
                get_connection(workspace_id, connection_id)

    def test_create_connection_success(self):
        workspace_id = "bc632205-5bd4-4f44-a123-12df60554aa5"
        connection_info = schema.AirbyteConnectionCreate(
            source_id="6d085a9c-18b6-4a8a-ae3e-3ca7f8a968b1",
            destination_id="b241b58f-aa3e-4220-a4d9-0f4ae26e99e5",
            streamnames=["Streamr", "stream2"],
            name="test_connection"
        )
        result = create_connection(workspace_id, connection_info)
        assert "connectionId" in result

    def test_create_connection_failure(self):
        workspace_id = "your_workspace_id"
        connection_info = schema.AirbyteConnectionCreate(
            source_id="your_source_id",
            destination_id="your_destination_id",
            streamnames=[],
            name="test_connection"
        )
        with pytest.raises(Exception) as excinfo:
            create_connection(workspace_id, connection_info)
        assert "must specify stream names" in str(excinfo.value)


        