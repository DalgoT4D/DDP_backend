import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.tests.unit_integration_tests.test_airbyte_unit_schemas import *
from ddpui.ddpairbyte.airbyte_service import *
from pydantic import ValidationError


class TestWorkspace:
    workspace_id = None

    def test_a_create_workspace(self):
        payload = {"name": "test_workspace"}

        try:
            CreateWorkspaceTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in the payload: {e.errors()}")

        try:
            res = create_workspace(**payload)
            CreateWorkspaceTestResponse(**res)
            TestWorkspace.workspace_id = res["workspaceId"]
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_workspace(self):
        try:
            res = get_workspace(workspace_id=TestWorkspace.workspace_id)
            GetWorkspaceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_workspaces(self):
        try:
            res = get_workspaces()
            GetWorkspacesTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_set_workspace_name(self):
        new_name = "test"

        try:
            res = set_workspace_name(
                workspace_id=TestWorkspace.workspace_id, name=new_name
            )
            SetWorkspaceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


@pytest.fixture(scope="session")
def test_workspace_id():
    test_workspace = TestWorkspace()
    test_workspace.test_a_create_workspace()
    return test_workspace.workspace_id


class TestAirbyteSource:
    def test_a_create_source(self, test_workspace_id):
        source_definitions = get_source_definitions(workspace_id=test_workspace_id)
        for source_definition in source_definitions:
            if source_definition["name"] == "File (CSV, JSON, Excel, Feather, Parquet)":
                source_definition_id = source_definition["sourceDefinitionId"]
                break

        payload = {
            "sourcedef_id": source_definition_id,
            "config": {
                "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
                "format": "csv",
                "provider": {"storage": "HTTPS"},
                "dataset_name": "covid19data",
            },
            "workspace_id": str(test_workspace_id),
            "name": "source1",
        }
        try:
            CreateSourceTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")
        try:
            res = create_source(**payload)
            CreateSourceTestResponse(**res)
            TestAirbyteSource.source_id = res["sourceId"]
            TestAirbyteSource.source_definition_id = res["sourceDefinitionId"]
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_definitions(self, test_workspace_id):
        try:
            res = get_source_definitions(workspace_id=test_workspace_id)
            GetSourceDefinitionsTestResponse(__root__=res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_source(self, test_workspace_id):
        try:
            res = get_source(
                workspace_id=test_workspace_id,
                source_id=TestAirbyteSource.source_id,
            )
            GetSourceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_sources(self, test_workspace_id):
        try:
            res = get_sources(workspace_id=test_workspace_id)
            GetSourcesTestResponse(sources=res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_update_source(self):
        payload = {
            "name": "source9",
            "sourcedef_id": TestAirbyteSource.source_definition_id,
            "source_id": TestAirbyteSource.source_id,
            "config": {
                "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
                "format": "csv",
                "provider": {"storage": "HTTPS"},
                "dataset_name": "covid19data",
            },
        }
        try:
            UpdateSourceTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")

        try:
            res = update_source(**payload)
            UpdateSourceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_check_source_connection(self, test_workspace_id):
        workspace_id = test_workspace_id
        source_id = TestAirbyteSource.source_id

        try:
            res = check_source_connection(workspace_id, source_id)
            CheckSourceConnectionTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


@pytest.fixture(scope="session")
def test_source_id(test_workspace_id):
    test_source = TestAirbyteSource()
    test_source.test_a_create_source(test_workspace_id)
    return test_source.source_id


class TestAirbyteDestination:
    def test_a_create_destination(self, test_workspace_id):
        destination_definitions = get_destination_definitions(
            workspace_id=test_workspace_id
        )
        for destination_definition in destination_definitions:
            if destination_definition["name"] == "Postgres":
                destination_definition_id = destination_definition[
                    "destinationDefinitionId"
                ]
                break

        payload = {
            "name": "destination1",
            "destinationdef_id": destination_definition_id,
            "config": {
                "host": "dev-test.c4hvhyuxrcet.ap-south-1.rds.amazonaws.com",
                "port": 5432,
                "database": "ddpabhis",
                "username": "abhis",
                "schema": "staging",
            },
            "workspace_id": str(test_workspace_id),
        }
        try:
            CreateDestinationTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")

        try:
            res = create_destination(**payload)
            CreateDestinationTestResponse(**res)
            TestAirbyteDestination.destination_id = res["destinationId"]
            TestAirbyteDestination.destination_definition_id = res[
                "destinationDefinitionId"
            ]
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_destination(self, test_workspace_id):
        try:
            res = get_destination(
                workspace_id=test_workspace_id,
                destination_id=TestAirbyteDestination.destination_id,
            )
            GetDestinationTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_update_destination(self):
        payload = {
            "name": "test_update",
            "destination_id": TestAirbyteDestination.destination_id,
            "config": {
                "host": "dev-test.c4hvhyuxrcet.ap-south-1.rds.amazonaws.com",
                "port": 5432,
                "database": "ddpabhis",
                "username": "abhis",
                "schema": "staging",
            },
            "destinationdef_id": TestAirbyteDestination.destination_definition_id,
        }

        try:
            UpdateDestinationTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")

        try:
            res = update_destination(**payload)
            UpdateDestinationTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Field do not match in resposne: {e.errors()}")

    def test_check_destination_connection(self, test_workspace_id):
        workspace_id = test_workspace_id
        destination_id = TestAirbyteDestination.destination_id

        try:
            res = check_destination_connection(workspace_id, destination_id)
            CheckDestinationConnectionTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


@pytest.fixture(scope="session")
def test_destination_id(test_workspace_id):
    test_destination = TestAirbyteDestination()
    test_destination.test_a_create_destination(test_workspace_id)
    return test_destination.destination_id


class TestConnection:
    def test_a_create_connection(
        self, test_workspace_id, test_source_id, test_destination_id
    ):
        workspace_id = str(test_workspace_id)
        connection_info = schema.AirbyteConnectionCreate(
            sourceId=str(test_source_id),
            destinationId=str(test_destination_id),
            name="Test Connection",
            streamNames=["companies"],
        )

        try:
            res = create_connection(workspace_id, connection_info)
            CreateConnectionTestResponse(**res)
            TestConnection.connection_id = res["connectionId"]
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_connection(self, test_workspace_id):
        workspace_id = test_workspace_id
        connection_id = TestConnection.connection_id

        try:
            res = get_connection(workspace_id, connection_id)
            GetConnectionTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_update_connection(
        self, test_workspace_id, test_source_id, test_destination_id
    ):
        workspace_id = test_workspace_id
        connection_id = (TestConnection.connection_id,)
        connection_info = schema.AirbyteConnectionUpdate(
            sourceId=test_source_id,
            destinationId=test_destination_id,
            connectionId=TestConnection.connection_id,
            streamNames=["companies"],
            name="Test Connection",
        )

        try:
            res = update_connection(workspace_id, connection_id, connection_info)
            UpdateConnectionTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")
