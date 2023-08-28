import os
import django
from pydantic import ValidationError
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.tests.helper.test_airbyte_unit_schemas import *
from ddpui.ddpairbyte.airbyte_service import *


class TestDeleteSource:
    def test_create_workspace(self):  # skipcq: PYL-R0201
        """creates a workspace, checks airbyte response"""
        payload = {"name": "test_workspace"}

        try:
            CreateWorkspaceTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in the payload: {error.errors()}"
            ) from error

        try:
            res = create_workspace(**payload)
            CreateWorkspaceTestResponse(**res)
            TestWorkspace.workspace_id = res["workspaceId"]
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_a_create_source(self, test_workspace_id):  # skipcq: PYL-R0201
        source_definitions = get_source_definitions(workspace_id=test_workspace_id)[
            "sourceDefinitions"
        ]
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


class TestWorkspace:
    """class which holds all the workspace tests"""

    workspace_id = None

    def test_create_workspace(self):  # skipcq: PYL-R0201
        """creates a workspace, checks airbyte response"""
        payload = {"name": "test_workspace"}

        try:
            CreateWorkspaceTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in the payload: {error.errors()}"
            ) from error

        try:
            res = create_workspace(**payload)
            CreateWorkspaceTestResponse(**res)
            TestWorkspace.workspace_id = res["workspaceId"]
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_workspace(self):  # skipcq: PYL-R0201
        """gets a workspace, checks airbyte response"""
        try:
            res = get_workspace(workspace_id=TestWorkspace.workspace_id)
            GetWorkspaceTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_workspaces(self):  # skipcq: PYL-R0201
        """gets all workspaces, checks airbyte response"""
        try:
            res = get_workspaces()
            GetWorkspacesTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_set_workspace_name(self):  # skipcq: PYL-R0201
        """sets workspace name, checks airbyte response"""
        new_name = "test"

        try:
            res = set_workspace_name(
                workspace_id=TestWorkspace.workspace_id, name=new_name
            )
            SetWorkspaceTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error


@pytest.fixture(scope="session")
def test_workspace_id():
    """workspace id of test workspace"""
    test_workspace = TestWorkspace()
    test_workspace.test_create_workspace()
    return test_workspace.workspace_id


class TestAirbyteSource:
    """class which holds all the source tests"""

    source_config = {
        "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
        "format": "csv",
        "provider": {"storage": "HTTPS"},
        "dataset_name": "covid19data",
    }

    def test_source_connection(self, test_workspace_id):
        """tests connectivity to a source"""
        source_definitions = get_source_definitions(workspace_id=test_workspace_id)[
            "sourceDefinitions"
        ]
        for source_definition in source_definitions:
            if source_definition["name"] == "File (CSV, JSON, Excel, Feather, Parquet)":
                TestAirbyteSource.source_definition_id = source_definition[
                    "sourceDefinitionId"
                ]
                break

        try:
            res = check_source_connection(
                test_workspace_id,
                AirbyteSourceCreate(
                    name="unused",
                    sourceDefId=TestAirbyteSource.source_definition_id,
                    config=self.source_config,
                ),
            )
            CheckSourceConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_a_create_source(self, test_workspace_id):  # skipcq: PYL-R0201
        """tests source creation"""
        payload = {
            "sourcedef_id": TestAirbyteSource.source_definition_id,
            "config": TestAirbyteSource.source_config,
            "workspace_id": str(test_workspace_id),
            "name": "source1",
        }
        try:
            CreateSourceTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in payload: {error.errors()}"
            ) from error
        try:
            res = create_source(**payload)
            CreateSourceTestResponse(**res)
            TestAirbyteSource.source_id = res["sourceId"]
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_source_connection_for_update(self):
        """tests connectivity to a source while editing"""
        try:
            res = check_source_connection_for_update(
                TestAirbyteSource.source_id,
                AirbyteSourceUpdateCheckConnection(
                    name="unused",
                    config=self.source_config,
                ),
            )
            CheckSourceConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_definitions(self, test_workspace_id):  # skipcq: PYL-R0201
        """tests retrieval of source definitions"""
        try:
            res = get_source_definitions(workspace_id=test_workspace_id)[
                "sourceDefinitions"
            ]
            GetSourceDefinitionsTestResponse(__root__=res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_source_schema_catalog(self, test_workspace_id):  # skipcq: PYL-R0201
        """fetches the schema catalog for a source"""
        try:
            res = get_source_schema_catalog(
                test_workspace_id, TestAirbyteSource.source_id
            )
            GetSourceSchemaCatalogTestResponse(catalog=res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_fail_to_get_source_schema_catalog(
        self, test_workspace_id
    ):  # skipcq: PYL-R0201
        """fetches the schema catalog for a source"""
        try:
            get_source_schema_catalog(test_workspace_id, "not-a-source-id")
        except HttpError:
            pass

    def test_get_source(self, test_workspace_id):  # skipcq: PYL-R0201
        """tests retrieval of a single source"""
        try:
            res = get_source(
                workspace_id=test_workspace_id,
                source_id=TestAirbyteSource.source_id,
            )
            GetSourceTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_sources(self, test_workspace_id):  # skipcq: PYL-R0201
        """tests retrieval of all sources"""
        try:
            res = get_sources(workspace_id=test_workspace_id)["sources"]
            GetSourcesTestResponse(sources=res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_update_source(self):  # skipcq: PYL-R0201
        """tests updating a source"""
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
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in payload: {error.errors()}"
            ) from error

        try:
            res = update_source(**payload)
            UpdateSourceTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error


@pytest.fixture(scope="session")
def test_source_id(test_workspace_id):
    test_source = TestAirbyteSource()
    test_source.test_a_create_source(test_workspace_id)
    return test_source.source_id


class TestAirbyteDestination:
    destination_config = {
        "host": os.getenv("TESTING_AIRBYTE_DEST_CONFIG_DBHOST"),
        "port": 5432,
        "database": os.getenv("TESTING_AIRBYTE_DEST_CONFIG_DBNAME"),
        "username": os.getenv("TESTING_AIRBYTE_DEST_CONFIG_DBUSER"),
        "password": os.getenv("TESTING_AIRBYTE_DEST_CONFIG_DBPASS"),
        "schema": "staging",
    }

    def test_a_create_destination(self, test_workspace_id):  # skipcq: PYL-R0201
        destination_definitions = get_destination_definitions(
            workspace_id=test_workspace_id
        )["destinationDefinitions"]
        for destination_definition in destination_definitions:
            if destination_definition["name"] == "Postgres":
                destination_definition_id = destination_definition[
                    "destinationDefinitionId"
                ]
                break

        TestAirbyteDestination.destination_definition_id = destination_definition_id
        payload = {
            "name": "destination1",
            "destinationdef_id": TestAirbyteDestination.destination_definition_id,
            "config": TestAirbyteDestination.destination_config,
            "workspace_id": str(test_workspace_id),
        }
        try:
            CreateDestinationTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in payload: {error.errors()}"
            ) from error

        try:
            res = create_destination(**payload)
            CreateDestinationTestResponse(**res)
            TestAirbyteDestination.destination_id = res["destinationId"]
            TestAirbyteDestination.destination_definition_id = res[
                "destinationDefinitionId"
            ]
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_destination(self, test_workspace_id):  # skipcq: PYL-R0201
        try:
            res = get_destination(
                workspace_id=test_workspace_id,
                destination_id=TestAirbyteDestination.destination_id,
            )
            GetDestinationTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_update_destination(self):  # skipcq: PYL-R0201
        payload = {
            "name": "test_update",
            "destination_id": TestAirbyteDestination.destination_id,
            "config": {
                "host": os.getenv("DBHOST"),
                "port": 5432,
                "database": os.getenv("DBNAME"),
                "username": os.getenv("DBUSER"),
                "schema": "staging",
            },
            "destinationdef_id": TestAirbyteDestination.destination_definition_id,
        }

        try:
            UpdateDestinationTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in payload: {error.errors()}"
            ) from error

        try:
            res = update_destination(**payload)
            UpdateDestinationTestResponse(**res)
        except ValidationError as error:
            raise ValueError(
                f"Field do not match in resposne: {error.errors()}"
            ) from error

    def test_check_destination_connection(self, test_workspace_id):  # skipcq: PYL-R0201
        workspace_id = test_workspace_id
        try:
            res = check_destination_connection(
                workspace_id,
                AirbyteDestinationCreate(
                    name="unused",
                    destinationDefId=TestAirbyteDestination.destination_definition_id,
                    config=TestAirbyteDestination.destination_config,
                ),
            )
            CheckDestinationConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_check_destination_connection_for_update(self):  # skipcq: PYL-R0201
        try:
            res = check_destination_connection_for_update(
                TestAirbyteDestination.destination_id,
                AirbyteDestinationUpdateCheckConnection(
                    name="unused",
                    config=TestAirbyteDestination.destination_config,
                ),
            )
            CheckDestinationConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error


@pytest.fixture(scope="session")
def test_destination_id(test_workspace_id):
    test_destination = TestAirbyteDestination()
    test_destination.test_a_create_destination(test_workspace_id)
    return test_destination.destination_id


class TestConnection:
    def test_a_create_connection(
        self,
        test_workspace_id,
        test_source_id,
        test_destination_id,
    ):  # skipcq: PYL-R0201
        workspace_id = str(test_workspace_id)
        connection_info = schema.AirbyteConnectionCreate(
            sourceId=str(test_source_id),
            destinationId=str(test_destination_id),
            name="Test Connection",
            streams=[
                {
                    "name": "covid19data",
                    "selected": True,
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "overwrite",
                }
            ],
            normalize=False,
        )

        test_airbyte_norm_op_id = create_normalization_operation(test_workspace_id)[
            "operationId"
        ]
        try:
            res = create_connection(
                workspace_id, test_airbyte_norm_op_id, connection_info
            )
            CreateConnectionTestResponse(**res)
            TestConnection.connection_id = res["connectionId"]
            # check if the streams have been set in the connection
            conn = get_connection(workspace_id, res["connectionId"])
            assert conn is not None
            assert "syncCatalog" in conn
            assert "streams" in conn["syncCatalog"]
            assert len(conn["syncCatalog"]["streams"]) == len(connection_info.streams)

            for stream in conn["syncCatalog"]["streams"]:
                assert "config" in stream
                assert stream["config"]["selected"] is True

        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_get_connection(self, test_workspace_id):  # skipcq: PYL-R0201
        workspace_id = test_workspace_id
        connection_id = TestConnection.connection_id

        try:
            res = get_connection(workspace_id, connection_id)
            GetConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_update_connection(
        self, test_workspace_id, test_source_id, test_destination_id
    ):  # skipcq: PYL-R0201
        workspace_id = test_workspace_id
        current_connection = get_connection(workspace_id, TestConnection.connection_id)
        connection_info = schema.AirbyteConnectionUpdate(
            sourceId=test_source_id,
            destinationId=test_destination_id,
            connectionId=TestConnection.connection_id,
            streams=[
                {
                    "name": "covid19data",
                    "selected": True,
                    "syncMode": "full_refresh",
                    "destinationSyncMode": "append",
                }
            ],
            name="New Connection Name",
        )

        try:
            res = update_connection(workspace_id, connection_info, current_connection)
            UpdateConnectionTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error

    def test_delete_connection(self, test_workspace_id):  # skipcq: PYL-R0201
        workspace_id = test_workspace_id
        connection_id = TestConnection.connection_id

        try:
            res = delete_connection(workspace_id, connection_id)
            assert res == {}
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}") from error
