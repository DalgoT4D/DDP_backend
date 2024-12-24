import json
import os
import django
from pydantic import ValidationError
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.tests.helper.test_airbyte_unit_schemas import *
from ddpui.ddpairbyte.airbyte_service import *


def load_configurations():
    """Loads all configurations from a single JSON file."""
    config_file_path = os.getenv("INTEGRATION_TESTS_PATH")  # file path defined in env file.
    with open(config_file_path, "r") as config_file:
        return json.load(config_file)


integration_configs = load_configurations()


@pytest.mark.skip(reason="Skipping this test as airbyte integraion needs to be done")
class TestDeleteSource:
    def test_create_workspace(self):  # skipcq: PYL-R0201
        """creates a workspace, checks airbyte response"""
        payload = {"name": "test_workspace"}

        try:
            CreateWorkspaceTestPayload(**payload)
        except ValidationError as error:
            raise ValueError(f"Field do not match in the payload: {error.errors()}") from error

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


@pytest.mark.skip(reason="Skipping this test as airbyte integraion needs to be done")
class TestWorkspace:
    """Class which holds all the workspace tests dynamically for multiple workspaces."""

    def test_create_workspace(self):
        """Creates workspaces dynamically and checks airbyte response."""
        workspace_configs = integration_configs.get("workspaces", [])
        TestWorkspace.created_workspaces = []  # Store created workspaces for later use

        for workspace_config in workspace_configs:
            payload = {"name": workspace_config["name"]}

            try:
                CreateWorkspaceTestPayload(**payload)
                res = create_workspace(**payload)
                CreateWorkspaceTestResponse(**res)
                TestWorkspace.created_workspaces.append(
                    {"workspace_id": res["workspaceId"], "name": workspace_config["name"]}
                )
                print(
                    f"Successfully created workspace: {workspace_config['name']} with ID: {res['workspaceId']}"
                )
            except ValidationError as error:
                raise ValueError(
                    f"Error creating workspace '{workspace_config['name']}': {error.errors()}"
                )

    def test_get_workspace(self):
        """Gets details for all created workspaces."""
        for workspace in TestWorkspace.created_workspaces:
            workspace_id = workspace["workspace_id"]
            try:
                res = get_workspace(workspace_id=workspace_id)
                GetWorkspaceTestResponse(**res)
                print(f"Successfully retrieved details for workspace ID: {workspace_id}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for workspace ID '{workspace_id}': {error.errors()}"
                )

    def test_get_workspaces(self):
        """Gets all workspaces and checks airbyte response."""
        try:
            res = get_workspaces()
            GetWorkspacesTestResponse(**res)
            print("Successfully retrieved all workspaces.")
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}")

    def test_set_workspace_name(self):
        """Sets new names for all created workspaces."""
        for workspace in TestWorkspace.created_workspaces:
            workspace_id = workspace["workspace_id"]
            new_name = f"{workspace['name']}_updated"

            payload = {"workspace_id": workspace_id, "name": new_name}

            try:
                res = set_workspace_name(**payload)
                SetWorkspaceTestResponse(**res)
                print(f"Successfully updated workspace ID: {workspace_id} to new name: {new_name}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for workspace ID '{workspace_id}': {error.errors()}"
                )


@pytest.fixture(scope="session")
def test_workspace_id():
    """workspace id of test workspace"""
    test_workspace = TestWorkspace()
    test_workspace.test_create_workspace()
    return test_workspace.workspace_id


@pytest.mark.skip(reason="Skipping this test as airbyte integraion needs to be done")
class TestAirbyteSource:
    """class which holds all the source tests"""

    def test_source_connection(self, test_workspace_id):
        """tests connectivity to a source"""
        source_configs = integration_configs.get("sources", [])
        source_definitions = get_source_definitions(workspace_id=test_workspace_id)[
            "sourceDefinitions"
        ]

        for source_config in source_configs:
            name = source_config["name"]
            config = source_config["config"]

            # Finiding source definition for the current source
            source_definition = None
            for sd in source_definitions:
                if sd["name"] == name:
                    source_definition = sd
                    break

            if not source_definition:
                raise ValueError(f"Source definition '{name}' not found.")

            source_definition_id = source_definition["sourceDefinitionId"]

            # Check the source connection
            try:
                res = check_source_connection(
                    test_workspace_id,
                    AirbyteSourceCreate(
                        name=f"source_{name.lower().replace(' ', '_')}",
                        sourceDefId=source_definition_id,
                        config=config,
                    ),
                )
                CheckSourceConnectionTestResponse(**res)
                print(f"Successfully connected to source: {name}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for source '{name}': {error.errors()}"
                )

    def test_a_create_source(self, test_workspace_id):
        """Tests source creation dynamically for each source."""
        source_configs = self.load_configurations()
        source_definitions = get_source_definitions(workspace_id=test_workspace_id)[
            "sourceDefinitions"
        ]

        TestAirbyteSource.created_sources = []  # Store created source IDs for later tests

        for source_config in source_configs:
            name = source_config["name"]
            config = source_config["config"]

            # Find the source definition
            source_definition = None
            for sd in source_definitions:
                if sd["name"] == name:
                    source_definition = sd
                    break

            if not source_definition:
                raise ValueError(f"Source definition '{name}' not found.")

            source_definition_id = source_definition["sourceDefinitionId"]

            # Build the payload for source creation
            payload = {
                "sourcedef_id": source_definition_id,
                "config": config,
                "workspace_id": str(test_workspace_id),
                "name": f"source_{name.lower().replace(' ', '_')}",
            }

            # Create the source
            try:
                CreateSourceTestPayload(**payload)
                res = create_source(**payload)
                CreateSourceTestResponse(**res)
                TestAirbyteSource.created_sources.append(
                    {"source_id": res["sourceId"], "config": config}
                )
                print(f"Successfully created source: {name} with ID: {res['sourceId']}")
            except ValidationError as error:
                raise ValueError(f"Error creating source '{name}': {error.errors()}")

    def test_source_connection_for_update(self):
        """Tests connectivity for updating all sources dynamically."""
        for source in TestAirbyteSource.created_sources:
            source_id = source["source_id"]
            config = source["config"]
            name = source["name"]

            try:
                res = check_source_connection_for_update(
                    source_id,
                    AirbyteSourceUpdateCheckConnection(
                        name=name,
                        config=config,
                    ),
                )
                CheckSourceConnectionTestResponse(**res)
                print(f"Successfully tested connection for update on source ID: {source_id}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for source ID '{source_id}': {error.errors()}"
                )

    def test_get_definitions(self, test_workspace_id):
        """Tests retrieval of source definitions."""
        try:
            res = get_source_definitions(workspace_id=test_workspace_id)["sourceDefinitions"]
            GetSourceDefinitionsTestResponse(__root__=res)
            print("Successfully retrieved source definitions.")
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}")

    def test_get_source_schema_catalog(self, test_workspace_id):
        """Fetches the schema catalog for all created sources."""
        for source in TestAirbyteSource.created_sources:
            source_id = source["source_id"]
            try:
                res = get_source_schema_catalog(test_workspace_id, source_id)
                GetSourceSchemaCatalogTestResponse(catalog=res)
                print(f"Successfully retrieved schema catalog for source ID: {source_id}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for source ID '{source_id}': {error.errors()}"
                )

    def test_fail_to_get_source_schema_catalog(self, test_workspace_id):
        """Fetches the schema catalog for a non-existent source to validate failure."""
        try:
            get_source_schema_catalog(test_workspace_id, "not-a-source-id")
        except HttpError:
            print("Correctly failed to fetch schema catalog for invalid source ID.")

    def test_get_source(self, test_workspace_id):
        """Tests retrieval of a single source."""
        for source in TestAirbyteSource.created_sources:
            source_id = source["source_id"]
            try:
                res = get_source(
                    workspace_id=test_workspace_id,
                    source_id=source_id,
                )
                GetSourceTestResponse(**res)
                print(f"Successfully retrieved details for source ID: {source_id}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for source ID '{source_id}': {error.errors()}"
                )

    def test_get_sources(self, test_workspace_id):
        """Tests retrieval of all sources."""
        try:
            res = get_sources(workspace_id=test_workspace_id)["sources"]
            GetSourcesTestResponse(sources=res)
            print("Successfully retrieved all sources.")
        except ValidationError as error:
            raise ValueError(f"Response validation failed: {error.errors()}")

    def test_update_source(self):
        """Tests updating all created sources dynamically."""
        for source in TestAirbyteSource.created_sources:
            source_id = source["source_id"]
            config = source["config"]
            source_definition_id = source["sourceDefinitionId"]

            payload = {
                "name": f"updated_{source_id}",
                "sourcedef_id": source_definition_id,
                "source_id": source_id,
                "config": config,
            }

            try:
                UpdateSourceTestPayload(**payload)
                res = update_source(**payload)
                UpdateSourceTestResponse(**res)
                print(f"Successfully updated source ID: {source_id}")
            except ValidationError as error:
                raise ValueError(
                    f"Response validation failed for source ID '{source_id}': {error.errors()}"
                )


@pytest.fixture(scope="session")
def test_source_id(test_workspace_id):
    test_source = TestAirbyteSource()
    test_source.test_a_create_source(test_workspace_id)
    return test_source.source_id


@pytest.mark.skip(reason="Skipping this test as airbyte integraion needs to be done")
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
        destination_definitions = get_destination_definitions(workspace_id=test_workspace_id)[
            "destinationDefinitions"
        ]
        for destination_definition in destination_definitions:
            if destination_definition["name"] == "Postgres":
                destination_definition_id = destination_definition["destinationDefinitionId"]
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
            raise ValueError(f"Field do not match in payload: {error.errors()}") from error

        try:
            res = create_destination(**payload)
            CreateDestinationTestResponse(**res)
            TestAirbyteDestination.destination_id = res["destinationId"]
            TestAirbyteDestination.destination_definition_id = res["destinationDefinitionId"]
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
            raise ValueError(f"Field do not match in payload: {error.errors()}") from error

        try:
            res = update_destination(**payload)
            UpdateDestinationTestResponse(**res)
        except ValidationError as error:
            raise ValueError(f"Field do not match in resposne: {error.errors()}") from error

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


@pytest.mark.skip(reason="Skipping this test as airbyte integraion needs to be done")
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
                    "cursorField": "default",
                }
            ],
        )

        try:
            res = create_connection(workspace_id, connection_info)
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
                assert stream["config"]["cursorField"] == []

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
                    "cursorField": ["default"],
                }
            ],
            name="New Connection Name",
        )

        try:
            res = update_connection(workspace_id, connection_info, current_connection)
            UpdateConnectionTestResponse(**res)

            # check if the streams have been set in the connection
            conn = get_connection(workspace_id, res["connectionId"])
            assert conn is not None
            assert "syncCatalog" in conn
            assert "streams" in conn["syncCatalog"]
            assert len(conn["syncCatalog"]["streams"]) == len(connection_info.streams)

            for stream in conn["syncCatalog"]["streams"]:
                assert "config" in stream
                assert stream["config"]["selected"] is True
                assert stream["config"]["cursorField"] == []

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
