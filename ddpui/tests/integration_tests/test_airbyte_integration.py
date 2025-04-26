import os
import django
from pydantic import ValidationError
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.tests.helper.test_airbyte_unit_schemas import *
from ddpui.ddpairbyte.airbyte_service import *

# Load configuration from .env file
from dotenv import load_dotenv
load_dotenv()

class TestAirbyteIntegration:
    """Integration test suite for Airbyte"""

    workspace_id = None
    source_ids = []
    destination_id = None
    connection_ids = []

    @pytest.fixture(scope="class", autouse=True)
    def setup_workspace(self):
        """Create a new Airbyte workspace for testing."""
        payload = {"name": "test_workspace"}
        res = create_workspace(**payload)
        self.workspace_id = res["workspaceId"]
        yield
        delete_workspace(self.workspace_id)

    def test_create_destination(self):
        """Create a destination (warehouse)."""
        destination_definitions = get_destination_definitions(workspace_id=self.workspace_id)["destinationDefinitions"]
        destination_name = os.getenv("TEST_DESTINATION_NAME", "Postgres")
        for definition in destination_definitions:
            if definition["name"] == destination_name:
                destination_definition_id = definition["destinationDefinitionId"]
                break

        payload = {
            "name": "test_destination",
            "destinationdef_id": destination_definition_id,
            "config": {
                "host": os.getenv("TEST_DEST_HOST"),
                "port": int(os.getenv("TEST_DEST_PORT", 5432)),
                "database": os.getenv("TEST_DEST_DB"),
                "username": os.getenv("TEST_DEST_USER"),
                "password": os.getenv("TEST_DEST_PASS"),
                "schema": os.getenv("TEST_DEST_SCHEMA", "public"),
            },
            "workspace_id": self.workspace_id,
        }
        res = create_destination(**payload)
        self.destination_id = res["destinationId"]

    def test_create_sources(self):
        """Create multiple sources based on configuration."""
        source_definitions = get_source_definitions(workspace_id=self.workspace_id)["sourceDefinitions"]
        sources = os.getenv("TEST_SOURCES", "").split(",")
        for source_name in sources:
            for definition in source_definitions:
                if definition["name"] == source_name.strip():
                    source_definition_id = definition["sourceDefinitionId"]
                    break

            payload = {
                "sourcedef_id": source_definition_id,
                "config": {
                    "url": os.getenv(f"{source_name.upper()}_URL"),
                    "format": os.getenv(f"{source_name.upper()}_FORMAT", "csv"),
                    "provider": {"storage": os.getenv(f"{source_name.upper()}_PROVIDER", "HTTPS")},
                    "dataset_name": os.getenv(f"{source_name.upper()}_DATASET", "default_dataset"),
                },
                "workspace_id": self.workspace_id,
                "name": f"test_{source_name.strip().lower()}_source",
            }
            res = create_source(**payload)
            self.source_ids.append(res["sourceId"])

    def test_create_connections(self):
        """Create connections between all sources and the destination."""
        for source_id in self.source_ids:
            payload = {
                "sourceId": source_id,
                "destinationId": self.destination_id,
                "name": f"connection_{source_id}",
                "streams": [
                    {
                        "name": "default_stream",
                        "selected": True,
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                    }
                ],
            }
            res = create_connection(self.workspace_id, payload)
            self.connection_ids.append(res["connectionId"])

    def test_run_syncs(self):
        """Run all connection syncs and validate their completion."""
        for connection_id in self.connection_ids:
            run_sync(self.workspace_id, connection_id)
            sync_status = get_sync_status(self.workspace_id, connection_id)
            assert sync_status == "completed", f"Sync failed for connection {connection_id}"

    def test_validate_logs(self):
        """Validate fetching of sync history and logs for all connections."""
        for connection_id in self.connection_ids:
            logs = get_sync_logs(self.workspace_id, connection_id)
            assert logs is not None, f"No logs found for connection {connection_id}"
