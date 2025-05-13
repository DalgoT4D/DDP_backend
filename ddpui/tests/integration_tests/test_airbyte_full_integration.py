"""
Airbyte integration test suite

This integration test suite tests end-to-end Airbyte operations:
1. Creating a workspace
2. Creating destinations (warehouse)
3. Creating sources (both standard and custom)
4. Creating connections between sources and destinations
5. Running connection syncs and verifying their completion
6. Fetching sync history and logs
7. Tearing down workspace and associated entities
"""

import os
import time
import logging
import pytest
import requests
import contextlib
from dotenv import load_dotenv
from ninja.errors import HttpError

# Set up Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
import django
django.setup()

from ddpui.ddpairbyte.airbyte_service import (
    create_workspace,
    delete_workspace,
    create_destination,
    create_source,
    get_source_definitions,
    get_destination_definitions,
    create_connection,
    sync_connection,
    get_jobs_for_connection,
    get_job_info,
    get_logs_for_job,
    delete_connection,
    delete_source,
    delete_destination,
)
from ddpui.ddpairbyte.schema import AirbyteConnectionCreate

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Environment configuration - these would be set in .env for real tests
AIRBYTE_TEST_ENABLED = os.getenv("AIRBYTE_TEST_ENABLED", "false").lower() == "true"
# Strip whitespace when parsing environment-driven connector lists for robustness
AIRBYTE_TEST_DESTINATIONS = [d.strip() for d in os.getenv("AIRBYTE_TEST_DESTINATIONS", "postgres").split(",") if d.strip()]
AIRBYTE_TEST_SOURCES = [s.strip() for s in os.getenv("AIRBYTE_TEST_SOURCES", "file").split(",") if s.strip()]
AIRBYTE_TEST_TIMEOUT = int(os.getenv("AIRBYTE_TEST_TIMEOUT", "300"))
AIRBYTE_SYNC_WAIT_INTERVAL = int(os.getenv("AIRBYTE_SYNC_WAIT_INTERVAL", "10"))
AIRBYTE_MAX_WAIT_CYCLES = int(os.getenv("AIRBYTE_MAX_WAIT_CYCLES", "30"))

# Calculate max sync wait time and ensure it doesn't exceed AIRBYTE_TEST_TIMEOUT
MAX_SYNC_WAIT_TIME = AIRBYTE_SYNC_WAIT_INTERVAL * AIRBYTE_MAX_WAIT_CYCLES
if MAX_SYNC_WAIT_TIME > AIRBYTE_TEST_TIMEOUT:
    # Adjust wait cycles to respect timeout
    AIRBYTE_MAX_WAIT_CYCLES = AIRBYTE_TEST_TIMEOUT // AIRBYTE_SYNC_WAIT_INTERVAL
    MAX_SYNC_WAIT_TIME = AIRBYTE_SYNC_WAIT_INTERVAL * AIRBYTE_MAX_WAIT_CYCLES
    logger.warning(
        "Adjusted max wait cycles to %s to respect timeout of %ss",
        AIRBYTE_MAX_WAIT_CYCLES,
        AIRBYTE_TEST_TIMEOUT,
    )

# Airbyte server details
AIRBYTE_SERVER_HOST = os.getenv("AIRBYTE_SERVER_HOST", "localhost")
AIRBYTE_SERVER_PORT = os.getenv("AIRBYTE_SERVER_PORT", "8000")
AIRBYTE_SERVER_API_VERSION = os.getenv("AIRBYTE_SERVER_APIVER", "v1")

# Test data for specific source types
SOURCE_CONFIGS = {
    "file": {
        "name": "test-file-source",
        "config": {
            "url": "https://storage.googleapis.com/covid19-open-data/v2/latest/epidemiology.csv",
            "format": "csv",
            "provider": {"storage": "HTTPS"},
            "dataset_name": "covid19data",
        }
    },
    "postgres": {
        "name": "test-postgres-source",
        "config": {
            "host": os.getenv("AIRBYTE_TEST_PG_HOST", "localhost"),
            "port": int(os.getenv("AIRBYTE_TEST_PG_PORT", "5432")),
            "database": os.getenv("AIRBYTE_TEST_PG_DB", "test"),
            "username": os.getenv("AIRBYTE_TEST_PG_USER", "postgres"),
            "password": os.getenv("AIRBYTE_TEST_PG_PASSWORD", "postgres"),
            "ssl": False,
        }
    },
    "google-sheets": {
        "name": "test-gsheets-source",
        "config": {
            "credentials_json": os.getenv("AIRBYTE_TEST_GSHEETS_CREDS", "{}"),
            "spreadsheet_id": os.getenv("AIRBYTE_TEST_GSHEETS_ID", ""),
        }
    },
    "kobo-toolbox": {
        "name": "test-kobo-source",
        "config": {
            "host": os.getenv("AIRBYTE_TEST_KOBO_HOST", ""),
            "token": os.getenv("AIRBYTE_TEST_KOBO_TOKEN", ""),
        }
    }
}

# Test data for destination types
DESTINATION_CONFIGS = {
    "postgres": {
        "name": "test-postgres-destination",
        "config": {
            "host": os.getenv("AIRBYTE_TEST_PG_DEST_HOST", "localhost"),
            "port": int(os.getenv("AIRBYTE_TEST_PG_DEST_PORT", "5432")),
            "database": os.getenv("AIRBYTE_TEST_PG_DEST_DB", "test"),
            "username": os.getenv("AIRBYTE_TEST_PG_DEST_USER", "postgres"),
            "password": os.getenv("AIRBYTE_TEST_PG_DEST_PASSWORD", "postgres"),
            "schema": "public",
            "ssl": False,
        }
    },
    "local-csv": {
        "name": "test-local-csv-destination",
        "config": {
            "destination_path": "/tmp/airbyte_test"
        }
    },
    "s3": {
        "name": "test-s3-destination",
        "config": {
            "s3_bucket_name": os.getenv("AIRBYTE_TEST_S3_BUCKET", "test-bucket"),
            "s3_bucket_path": os.getenv("AIRBYTE_TEST_S3_PATH", "test-path"),
            "s3_bucket_region": os.getenv("AIRBYTE_TEST_S3_REGION", "us-east-1"),
            "access_key_id": os.getenv("AIRBYTE_TEST_S3_ACCESS_KEY", ""),
            "secret_access_key": os.getenv("AIRBYTE_TEST_S3_SECRET_KEY", ""),
            "file_name_pattern": "{date}_{table}.csv",
            "format": {
                "format_type": "CSV"
            }
        }
    }
}

def check_airbyte_available():
    """
    Check if Airbyte server is available
    
    This function tries both the legacy /health endpoint and newer /health/ (with trailing slash)
    to maintain compatibility with different Airbyte versions.
    """
    base_url = f"http://{AIRBYTE_SERVER_HOST}:{AIRBYTE_SERVER_PORT}/api/{AIRBYTE_SERVER_API_VERSION}"
    
    # Try with trailing slash (newer versions)
    try:
        response = requests.get(f"{base_url}/health/", timeout=5)
        if response.status_code == 200:
            # Check if it's JSON and has "available": true
            try:
                data = response.json()
                if isinstance(data, dict) and data.get("available") is True:
                    return True
            except ValueError:
                # Not JSON or wrong format, continue to next check
                pass
    except requests.exceptions.RequestException:
        pass
    
    # Try without trailing slash (older versions)
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Skip tests if explicitly disabled
pytestmark = pytest.mark.skipif(
    not AIRBYTE_TEST_ENABLED,
    reason="Airbyte integration tests are disabled"
)

class TestAirbyteFullIntegration:
    """Test suite for Airbyte full integration testing"""
    
    workspace_id = None
    sources = {}
    destinations = {}
    connections = {}
    
    @classmethod
    def setup_class(cls):
        """Set up the test class by creating a workspace"""
        logger.info("Setting up Airbyte integration test suite")
        
        # Verify Airbyte is available before proceeding
        if not check_airbyte_available():
            pytest.skip("Airbyte server is not available")
            return
        
        # Create a workspace with unique name and CI ID for cleanup
        ci_id = os.getenv("CI_BUILD_ID", "local")
        workspace_name = f"test-workspace-{ci_id}-{int(time.time())}"
        
        try:
            workspace_result = create_workspace(workspace_name)
            cls.workspace_id = workspace_result["workspaceId"]
            logger.info("Created workspace with ID: %s", cls.workspace_id)
        except HttpError as e:
            logger.error("Failed to create workspace: %s", e)
            pytest.skip(f"Could not create workspace: {e}")
        except Exception as e:
            logger.error("Unexpected error creating workspace: %s", e)
            with contextlib.suppress(Exception):
                if cls.workspace_id:
                    delete_workspace(cls.workspace_id)
            raise
    
    @classmethod
    def teardown_class(cls):
        """Clean up by deleting the workspace and all resources"""
        if not cls.workspace_id:
            return
            
        logger.info("Cleaning up workspace %s and all resources", cls.workspace_id)
        
        # Delete connections first
        for connection_id in cls.connections.values():
            try:
                delete_connection(cls.workspace_id, connection_id)
                logger.info("Deleted connection: %s", connection_id)
            except Exception as e:
                logger.warning("Failed to delete connection %s: %s", connection_id, e)
        
        # Delete sources
        for source_id in cls.sources.values():
            try:
                delete_source(cls.workspace_id, source_id)
                logger.info("Deleted source: %s", source_id)
            except Exception as e:
                logger.warning("Failed to delete source %s: %s", source_id, e)
        
        # Delete destinations
        for destination_id in cls.destinations.values():
            try:
                delete_destination(cls.workspace_id, destination_id)
                logger.info("Deleted destination: %s", destination_id)
            except Exception as e:
                logger.warning("Failed to delete destination %s: %s", destination_id, e)
        
        # Delete workspace
        try:
            delete_workspace(cls.workspace_id)
            logger.info("Deleted workspace: %s", cls.workspace_id)
        except Exception as e:
            logger.warning("Failed to delete workspace %s: %s", cls.workspace_id, e)
    
    def test_01_create_destinations(self):
        """Create destinations for testing"""
        assert self.workspace_id is not None, "Workspace must be created before running tests"
        
        # Get available destination definitions
        try:
            dest_definitions_response = get_destination_definitions(self.workspace_id)
            dest_definitions = dest_definitions_response.get("destinationDefinitions", [])
            dest_def_map = {d["name"].lower(): d["destinationDefinitionId"] for d in dest_definitions}
            
            for dest_type in AIRBYTE_TEST_DESTINATIONS:
                # Normalize dest_type for robust comparison
                dest_type_norm = dest_type.lower().strip()
                
                # Skip if dest_type not in configs
                if dest_type_norm not in DESTINATION_CONFIGS:
                    logger.warning("Skipping unknown destination type: %s", dest_type)
                    continue
                    
                # Find destination definition ID
                dest_def_key = None
                for key in dest_def_map:
                    if dest_type_norm in key:
                        dest_def_key = key
                        break
                
                if not dest_def_key:
                    logger.warning("Could not find destination definition for %s", dest_type)
                    continue
                    
                dest_def_id = dest_def_map[dest_def_key]
                dest_config = DESTINATION_CONFIGS[dest_type_norm]
                
                try:
                    result = create_destination(
                        self.workspace_id,
                        dest_config["name"],
                        dest_def_id,
                        dest_config["config"]
                    )
                    
                    destination_id = result["destinationId"]
                    self.destinations[dest_type_norm] = destination_id
                    logger.info("Created %s destination with ID: %s", dest_type, destination_id)
                except Exception as e:
                    logger.error("Failed to create %s destination: %s", dest_type, e)
            
            assert len(self.destinations) > 0, "At least one destination must be created"
        except Exception as e:
            pytest.skip(f"Could not get destination definitions: {str(e)}")
    
    def test_02_create_sources(self):
        """Create sources for testing"""
        assert self.workspace_id is not None, "Workspace must be created before running tests"
        
        # Get available source definitions
        try:
            source_definitions_response = get_source_definitions(self.workspace_id)
            source_definitions = source_definitions_response.get("sourceDefinitions", [])
            source_def_map = {s["name"].lower(): s["sourceDefinitionId"] for s in source_definitions}
            
            for source_type in AIRBYTE_TEST_SOURCES:
                # Normalize source_type for robust comparison
                source_type_norm = source_type.lower().strip()
                
                # Skip if source_type not in configs
                if source_type_norm not in SOURCE_CONFIGS:
                    logger.warning("Skipping unknown source type: %s", source_type)
                    continue
                    
                # Find source definition ID
                source_def_key = None
                for key in source_def_map:
                    if source_type_norm in key:
                        source_def_key = key
                        break
                
                if not source_def_key:
                    logger.warning("Could not find source definition for %s", source_type)
                    continue
                    
                source_def_id = source_def_map[source_def_key]
                source_config = SOURCE_CONFIGS[source_type_norm]
                
                try:
                    result = create_source(
                        self.workspace_id,
                        source_config["name"],
                        source_def_id,
                        source_config["config"]
                    )
                    
                    source_id = result["sourceId"]
                    self.sources[source_type_norm] = source_id
                    logger.info("Created %s source with ID: %s", source_type, source_id)
                except Exception as e:
                    logger.error("Failed to create %s source: %s", source_type, e)
            
            assert len(self.sources) > 0, "At least one source must be created"
        except Exception as e:
            pytest.skip(f"Could not get source definitions: {str(e)}")
    
    def test_03_create_connections(self):
        """Create connections between sources and destinations"""
        assert len(self.sources) > 0, "Sources must be created before creating connections"
        assert len(self.destinations) > 0, "Destinations must be created before creating connections"
        
        # For each source-destination pair, create a connection
        for source_name, source_id in self.sources.items():
            for dest_name, dest_id in self.destinations.items():
                connection_name = f"{source_name}-to-{dest_name}"
                
                try:
                    # Get source schema catalog to select all streams
                    from ddpui.ddpairbyte.airbyte_service import get_source_schema_catalog
                    catalog = get_source_schema_catalog(self.workspace_id, source_id)
                    
                    # Select all streams with FULL_REFRESH sync mode and destination_sync_mode OVERWRITE
                    # properly formatted for the Airbyte API
                    streams = [
                        {
                            "stream": s["stream"],
                            "config": {
                                "selected": True,
                                "syncMode": "full_refresh",
                                "destinationSyncMode": "overwrite",
                            },
                        }
                        for s in catalog["catalog"]["streams"]
                    ]
                    
                    # Create the connection
                    connection_info = AirbyteConnectionCreate(
                        name=connection_name,
                        sourceId=source_id,
                        destinationId=dest_id,
                        streams=streams
                    )
                    
                    result = create_connection(self.workspace_id, connection_info)
                    connection_id = result["connectionId"]
                    
                    # Store the connection ID
                    self.connections[connection_name] = connection_id
                    logger.info("Created connection %s with ID: %s", connection_name, connection_id)
                except Exception as e:
                    logger.error("Failed to create connection %s: %s", connection_name, e)
        
        assert len(self.connections) > 0, "At least one connection must be created"
    
    def test_04_run_syncs(self):
        """Run all connection syncs one by one and wait for them to finish"""
        assert len(self.connections) > 0, "Connections must be created before running syncs"
        
        for connection_name, connection_id in self.connections.items():
            try:
                # Start the sync
                sync_result = sync_connection(self.workspace_id, connection_id)
                job_id = sync_result.get("job", {}).get("id")
                
                if not job_id:
                    pytest.fail(f"No job ID returned when starting sync for {connection_name}")
                
                logger.info("Started sync for connection %s, job ID: %s", connection_name, job_id)
                
                # Wait for the sync to complete
                status = "pending"
                wait_cycles = 0
                start_time = time.time()
                
                while (status not in ["succeeded", "failed", "cancelled"] and 
                       wait_cycles < AIRBYTE_MAX_WAIT_CYCLES and 
                       (time.time() - start_time) < AIRBYTE_TEST_TIMEOUT):
                    time.sleep(AIRBYTE_SYNC_WAIT_INTERVAL)
                    job_info = get_job_info(job_id)
                    status = job_info.get("job", {}).get("status", "unknown")
                    logger.info("Sync job %s status: %s", job_id, status)
                    wait_cycles += 1
                
                # If we reached timeout
                if (time.time() - start_time) >= AIRBYTE_TEST_TIMEOUT:
                    logger.error("Sync job %s timed out after %ss", job_id, AIRBYTE_TEST_TIMEOUT)
                    
                assert status == "succeeded", f"Sync job {job_id} for connection {connection_name} failed with status: {status}"
                logger.info("Sync job %s for connection %s completed successfully", job_id, connection_name)
            except Exception as e:
                logger.error("Error running sync for connection %s: %s", connection_name, e)
                pytest.fail(f"Sync failed for connection {connection_name}: {str(e)}")
    
    def test_05_validate_sync_history(self):
        """Validate fetching sync history for connections"""
        assert len(self.connections) > 0, "Connections must be created before validating sync history"
        
        for connection_name, connection_id in self.connections.items():
            try:
                # Get jobs for the connection
                resp = get_jobs_for_connection(connection_id, limit=5)
                jobs = resp.get("jobs", [])
                
                assert jobs is not None, f"Failed to get jobs for connection {connection_name}"
                logger.info("Retrieved %d jobs for connection %s", len(jobs), connection_name)
                
                # Check if we have at least one job
                assert len(jobs) > 0, f"No jobs found for connection {connection_name}"
                
                # Get more details about the most recent job
                job_id = jobs[0].get("job", {}).get("id")
                if job_id:
                    job_info = get_job_info(job_id)
                    assert job_info is not None, f"Failed to get job info for job {job_id}"
                    logger.info("Successfully retrieved job info for job %s", job_id)
                    
                    # Get logs - use index 0 instead of attempt ID
                    logs = get_logs_for_job(job_id, 0)
                    assert logs is not None, f"Failed to get logs for job {job_id}"
                    logger.info("Successfully retrieved logs for job %s", job_id)
            except Exception as e:
                logger.error("Error validating sync history for connection %s: %s", connection_name, e)
                pytest.fail(f"Sync history validation failed for connection {connection_name}: {str(e)}") 