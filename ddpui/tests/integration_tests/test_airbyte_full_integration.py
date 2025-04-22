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
from dotenv import load_dotenv
from typing import List, Dict, Optional
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
AIRBYTE_TEST_DESTINATIONS = os.getenv("AIRBYTE_TEST_DESTINATIONS", "postgres").split(",")
AIRBYTE_TEST_SOURCES = os.getenv("AIRBYTE_TEST_SOURCES", "file").split(",")
AIRBYTE_TEST_TIMEOUT = int(os.getenv("AIRBYTE_TEST_TIMEOUT", "300"))
AIRBYTE_SYNC_WAIT_INTERVAL = int(os.getenv("AIRBYTE_SYNC_WAIT_INTERVAL", "10"))
AIRBYTE_MAX_WAIT_CYCLES = int(os.getenv("AIRBYTE_MAX_WAIT_CYCLES", "30"))

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
    """Check if Airbyte server is available"""
    try:
        response = requests.get(
            f"http://{AIRBYTE_SERVER_HOST}:{AIRBYTE_SERVER_PORT}/api/{AIRBYTE_SERVER_API_VERSION}/health",
            timeout=5
        )
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Skip these tests by default unless AIRBYTE_TEST_ENABLED is true
# and the Airbyte server is available
pytestmark = pytest.mark.skipif(
    not AIRBYTE_TEST_ENABLED or not check_airbyte_available(),
    reason="Airbyte integration tests are disabled or Airbyte server is not available"
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
        
        # Create a workspace
        try:
            workspace_result = create_workspace(f"test-workspace-{int(time.time())}")
            cls.workspace_id = workspace_result["workspaceId"]
            logger.info(f"Created workspace with ID: {cls.workspace_id}")
        except HttpError as e:
            logger.error(f"Failed to create workspace: {str(e)}")
            pytest.skip(f"Could not create workspace: {str(e)}")
            return
        except Exception as e:
            logger.error(f"Failed to create workspace: {str(e)}")
            pytest.skip(f"Unexpected error: {str(e)}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up by deleting the workspace and all resources"""
        if not cls.workspace_id:
            return
            
        logger.info(f"Cleaning up workspace {cls.workspace_id} and all resources")
        
        # Delete connections first
        for connection_id in cls.connections.values():
            try:
                delete_connection(cls.workspace_id, connection_id)
                logger.info(f"Deleted connection: {connection_id}")
            except Exception as e:
                logger.warning(f"Failed to delete connection {connection_id}: {str(e)}")
        
        # Delete sources
        for source_id in cls.sources.values():
            try:
                delete_source(cls.workspace_id, source_id)
                logger.info(f"Deleted source: {source_id}")
            except Exception as e:
                logger.warning(f"Failed to delete source {source_id}: {str(e)}")
        
        # Delete destinations
        for destination_id in cls.destinations.values():
            try:
                delete_destination(cls.workspace_id, destination_id)
                logger.info(f"Deleted destination: {destination_id}")
            except Exception as e:
                logger.warning(f"Failed to delete destination {destination_id}: {str(e)}")
        
        # Delete workspace
        try:
            delete_workspace(cls.workspace_id)
            logger.info(f"Deleted workspace: {cls.workspace_id}")
        except Exception as e:
            logger.warning(f"Failed to delete workspace {cls.workspace_id}: {str(e)}")
    
    def test_01_create_destinations(self):
        """Create destinations for testing"""
        assert self.workspace_id is not None, "Workspace must be created before running tests"
        
        # Get available destination definitions
        try:
            dest_definitions = get_destination_definitions(self.workspace_id)["destinationDefinitions"]
            dest_def_map = {d["name"].lower(): d["destinationDefinitionId"] for d in dest_definitions}
            
            for dest_type in AIRBYTE_TEST_DESTINATIONS:
                # Skip if dest_type not in configs
                if dest_type not in DESTINATION_CONFIGS:
                    logger.warning(f"Skipping unknown destination type: {dest_type}")
                    continue
                    
                # Find destination definition ID
                dest_def_key = None
                for key in dest_def_map:
                    if dest_type.lower() in key:
                        dest_def_key = key
                        break
                
                if not dest_def_key:
                    logger.warning(f"Could not find destination definition for {dest_type}")
                    continue
                    
                dest_def_id = dest_def_map[dest_def_key]
                dest_config = DESTINATION_CONFIGS[dest_type]
                
                try:
                    result = create_destination(
                        self.workspace_id,
                        dest_config["name"],
                        dest_def_id,
                        dest_config["config"]
                    )
                    
                    destination_id = result["destinationId"]
                    self.destinations[dest_type] = destination_id
                    logger.info(f"Created {dest_type} destination with ID: {destination_id}")
                except Exception as e:
                    logger.error(f"Failed to create {dest_type} destination: {str(e)}")
            
            assert len(self.destinations) > 0, "At least one destination must be created"
        except Exception as e:
            pytest.skip(f"Could not get destination definitions: {str(e)}")
    
    def test_02_create_sources(self):
        """Create sources for testing"""
        assert self.workspace_id is not None, "Workspace must be created before running tests"
        
        # Get available source definitions
        try:
            source_definitions = get_source_definitions(self.workspace_id)["sourceDefinitions"]
            source_def_map = {s["name"].lower(): s["sourceDefinitionId"] for s in source_definitions}
            
            for source_type in AIRBYTE_TEST_SOURCES:
                # Skip if source_type not in configs
                if source_type not in SOURCE_CONFIGS:
                    logger.warning(f"Skipping unknown source type: {source_type}")
                    continue
                    
                # Find source definition ID
                source_def_key = None
                for key in source_def_map:
                    if source_type.lower() in key:
                        source_def_key = key
                        break
                
                if not source_def_key:
                    logger.warning(f"Could not find source definition for {source_type}")
                    continue
                    
                source_def_id = source_def_map[source_def_key]
                source_config = SOURCE_CONFIGS[source_type]
                
                try:
                    result = create_source(
                        self.workspace_id,
                        source_config["name"],
                        source_def_id,
                        source_config["config"]
                    )
                    
                    source_id = result["sourceId"]
                    self.sources[source_type] = source_id
                    logger.info(f"Created {source_type} source with ID: {source_id}")
                except Exception as e:
                    logger.error(f"Failed to create {source_type} source: {str(e)}")
            
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
                    streams = []
                    for stream_entry in catalog["catalog"]["streams"]:
                        stream = stream_entry["stream"]
                        stream["syncMode"] = "full_refresh"
                        stream["destinationSyncMode"] = "overwrite"
                        streams.append(stream)
                    
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
                    logger.info(f"Created connection {connection_name} with ID: {connection_id}")
                except Exception as e:
                    logger.error(f"Failed to create connection {connection_name}: {str(e)}")
        
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
                    logger.error(f"No job ID returned for connection {connection_name}")
                    continue
                
                logger.info(f"Started sync for connection {connection_name}, job ID: {job_id}")
                
                # Wait for the sync to complete
                status = "pending"
                wait_cycles = 0
                
                while status not in ["succeeded", "failed", "cancelled"] and wait_cycles < AIRBYTE_MAX_WAIT_CYCLES:
                    time.sleep(AIRBYTE_SYNC_WAIT_INTERVAL)
                    job_info = get_job_info(job_id)
                    status = job_info.get("job", {}).get("status", "unknown")
                    logger.info(f"Sync job {job_id} status: {status}")
                    wait_cycles += 1
                
                assert status == "succeeded", f"Sync job {job_id} for connection {connection_name} failed with status: {status}"
                logger.info(f"Sync job {job_id} for connection {connection_name} completed successfully")
            except Exception as e:
                logger.error(f"Error running sync for connection {connection_name}: {str(e)}")
                pytest.fail(f"Sync failed for connection {connection_name}: {str(e)}")
    
    def test_05_validate_sync_history(self):
        """Validate fetching sync history for connections"""
        assert len(self.connections) > 0, "Connections must be created before validating sync history"
        
        for connection_name, connection_id in self.connections.items():
            try:
                # Get jobs for the connection
                jobs = get_jobs_for_connection(connection_id, limit=5)
                
                assert jobs is not None, f"Failed to get jobs for connection {connection_name}"
                logger.info(f"Retrieved {len(jobs)} jobs for connection {connection_name}")
                
                # Check if we have at least one job
                assert len(jobs) > 0, f"No jobs found for connection {connection_name}"
                
                # Get more details about the most recent job
                job_id = jobs[0].get("job", {}).get("id")
                if job_id:
                    job_info = get_job_info(job_id)
                    assert job_info is not None, f"Failed to get job info for job {job_id}"
                    logger.info(f"Successfully retrieved job info for job {job_id}")
                    
                    # Get logs
                    attempt_id = job_info.get("attempts", [{}])[0].get("id", 0)
                    logs = get_logs_for_job(job_id, attempt_id)
                    assert logs is not None, f"Failed to get logs for job {job_id}"
                    logger.info(f"Successfully retrieved logs for job {job_id}")
            except Exception as e:
                logger.error(f"Error validating sync history for connection {connection_name}: {str(e)}")
                pytest.fail(f"Sync history validation failed for connection {connection_name}: {str(e)}")

if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 