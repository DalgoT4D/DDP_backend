from typing import List, Dict, Any, Optional, Union, Tuple, cast, TypedDict
import requests
import json
import time
import os
import uuid
from datetime import datetime
from pydantic import BaseModel, Field

from django.conf import settings
from ddpui.models.org import Org
from ddpui.models.warehouse import WarehouseCredential
from ddpui.models.credentials import DataSourceCredential
from ddpui.models.airbyte_sync import AirbyteSync
from ddpui.ddpairbyte.airbyte_helpers import (
    get_user_workspace,
    create_customer_destination,
    normalize_simple_configs,
    find_source_definition_id_by_name,
    find_destination_definition_id_by_name,
)
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("airbyte_service")

# Pydantic models for API payloads and responses
class StreamConfig(BaseModel):
    aliasName: str
    destinationSyncMode: str
    selected: bool
    syncMode: str

class StreamObject(BaseModel):
    name: str
    namespace: Optional[str] = None
    json_schema: Optional[Dict[str, Any]] = None
    supported_sync_modes: Optional[List[str]] = None
    source_defined_cursor: Optional[bool] = None
    default_cursor_field: Optional[List[str]] = None
    source_defined_primary_key: Optional[List[List[str]]] = None
    
class Stream(BaseModel):
    stream: StreamObject
    config: StreamConfig

class SyncCatalog(BaseModel):
    streams: List[Stream]

class ScheduleData(BaseModel):
    basic_schedule: Dict[str, Any] = Field(..., alias="basicSchedule")
    
class Schedule(BaseModel):
    scheduleType: str
    scheduleData: Optional[ScheduleData] = None

class SourceDefinition(BaseModel):
    sourceDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: Optional[str] = None
    icon: Optional[str] = None

class SourceSpecification(BaseModel):
    connectionSpecification: Dict[str, Any]
    
class Source(BaseModel):
    sourceId: str
    name: str
    sourceDefinitionId: str
    workspaceId: str
    connectionConfiguration: Dict[str, Any]
    
class Destination(BaseModel):
    destinationId: str
    name: str
    destinationDefinitionId: str
    workspaceId: str
    connectionConfiguration: Dict[str, Any]

class Connection(BaseModel):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str
    status: str
    syncCatalog: SyncCatalog
    
class Job(BaseModel):
    jobId: str
    status: str
    createdAt: int
    updatedAt: int

class AirbyteService:
    """Service class for interacting with Airbyte API."""
    
    def __init__(self, org: Org) -> None:
        """
        Initialize the Airbyte service.
        
        Args:
            org: The organization object
        """
        self.org: Org = org
        self.airbyte_url: str = os.getenv("AIRBYTE_URL", "http://localhost:8006")
        self.workspaceId: str = get_user_workspace(self.org)
        self.headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        
    def _post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a POST request to the Airbyte API.
        
        Args:
            endpoint: API endpoint path
            data: Request payload
            
        Returns:
            Dict containing the response data
            
        Raises:
            Exception: If the request fails
        """
        url: str = f"{self.airbyte_url}/api/v1/{endpoint}"
        response: requests.Response = requests.post(url, json=data, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"Error in Airbyte API call: {response.text}")
            
        return response.json()
        
    def list_source_definitions(self) -> List[Dict[str, Any]]:
        """
        List all available source definitions.
        
        Returns:
            List of source definitions
        """
        data: Dict[str, str] = {"workspaceId": self.workspaceId}
        response: Dict[str, Any] = self._post("source_definitions/list", data)
        return response.get("sourceDefinitions", [])
        
    def get_source_definition_specifications(self, source_definition_id: str) -> Dict[str, Any]:
        """
        Get specifications for a source definition.
        
        Args:
            source_definition_id: ID of the source definition
            
        Returns:
            Source definition specifications
        """
        data: Dict[str, str] = {"sourceDefinitionId": source_definition_id, "workspaceId": self.workspaceId}
        response: Dict[str, Any] = self._post("source_definition_specifications/get", data)
        return response.get("connectionSpecification", {})
        
    def create_source(
        self, 
        source_definition_id: str, 
        source_name: str, 
        connection_configuration: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new source.
        
        Args:
            source_definition_id: ID of the source definition
            source_name: Name for the new source
            connection_configuration: Configuration for the source connection
            
        Returns:
            Created source information
        """
        data: Dict[str, Any] = {
            "sourceDefinitionId": source_definition_id,
            "connectionConfiguration": connection_configuration,
            "workspaceId": self.workspaceId,
            "name": source_name
        }
        response: Dict[str, Any] = self._post("sources/create", data)
        return response
        
    def list_sources(self) -> List[Dict[str, Any]]:
        """
        List all sources in the workspace.
        
        Returns:
            List of sources
        """
        data: Dict[str, str] = {"workspaceId": self.workspaceId}
        response: Dict[str, Any] = self._post("sources/list", data)
        return response.get("sources", [])
        
    def discover_source_schema(self, source_id: str) -> Dict[str, Any]:
        """
        Discover schema for a source.
        
        Args:
            source_id: ID of the source
            
        Returns:
            Source schema
        """
        data: Dict[str, str] = {"sourceId": source_id}
        response: Dict[str, Any] = self._post("sources/discover_schema", data)
        return response.get("catalog", {})
        
    def get_destination_definition_specifications(self, destination_definition_id: str) -> Dict[str, Any]:
        """
        Get specifications for a destination definition.
        
        Args:
            destination_definition_id: ID of the destination definition
            
        Returns:
            Destination definition specifications
        """
        data: Dict[str, str] = {"destinationDefinitionId": destination_definition_id, "workspaceId": self.workspaceId}
        response: Dict[str, Any] = self._post("destination_definition_specifications/get", data)
        return response.get("connectionSpecification", {})
        
    def list_destinations(self) -> List[Dict[str, Any]]:
        """
        List all destinations in the workspace.
        
        Returns:
            List of destinations
        """
        data: Dict[str, str] = {"workspaceId": self.workspaceId}
        response: Dict[str, Any] = self._post("destinations/list", data)
        return response.get("destinations", [])
        
    def create_connection(
        self, 
        source_id: str, 
        destination_id: str, 
        connection_name: str, 
        streams: List[Dict[str, Any]], 
        schedule: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new connection between source and destination.
        
        Args:
            source_id: ID of the source
            destination_id: ID of the destination
            connection_name: Name for the new connection
            streams: List of streams to sync
            schedule: Optional schedule configuration
            
        Returns:
            Created connection information
        """
        sync_catalog: Dict[str, List[Dict[str, Any]]] = {"streams": streams}
        
        connection_data: Dict[str, Any] = {
            "sourceId": source_id,
            "destinationId": destination_id,
            "name": connection_name,
            "syncCatalog": sync_catalog,
            "status": "active",
        }
        
        if schedule:
            connection_data["scheduleType"] = schedule.get("scheduleType", "manual")
            if schedule.get("scheduleData"):
                connection_data["scheduleData"] = schedule.get("scheduleData")
        
        response: Dict[str, Any] = self._post("connections/create", connection_data)
        return response
        
    def update_connection(
        self, 
        connection_id: str, 
        sync_catalog: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update an existing connection.
        
        Args:
            connection_id: ID of the connection to update
            sync_catalog: New sync catalog configuration
            
        Returns:
            Updated connection information
        """
        data: Dict[str, Any] = {
            "connectionId": connection_id,
            "syncCatalog": sync_catalog
        }
        
        response: Dict[str, Any] = self._post("connections/update", data)
        return response
        
    def trigger_sync(self, connection_id: str) -> Dict[str, Any]:
        """
        Trigger a sync for a connection.
        
        Args:
            connection_id: ID of the connection to sync
            
        Returns:
            Job information
        """
        data: Dict[str, str] = {"connectionId": connection_id}
        response: Dict[str, Any] = self._post("connections/sync", data)
        return response
        
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get status of a job.
        
        Args:
            job_id: ID of the job
            
        Returns:
            Job status information
        """
        data: Dict[str, str] = {"id": job_id}
        response: Dict[str, Any] = self._post("jobs/get", data)
        return response
        
    def create_customer_credential_source(
        self, 
        source_def_name: str, 
        source_name: str, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a source for customer credentials.
        
        Args:
            source_def_name: Name of the source definition
            source_name: Name for the new source
            config: Source configuration
            
        Returns:
            Created source information
        """
        source_def_id: Optional[str] = find_source_definition_id_by_name(self, source_def_name)
        if source_def_id is None:
            raise Exception(f"Source definition {source_def_name} not found")
            
        normalized_config: Dict[str, Any] = normalize_simple_configs(config)
        return self.create_source(source_def_id, source_name, normalized_config)
        
    def create_and_sync_customer_credential_source(
        self, 
        dbt_target: str, 
        warehouse_credentials: WarehouseCredential,
        source_def_name: str, 
        source_name: str, 
        config: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Create a source and sync it with a destination.
        
        Args:
            dbt_target: DBT target
            warehouse_credentials: Warehouse credentials
            source_def_name: Name of the source definition
            source_name: Name for the new source
            config: Source configuration
            
        Returns:
            Tuple of (source information, connection information, job information)
        """
        # Create the source
        source: Dict[str, Any] = self.create_customer_credential_source(
            source_def_name, source_name, config
        )
        
        # Create the destination if needed, or find an existing one
        destination: Dict[str, Any] = create_customer_destination(self, dbt_target, warehouse_credentials)
        
        # Ensure sourceId is a string
        source_id: Any = source.get("sourceId")
        if not isinstance(source_id, str):
            raise ValueError("sourceId must be a string")
        
        # Discover schema for source
        schema: Dict[str, Any] = self.discover_source_schema(source_id)
        
        # Prepare streams for connection with explicit type
        streams: List[Dict[str, Any]] = []
        for stream in schema.get("streams", []):
            stream_name: str = stream.get("stream", {}).get("name", "")  # Default to "" if None
            stream_config: Dict[str, Any] = {
                "stream": stream.get("stream", {}),
                "config": {
                    "aliasName": stream_name,
                    "destinationSyncMode": "append_dedup",
                    "selected": True,
                    "syncMode": "incremental"
                }
            }
            streams.append(stream_config)
        
        # Ensure destinationId and connectionId are strings
        destination_id: Any = destination.get("destinationId")
        if not isinstance(destination_id, str):
            raise ValueError("destinationId must be a string")
        
        # Create the connection
        connection: Dict[str, Any] = self.create_connection(
            source_id=source_id,
            destination_id=destination_id,
            connection_name=f"{source_name} to {dbt_target}",
            streams=streams
        )
        
        # Trigger the sync
        connection_id: Any = connection.get("connectionId")
        if not isinstance(connection_id, str):
            raise ValueError("connectionId must be a string")
        job: Dict[str, Any] = self.trigger_sync(connection_id)
        
        return source, connection, job