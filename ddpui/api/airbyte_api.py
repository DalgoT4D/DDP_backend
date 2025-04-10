from typing import List, Dict, Any, Optional, Union, Tuple, cast
from pydantic import BaseModel, Field
from django.http import JsonResponse
from rest_framework.request import Request

from ddpui.models.org import Org
from ddpui.models.airbyte_sync import AirbyteSync, AirbyteSyncRun
from ddpui.models.warehouse import WarehouseCredential
from ddpui.models.credentials import DataSourceCredential
from ddpui.ddpairbyte.airbyte_service import AirbyteService
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("airbyte_api")

# Pydantic models for request and response payloads
class SourceDefinitionResponse(BaseModel):
    sourceDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: Optional[str] = None
    icon: Optional[str] = None

class WorkspaceResponse(BaseModel):
    workspaceId: str
    name: str
    
class SourceResponse(BaseModel):
    sourceId: str
    name: str
    sourceDefinitionId: str
    workspaceId: str
    connectionConfiguration: Dict[str, Any]
    
class DestinationResponse(BaseModel):
    destinationId: str
    name: str
    destinationDefinitionId: str
    workspaceId: str
    connectionConfiguration: Dict[str, Any]

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

class ConnectionResponse(BaseModel):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str
    status: str
    syncCatalog: SyncCatalog
    schedule: Optional[Schedule] = None
    
class SourceSchemaResponse(BaseModel):
    catalog: Dict[str, Any]
    
class ConnectionCreateRequest(BaseModel):
    name: str
    sourceId: str
    destinationId: str
    streams: List[Dict[str, Any]]
    schedule: Optional[Dict[str, Any]] = None
    
class ConnectionUpdateRequest(BaseModel):
    connectionId: str
    syncCatalog: Dict[str, Any]
    
class ConnectionSyncRequest(BaseModel):
    connectionId: str
    
class SourceCreateRequest(BaseModel):
    sourceDefId: str
    name: str
    config: Dict[str, Any]
    
class JobStatusResponse(BaseModel):
    jobId: str
    status: str
    logs: Optional[Dict[str, Any]] = None

def get_source_definitions(request: Request, orgid: str) -> JsonResponse:
    """
    Get all source definitions from Airbyte.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        
    Returns:
        JsonResponse containing list of source definitions
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        source_definitions: List[Dict[str, Any]] = airbyte_service.list_source_definitions()
        return JsonResponse({"source_definitions": source_definitions})
    except Exception as e:
        logger.error(f"get_source_definitions: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def get_source_definition_specifications(request: Request, orgid: str, source_definition_id: str) -> JsonResponse:
    """
    Get specifications for a specific source definition.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        source_definition_id: The source definition ID
        
    Returns:
        JsonResponse containing source definition specifications
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        specs: Dict[str, Any] = airbyte_service.get_source_definition_specifications(source_definition_id)
        return JsonResponse({"specifications": specs})
    except Exception as e:
        logger.error(f"get_source_definition_specifications: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def create_source(request: Request, orgid: str) -> JsonResponse:
    """
    Create a new source in Airbyte.
    
    Args:
        request: The HTTP request object containing source details
        orgid: The organization ID
        
    Returns:
        JsonResponse containing the created source information
    """
    try:
        source_create_request: SourceCreateRequest = SourceCreateRequest(**request.data)
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        source: Dict[str, Any] = airbyte_service.create_source(
            source_definition_id=source_create_request.sourceDefId,
            source_name=source_create_request.name,
            connection_configuration=source_create_request.config
        )
        return JsonResponse({"source": source})
    except Exception as e:
        logger.error(f"create_source: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def get_sources(request: Request, orgid: str) -> JsonResponse:
    """
    Get all sources for an organization.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        
    Returns:
        JsonResponse containing list of sources
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        sources: List[Dict[str, Any]] = airbyte_service.list_sources()
        return JsonResponse({"sources": sources})
    except Exception as e:
        logger.error(f"get_sources: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def get_source_schema(request: Request, orgid: str, source_id: str) -> JsonResponse:
    """
    Get schema for a specific source.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        source_id: The source ID
        
    Returns:
        JsonResponse containing source schema
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        schema: Dict[str, Any] = airbyte_service.discover_source_schema(source_id)
        return JsonResponse({"schema": schema})
    except Exception as e:
        logger.error(f"get_source_schema: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def get_destination_definition_specifications(request: Request, orgid: str, destination_definition_id: str) -> JsonResponse:
    """
    Get specifications for a specific destination definition.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        destination_definition_id: The destination definition ID
        
    Returns:
        JsonResponse containing destination definition specifications
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        specs: Dict[str, Any] = airbyte_service.get_destination_definition_specifications(destination_definition_id)
        return JsonResponse({"specifications": specs})
    except Exception as e:
        logger.error(f"get_destination_definition_specifications: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def create_connection(request: Request, orgid: str) -> JsonResponse:
    """
    Create a new connection between source and destination.
    
    Args:
        request: The HTTP request object containing connection details
        orgid: The organization ID
        
    Returns:
        JsonResponse containing the created connection information
    """
    try:
        connection_request: ConnectionCreateRequest = ConnectionCreateRequest(**request.data)
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        
        connection: Dict[str, Any] = airbyte_service.create_connection(
            source_id=connection_request.sourceId,
            destination_id=connection_request.destinationId,
            connection_name=connection_request.name,
            streams=connection_request.streams,
            schedule=connection_request.schedule
        )
        return JsonResponse({"connection": connection})
    except Exception as e:
        logger.error(f"create_connection: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def update_connection(request: Request, orgid: str) -> JsonResponse:
    """
    Update an existing connection.
    
    Args:
        request: The HTTP request object containing updated connection details
        orgid: The organization ID
        
    Returns:
        JsonResponse containing the updated connection information
    """
    try:
        connection_update: ConnectionUpdateRequest = ConnectionUpdateRequest(**request.data)
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        
        result: Dict[str, Any] = airbyte_service.update_connection(
            connection_id=connection_update.connectionId,
            sync_catalog=connection_update.syncCatalog
        )
        return JsonResponse({"result": result})
    except Exception as e:
        logger.error(f"update_connection: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def trigger_sync(request: Request, orgid: str) -> JsonResponse:
    """
    Trigger a sync for a specific connection.
    
    Args:
        request: The HTTP request object containing connection ID
        orgid: The organization ID
        
    Returns:
        JsonResponse containing the sync job information
    """
    try:
        sync_request: ConnectionSyncRequest = ConnectionSyncRequest(**request.data)
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        
        job: Dict[str, Any] = airbyte_service.trigger_sync(sync_request.connectionId)
        return JsonResponse({"job": job})
    except Exception as e:
        logger.error(f"trigger_sync: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)

def get_job_status(request: Request, orgid: str, job_id: str) -> JsonResponse:
    """
    Get status of a specific job.
    
    Args:
        request: The HTTP request object
        orgid: The organization ID
        job_id: The job ID
        
    Returns:
        JsonResponse containing the job status information
    """
    try:
        org: Org = Org.objects.get(id=orgid)
        airbyte_service: AirbyteService = AirbyteService(org)
        status: Dict[str, Any] = airbyte_service.get_job_status(job_id)
        return JsonResponse({"status": status})
    except Exception as e:
        logger.error(f"get_job_status: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)