from typing import List, Dict, Any, Optional, Union, Tuple, cast, TypeVar, TYPE_CHECKING
import json
import os
import requests
import time
from django.conf import settings
from pydantic import BaseModel, Field

from ddpui.models.org import Org
from ddpui.models.warehouse import WarehouseCredential
from ddpui.utils.custom_logger import CustomLogger

# Type imports for type checking only
if TYPE_CHECKING:
    from ddpui.ddpairbyte.airbyte_service import AirbyteService

logger = CustomLogger("airbyte_helpers")

# Define Pydantic models for API requests and responses
class WorkspaceResponse(BaseModel):
    workspaceId: str
    name: str
    
class DestinationDefinition(BaseModel):
    destinationDefinitionId: str
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: Optional[str] = None
    icon: Optional[str] = None

class DestinationConfig(BaseModel):
    host: str
    port: int
    database: str
    username: str
    password: str
    schema_name: str = Field(..., alias="schema")  # Renamed to schema_name to avoid conflict with BaseModel.schema
    ssl: bool

class Destination(BaseModel):
    destinationId: str
    name: str
    destinationDefinitionId: str
    workspaceId: str
    connectionConfiguration: Dict[str, Any]

def normalize_simple_configs(configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize simple configurations for Airbyte.
    
    Args:
        configs: Raw configuration dictionary
        
    Returns:
        Normalized configuration dictionary
    """
    # Copy to avoid modifying the original
    result: Dict[str, Any] = configs.copy()
    
    # Handle special cases for specific fields
    if "start_date" in result and isinstance(result["start_date"], dict):
        start_date: Dict[str, Any] = result["start_date"]
        if "year" in start_date and "month" in start_date and "day" in start_date:
            year: int = start_date["year"]
            month: str = str(start_date["month"]).zfill(2)
            day: str = str(start_date["day"]).zfill(2)
            result["start_date"] = f"{year}-{month}-{day}T00:00:00Z"
            
    return result

def get_user_workspace(org: Org) -> str:
    """
    Get or create a workspace for an organization.
    
    Args:
        org: Organization object
        
    Returns:
        Workspace ID
        
    Raises:
        Exception: If unable to get or create workspace
    """
    airbyte_url: str = os.getenv("AIRBYTE_URL", "http://localhost:8006")
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    
    # First try to find existing workspace
    workspace_list_url: str = f"{airbyte_url}/api/v1/workspaces/list"
    response: requests.Response = requests.post(workspace_list_url, json={}, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to list workspaces: {response.text}")
        
    workspaces: List[Dict[str, Any]] = response.json().get("workspaces", [])
    
    # Safe access to org.id with type check
    org_id = getattr(org, 'id', None)
    if org_id is None:
        raise ValueError("Organization object must have an id attribute")
        
    workspace_name: str = f"org-{org_id}-workspace"
    
    # Look for existing workspace
    for workspace in workspaces:
        if workspace.get("name") == workspace_name:
            workspace_id = workspace.get("workspaceId")
            if workspace_id is not None:
                return str(workspace_id)
            raise ValueError("Workspace missing workspaceId")
    
    # Create new workspace if not found
    create_url: str = f"{airbyte_url}/api/v1/workspaces/create"
    create_data: Dict[str, str] = {"name": workspace_name}
    
    create_response: requests.Response = requests.post(create_url, json=create_data, headers=headers)
    
    if create_response.status_code != 200:
        raise Exception(f"Failed to create workspace: {create_response.text}")
        
    new_workspace: Dict[str, Any] = create_response.json()
    workspace_id = new_workspace.get("workspaceId")
    if workspace_id is not None:
        return str(workspace_id)
    raise ValueError("New workspace missing workspaceId")

def find_source_definition_id_by_name(airbyte_service: 'AirbyteService', source_def_name: str) -> Optional[str]:
    """
    Find source definition ID by name.
    
    Args:
        airbyte_service: AirbyteService instance
        source_def_name: Name of the source definition
        
    Returns:
        Source definition ID if found, None otherwise
    """
    source_defs: List[Dict[str, Any]] = airbyte_service.list_source_definitions()
    
    for source_def in source_defs:
        if source_def.get("name") == source_def_name:
            source_def_id = source_def.get("sourceDefinitionId")
            if source_def_id is not None:
                return str(source_def_id)
            
    return None

def find_destination_definition_id_by_name(airbyte_service: 'AirbyteService', destination_def_name: str) -> Optional[str]:
    """
    Find destination definition ID by name.
    
    Args:
        airbyte_service: AirbyteService instance
        destination_def_name: Name of the destination definition
        
    Returns:
        Destination definition ID if found, None otherwise
    """
    airbyte_url: str = os.getenv("AIRBYTE_URL", "http://localhost:8006")
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    
    list_url: str = f"{airbyte_url}/api/v1/destination_definitions/list"
    data: Dict[str, str] = {"workspaceId": airbyte_service.workspaceId}
    
    response: requests.Response = requests.post(list_url, json=data, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to list destination definitions: {response.text}")
        
    destination_defs: List[Dict[str, Any]] = response.json().get("destinationDefinitions", [])
    
    for dest_def in destination_defs:
        if dest_def.get("name") == destination_def_name:
            dest_def_id = dest_def.get("destinationDefinitionId")
            if dest_def_id is not None:
                return str(dest_def_id)
            
    return None

def create_customer_destination(
    airbyte_service: 'AirbyteService', 
    dbt_target: str, 
    warehouse_credentials: WarehouseCredential
) -> Dict[str, Any]:
    """
    Create or find a destination for a customer.
    
    Args:
        airbyte_service: AirbyteService instance
        dbt_target: DBT target name
        warehouse_credentials: Warehouse credentials
        
    Returns:
        Destination information
        
    Raises:
        Exception: If destination creation fails
    """
    # Check if destination already exists
    destinations: List[Dict[str, Any]] = airbyte_service.list_destinations()
    
    # Safe access to org.id with type check
    org = airbyte_service.org
    org_id = getattr(org, 'id', None)
    if org_id is None:
        raise ValueError("Organization object must have an id attribute")
        
    destination_name: str = f"org-{org_id}-{dbt_target}"
    
    for dest in destinations:
        if dest.get("name") == destination_name:
            return dest
    
    # Create a new destination
    destination_def_id: Optional[str] = find_destination_definition_id_by_name(airbyte_service, "Postgres")
    
    if destination_def_id is None:
        raise Exception("Postgres destination definition not found")
        
    config: Dict[str, Any] = {
        "host": warehouse_credentials.host,
        "port": warehouse_credentials.port,
        "database": warehouse_credentials.dbname,
        "username": warehouse_credentials.username,
        "password": warehouse_credentials.password,
        "schema": dbt_target,
        "ssl": warehouse_credentials.use_ssl
    }
    
    airbyte_url: str = os.getenv("AIRBYTE_URL", "http://localhost:8006")
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    
    create_url: str = f"{airbyte_url}/api/v1/destinations/create"
    create_data: Dict[str, Any] = {
        "workspaceId": airbyte_service.workspaceId,
        "name": destination_name,
        "destinationDefinitionId": destination_def_id,
        "connectionConfiguration": config
    }
    
    response: requests.Response = requests.post(create_url, json=create_data, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to create destination: {response.text}")
        
    return response.json()