"""
Airbyte service module
Functions which communicate with Airbyte
These functions do not access the Dalgo database
"""

from typing import Dict, List, Any, Optional, Union
import os
from datetime import datetime
import requests
from dotenv import load_dotenv
from ninja.errors import HttpError
from flags.state import flag_enabled
from ddpui import settings
from ddpui.ddpairbyte import schema
from ddpui.ddpprefect import prefect_service, AIRBYTESERVER
from ddpui.models.org import (
    Org,
    OrgPrefectBlockv1,
    OrgSchemaChange,
    OrgWarehouseSchema,
    ConnectionJob,
    ConnectionMeta,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import thread
from ddpui.utils.deploymentblocks import trigger_reset_and_sync_workflow
from ddpui.utils.helpers import remove_nested_attribute, nice_bytes
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteDestinationCreate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationUpdateCheckConnection,
)

load_dotenv()

logger = CustomLogger("airbyte")


def abreq(endpoint: str, req: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
    """Request to the airbyte server"""
    method = kwargs.get("method", "POST")
    if method not in ["GET", "POST"]:
        raise HttpError(500, "method not supported")

    request = thread.get_current_request()

    abhost = os.getenv("AIRBYTE_SERVER_HOST")
    abport = os.getenv("AIRBYTE_SERVER_PORT")
    abver = os.getenv("AIRBYTE_SERVER_APIVER")
    token = os.getenv("AIRBYTE_API_TOKEN")

    if request is not None:
        org_user = request.orguser
        org_slug = org_user.org.slug
        if flag_enabled("AIRBYTE_PROFILE", request_org_slug=org_slug):
            org_server_block = OrgPrefectBlockv1.objects.filter(
                org=org_user.org, block_type=AIRBYTESERVER
            ).first()

            if not org_server_block:
                raise HttpError(400, "airbyte server block not found")

            block_name = org_server_block.block_name

            try:
                airbyte_server_block = prefect_service.get_airbyte_server_block(block_name)
            except Exception as exc:
                raise Exception("could not connect to prefect-proxy") from exc

            logger.info("Making request to Airbyte server through prefect block: %s", endpoint)
            abhost = airbyte_server_block["host"]
            abport = airbyte_server_block["port"]
            abver = airbyte_server_block["version"]
            token = airbyte_server_block["token"]

    logger.info("Making request to Airbyte server: %s", endpoint)
    try:
        res = {}
        if method == "POST":
            res = requests.post(
                f"http://{abhost}:{abport}/api/{abver}/{endpoint}",
                headers={"Authorization": f"Basic {token}"},
                json=req,
                timeout=kwargs.get("timeout", 30),
            )
        elif method == "GET":
            res = requests.get(
                f"http://{abhost}:{abport}/api/{abver}/{endpoint}",
                headers={"Authorization": f"Basic {token}"},
                json=req,
                timeout=kwargs.get("timeout", 30),
            )
    except requests.exceptions.ConnectionError as conn_error:
        logger.exception(conn_error)
        raise HttpError(500, str(conn_error)) from conn_error

    try:
        result_obj = remove_nested_attribute(res.json(), "icon")
        logger.debug("Response from Airbyte server:")
        logger.debug(result_obj)
    except ValueError:
        logger.debug("Response from Airbyte server: %s", res.text)

    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error.args)
        raise HttpError(res.status_code, res.text) from error

    if "application/json" in res.headers.get("Content-Type", ""):
        return res.json()

    logger.error(
        "abreq result has content-type %s while hitting %s",
        res.headers.get("Content-Type", ""),
        endpoint,
    )
    return {}


def get_workspaces() -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all workspaces in airbyte server"""
    logger.info("Fetching workspaces from Airbyte server")

    res = abreq("workspaces/list")
    if "workspaces" not in res:
        logger.error("No workspaces found")
        raise HttpError(404, "no workspaces found")
    return res


def get_workspace(workspace_id: str) -> Dict[str, Any]:
    """Fetch a workspace from the airbyte server"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("workspaces/get", {"workspaceId": workspace_id})
    if "workspaceId" not in res:
        logger.info("Workspace not found: %s", workspace_id)
        raise HttpError(404, "workspace not found")
    return res


def set_workspace_name(workspace_id: str, name: str) -> Dict[str, Any]:
    """Set workspace name in the airbyte server"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Workspace ID must be a string")

    if not isinstance(name, str):
        raise HttpError(400, "Name must be a string")

    res = abreq("workspaces/update_name", {"workspaceId": workspace_id, "name": name})
    if "workspaceId" not in res:
        logger.info("Workspace not found: %s", workspace_id)
        raise HttpError(404, "workspace not found")
    return res


def create_workspace(name: str) -> Dict[str, Any]:
    """Create a workspace in the airbyte server"""
    if not isinstance(name, str):
        raise HttpError(400, "Name must be a string")

    res = abreq(
        "workspaces/create",
        {"name": name, "organizationId": "00000000-0000-0000-0000-000000000000"},
    )
    if "workspaceId" not in res:
        logger.info("Workspace not created: %s", name)
        raise HttpError(400, "workspace not created")
    return res


def delete_workspace(workspace_id: str) -> Dict[str, Any]:
    """Deletes an airbyte workspace"""
    res = abreq("workspaces/delete", {"workspaceId": workspace_id})
    return res


def get_source_definition(workspace_id: str, sourcedef_id: str) -> Dict[str, Any]:
    """Fetch source definition for an airbtye workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(sourcedef_id, str):
        raise HttpError(400, "Invalid source definition ID")

    res = abreq(
        "source_definitions/get_for_workspace",
        {"sourceDefinitionId": sourcedef_id, "workspaceId": workspace_id},
    )
    if "sourceDefinitionId" not in res:
        error_message = (
            f"Source definition : {sourcedef_id} not found for workspace: {workspace_id}"
        )
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_source_definitions(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch source definitions for an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("source_definitions/list_for_workspace", {"workspaceId": workspace_id})
    if "sourceDefinitions" not in res:
        error_message = f"Source definitions not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    # filter out sources we don't want to show
    indices = []
    blacklist = settings.AIRBYTE_SOURCE_BLACKLIST
    for idx, sdef in enumerate(res["sourceDefinitions"]):
        if sdef["dockerRepository"] in blacklist:
            indices.append(idx)

    # delete from the end so we don't have index shifting confusion
    for idx in reversed(indices):
        del res["sourceDefinitions"][idx]

    return res


def get_source_definition_specification(workspace_id: str, sourcedef_id: str) -> Dict[str, Any]:
    """Fetch source definition specification for a source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(sourcedef_id, str):
        raise HttpError(400, "Invalid source definition ID")

    res = abreq(
        "source_definition_specifications/get",
        {"sourceDefinitionId": sourcedef_id, "workspaceId": workspace_id},
    )

    if "connectionSpecification" not in res:
        error_message = (
            f"specification not found for source definition {sourcedef_id} "
            f"in workspace {workspace_id}"
        )
        logger.error(error_message)
        raise HttpError(404, error_message)

    if "properties" in res["connectionSpecification"] and (
        "__injected_declarative_manifest" in res["connectionSpecification"]["properties"]
    ):
        # remove the injected manifest
        del res["connectionSpecification"]["properties"]["__injected_declarative_manifest"]

    return res


def create_custom_source_definition(
    workspace_id: str,
    name: str,
    docker_repository: str,
    docker_image_tag: str,
    documentation_url: str,
) -> Dict[str, Any]:
    """Create a custom source definition in Airbyte."""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")
    if not isinstance(name, str):
        raise HttpError(400, "Invalid name")
    if not isinstance(docker_repository, str):
        raise HttpError(400, "Invalid docker repository")
    if not isinstance(docker_image_tag, str):
        raise HttpError(400, "Invalid docker image tag")
    if not isinstance(documentation_url, str):
        raise HttpError(400, "Invalid documentation URL")

    res = abreq(
        "source_definitions/create_custom",
        {
            "workspaceId": workspace_id,
            "sourceDefinition": {
                "name": name,
                "dockerRepository": docker_repository,
                "dockerImageTag": docker_image_tag,
                "documentationUrl": documentation_url,
            },
        },
    )
    if "sourceDefinitionId" not in res:
        error_message = f"Source definition not created: {name}"
        logger.error("Source definition not created: %s", name)
        raise HttpError(400, error_message)
    return res


def get_sources(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all sources in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("sources/list", {"workspaceId": workspace_id})
    if "sources" not in res:
        logger.error("Sources not found for workspace: %s", workspace_id)
        raise HttpError(404, "sources not found for workspace")
    return res


def get_source(workspace_id: str, source_id: str) -> Dict[str, Any]:
    """Fetch a source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(source_id, str):
        raise HttpError(400, "Invalid source ID")

    res = abreq("sources/get", {"sourceId": source_id})
    if "sourceId" not in res:
        logger.error("Source not found: %s", source_id)
        raise HttpError(404, "source not found")
    return res


def delete_source(workspace_id: str, source_id: str) -> Dict[str, Any]:
    """Deletes a source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(source_id, str):
        raise HttpError(400, "Invalid source ID")

    res = abreq("sources/delete", {"sourceId": source_id})
    return res


def create_source(workspace_id: str, name: str, sourcedef_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Create source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(name, str):
        raise HttpError(400, "name must be a string")
    if not isinstance(sourcedef_id, str):
        raise HttpError(400, "sourcedef_id must be a string")
    if not isinstance(config, dict):
        raise HttpError(400, "config must be a dictionary")

    res = abreq(
        "source_definition_specifications/get",
        {"sourceDefinitionId": sourcedef_id, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        raise HttpError(500, "could not find spec for this source type")

    source_definition_spec = res["connectionSpecification"]
    for prop, prop_def in source_definition_spec["properties"].items():
        if prop_def.get("const"):
            config[prop] = prop_def["const"]

    res = abreq(
        "sources/create",
        {
            "workspaceId": workspace_id,
            "name": name,
            "sourceDefinitionId": sourcedef_id,
            "connectionConfiguration": config,
        },
    )
    if "sourceId" not in res:
        logger.error("Failed to create source: %s", res)
        raise HttpError(500, "failed to create source")
    return res


def update_source(source_id: str, name: str, config: Dict[str, Any], sourcedef_id: str) -> Dict[str, Any]:
    """Update source in an airbyte workspace"""
    if not isinstance(source_id, str):
        raise HttpError(400, "source_id must be a string")
    if not isinstance(name, str):
        raise HttpError(400, "name must be a string")
    if not isinstance(config, dict):
        raise HttpError(400, "config must be a dictionary")
    if not isinstance(sourcedef_id, str):
        raise HttpError(400, "sourcedef_id must be a string")

    res = abreq(
        "sources/update",
        {
            "sourceId": source_id,
            "name": name,
            "connectionConfiguration": config,
            "sourceDefinitionId": sourcedef_id,
        },
    )
    if "sourceId" not in res:
        logger.error("Failed to update source: %s", res)
        raise HttpError(500, "failed to update source")
    return res


def check_source_connection(workspace_id: str, data: AirbyteSourceCreate) -> Dict[str, Any]:
    """Test a potential source's connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq(
        "source_definition_specifications/get",
        {"sourceDefinitionId": data.sourceDefId, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        raise HttpError(500, "could not find spec for this source type")

    source_definition_spec = res["connectionSpecification"]
    for prop, prop_def in source_definition_spec["properties"].items():
        if prop_def.get("const"):
            data.config[prop] = prop_def["const"]

    res = abreq(
        "scheduler/sources/check_connection",
        {
            "sourceDefinitionId": data.sourceDefId,
            "connectionConfiguration": data.config,
            "workspaceId": workspace_id,
        },
        timeout=60,
    )
    if "jobInfo" not in res or res.get("status") == "failed":
        failure_reason = res.get("message", "Something went wrong, please check your credentials")
        logger.error("Failed to check the source connection: %s", res)
        raise HttpError(500, failure_reason)
    return res


def check_source_connection_for_update(source_id: str, data: AirbyteSourceUpdateCheckConnection):
    """Test connection on a potential edit on source"""
    res = abreq(
        "sources/check_connection_for_update",
        {
            "sourceId": source_id,
            "connectionConfiguration": data.config,
            "name": data.name,
        },
        timeout=60,
    )
    if "jobInfo" not in res or res.get("status") == "failed":
        failure_reason = res.get("message", "Something went wrong, please check your credentials")
        logger.error("Failed to check the source connection: %s", res)
        raise HttpError(500, failure_reason)
    # {
    #   'status': 'succeeded',
    #   'jobInfo': {
    #     'id': 'ecd78210-5eaa-4a70-89ad-af1d9bc7c7f2',
    #     'configType': 'check_connection_source',
    #     'configId': 'Optional[decd338e-5647-4c0b-adf4-da0e75f5a750]',
    #     'createdAt': 1678891375849,
    #     'endedAt': 1678891403356,
    #     'succeeded': True,
    #     'connectorConfigurationUpdated': False,
    #     'logs': {'logLines': [str]}
    #   }
    # }
    return res


def get_source_schema_catalog(
    workspace_id: str, source_id: str
) -> Dict[str, Any]:  # pylint: disable=unused-argument
    """Fetch source schema catalog for a source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(source_id, str):
        raise HttpError(400, "source_id must be a string")

    res = abreq(
        "sources/discover_schema", {"sourceId": source_id, "disable_cache": True}, timeout=600
    )  # timeout of 10 mins
    # is it not possible that the job is long-running
    # and we need to check its status later?
    if "catalog" not in res and "jobInfo" in res:
        # special handling for errors we know
        if "failureReason" in res["jobInfo"]:
            if (
                res["jobInfo"]["failureReason"]["externalMessage"]
                == "Something went wrong in the connector. See the logs for more details."
            ):
                raise HttpError(
                    400,
                    res["jobInfo"]["failureReason"]["stacktrace"],
                )
            raise HttpError(
                400,
                res["jobInfo"]["failureReason"]["externalMessage"],
            )
        else:
            # for errors unknown to airbyte we might not have "failureReason"
            message = "Failed to discover schema"
            error = message + f" for source: {source_id}"
            if "logs" in res["jobInfo"]:
                error += "\n".join(res["jobInfo"]["logs"]["logLines"])
            logger.error(error)
            raise HttpError(400, message)
    if "catalog" not in res and "jobInfo" not in res:
        raise HttpError(400, res["message"])
    return res


def get_destination_definitions(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch destination definitions for an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("destination_definitions/list_for_workspace", {"workspaceId": workspace_id})
    if "destinationDefinitions" not in res:
        error_message = f"Destination definitions not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_destination_definition(workspace_id: str, destinationdef_id: str) -> Dict[str, Any]:
    """Fetch destination definition for an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "Invalid destination definition ID")

    res = abreq(
        "destination_definitions/get_for_workspace",
        {"destinationDefinitionId": destinationdef_id, "workspaceId": workspace_id},
    )
    if "destinationDefinitionId" not in res:
        error_message = (
            f"Destination definition : {destinationdef_id} not found for workspace: {workspace_id}"
        )
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_destination_definition_specification(
    workspace_id: str, destinationdef_id: str
) -> Dict[str, Any]:
    """Fetch destination definition specification for a destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "Invalid destination definition ID")

    res = abreq(
        "destination_definition_specifications/get",
        {"destinationDefinitionId": destinationdef_id, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        error_message = (
            f"specification not found for destination definition {destinationdef_id} "
            f"in workspace {workspace_id}"
        )
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_destinations(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all destinations in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("destinations/list", {"workspaceId": workspace_id})
    if "destinations" not in res:
        error_message = f"Destinations not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_destination(workspace_id: str, destination_id: str) -> Dict[str, Any]:
    """Fetch a single destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(destination_id, str):
        raise HttpError(400, "Invalid destination ID")

    res = abreq("destinations/get", {"destinationId": destination_id})
    if "destinationId" not in res:
        error_message = f"Destination not found: {destination_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def delete_destination(
    workspace_id: str, destination_id: str  # skipcq PYL-W0613
) -> Dict[str, Any]:  # pylint: disable=unused-argument
    """Delete a destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(destination_id, str):
        raise HttpError(400, "Invalid destination ID")

    res = abreq("destinations/delete", {"destinationId": destination_id})
    return res


def create_destination(
    workspace_id: str, name: str, destinationdef_id: str, config: Dict[str, Any]
) -> Dict[str, Any]:
    """Create a destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(name, str):
        raise HttpError(400, "Invalid name")

    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "Invalid destination definition ID")

    if not isinstance(config, dict):
        raise HttpError(400, "Invalid config")

    res = abreq(
        "destinations/create",
        {
            "workspaceId": workspace_id,
            "destinationDefinitionId": destinationdef_id,
            "connectionConfiguration": config,
            "name": name,
        },
    )
    if "destinationId" not in res:
        error_message = f"Failed to create destination: {name}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res


def update_destination(
    destination_id: str, name: str, config: Dict[str, Any], destinationdef_id: str
) -> Dict[str, Any]:
    """Update a destination in an airbyte workspace"""
    if not isinstance(destination_id, str):
        raise HttpError(400, "Invalid destination ID")

    if not isinstance(name, str):
        raise HttpError(400, "Invalid name")

    if not isinstance(config, dict):
        raise HttpError(400, "Invalid config")

    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "Invalid destination definition ID")

    res = abreq(
        "destinations/update",
        {
            "destinationId": destination_id,
            "destinationDefinitionId": destinationdef_id,
            "connectionConfiguration": config,
            "name": name,
        },
    )
    if "destinationId" not in res:
        error_message = f"Failed to update destination: {destination_id}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res


def check_destination_connection(
    workspace_id: str, data: schema.AirbyteDestinationCreate
) -> Dict[str, Any]:
    """Check connection for a destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq(
        "destinations/check_connection",
        {
            "workspaceId": workspace_id,
            "destinationDefinitionId": data.destinationDefId,
            "connectionConfiguration": data.config,
        },
    )
    return res


def check_destination_connection_for_update(
    destination_id: str, data: schema.AirbyteDestinationUpdateCheckConnection
) -> Dict[str, Any]:
    """Check connection for a destination update in an airbyte workspace"""
    if not isinstance(destination_id, str):
        raise HttpError(400, "Invalid destination ID")

    res = abreq(
        "destinations/check_connection_for_update",
        {
            "destinationId": destination_id,
            "connectionConfiguration": data.config,
        },
    )
    return res


def get_connections(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all connections in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        error_message = f"Connections not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_webbackend_connections(workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all connections in an airbyte workspace using web backend"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("web_backend/connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        error_message = f"Connections not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_connection(workspace_id: str, connection_id: str) -> Dict[str, Any]:
    """Fetch a single connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    res = abreq("connections/get", {"connectionId": connection_id})
    if "connectionId" not in res:
        error_message = f"Connection not found: {connection_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def create_connection(
    workspace_id: str,
    connection_info: schema.AirbyteConnectionCreate,
) -> Dict[str, Any]:
    """Create a connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq(
        "connections/create",
        {
            "workspaceId": workspace_id,
            "sourceId": connection_info.sourceId,
            "destinationId": connection_info.destinationId,
            "name": connection_info.name,
            "namespaceDefinition": connection_info.namespaceDefinition,
            "namespaceFormat": connection_info.namespaceFormat,
            "prefix": connection_info.prefix,
            "schedule": connection_info.schedule,
            "status": connection_info.status,
            "syncCatalog": connection_info.syncCatalog,
        },
    )
    if "connectionId" not in res:
        error_message = f"Failed to create connection: {connection_info.name}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res


def update_connection(
    workspace_id: str,
    connection_info: schema.AirbyteConnectionUpdate,
    current_connection: Dict[str, Any],
) -> Dict[str, Any]:
    """Update a connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq(
        "connections/update",
        {
            "connectionId": connection_info.connectionId,
            "name": connection_info.name,
            "namespaceDefinition": connection_info.namespaceDefinition,
            "namespaceFormat": connection_info.namespaceFormat,
            "prefix": connection_info.prefix,
            "schedule": connection_info.schedule,
            "status": connection_info.status,
            "syncCatalog": connection_info.syncCatalog,
        },
    )
    if "connectionId" not in res:
        error_message = f"Failed to update connection: {connection_info.connectionId}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res


def reset_connection(connection_id: str) -> Dict[str, Any]:
    """Reset a connection in an airbyte workspace"""
    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    res = abreq("connections/reset", {"connectionId": connection_id})
    return res


def delete_connection(workspace_id: str, connection_id: str) -> Dict[str, Any]:
    """Delete a connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    res = abreq("connections/delete", {"connectionId": connection_id})
    return res


def sync_connection(workspace_id: str, connection_id: str) -> Dict[str, Any]:
    """Sync a connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    res = abreq("connections/sync", {"connectionId": connection_id})
    return res


def get_job_info(job_id: str) -> Dict[str, Any]:
    """Fetch job info from airbyte"""
    if not isinstance(job_id, str):
        raise HttpError(400, "Invalid job ID")

    res = abreq("jobs/get", {"id": job_id})
    if "job" not in res:
        error_message = f"Job not found: {job_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_job_info_without_logs(job_id: str) -> Dict[str, Any]:
    """Fetch job info without logs from airbyte"""
    if not isinstance(job_id, str):
        raise HttpError(400, "Invalid job ID")

    res = abreq("jobs/get_without_logs", {"id": job_id})
    if "job" not in res:
        error_message = f"Job not found: {job_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def get_jobs_for_connection(
    connection_id: str, limit: int = 1, offset: int = 0, job_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Fetch jobs for a connection in airbyte"""
    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    if not isinstance(limit, int):
        raise HttpError(400, "Invalid limit")

    if not isinstance(offset, int):
        raise HttpError(400, "Invalid offset")

    if job_types is None:
        job_types = ["sync"]
    elif not isinstance(job_types, list):
        raise HttpError(400, "Invalid job types")

    res = abreq(
        "jobs/list",
        {
            "configId": connection_id,
            "configTypes": job_types,
            "pagination": {"pageSize": limit, "rowOffset": offset},
        },
    )
    if "jobs" not in res:
        error_message = f"Jobs not found for connection: {connection_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def parse_job_info(jobinfo: Dict[str, Any]) -> Dict[str, Any]:
    """Parse job info from airbyte"""
    if not isinstance(jobinfo, dict):
        raise HttpError(400, "Invalid job info")

    job = jobinfo["job"]
    attempts = jobinfo["attempts"]

    if not attempts:
        return {
            "status": job["status"],
            "logs": [],
        }

    latest_attempt = attempts[-1]
    return {
        "status": job["status"],
        "logs": latest_attempt["logs"]["logLines"],
    }


def get_logs_for_job(job_id: int, attempt_number: int = 0) -> List[str]:
    """Fetch logs for a job in airbyte"""
    if not isinstance(job_id, int):
        raise HttpError(400, "Invalid job ID")

    if not isinstance(attempt_number, int):
        raise HttpError(400, "Invalid attempt number")

    res = abreq("jobs/get", {"id": str(job_id)})
    if "attempts" not in res:
        error_message = f"Job not found: {job_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    if not res["attempts"]:
        return []

    if attempt_number >= len(res["attempts"]):
        return []

    return res["attempts"][attempt_number]["logs"]["logLines"]


def get_connection_catalog(connection_id: str, **kwargs) -> Dict[str, Any]:
    """Fetch catalog for a connection in airbyte"""
    if not isinstance(connection_id, str):
        raise HttpError(400, "Invalid connection ID")

    res = abreq("connections/get", {"connectionId": connection_id})
    if "syncCatalog" not in res:
        error_message = f"Catalog not found for connection: {connection_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)

    return res


def update_schema_change(
    org: Org,
    connection_info: schema.AirbyteConnectionSchemaUpdate,
    current_connection: Dict[str, Any],
) -> Dict[str, Any]:
    """Update schema change for a connection in airbyte"""
    if not isinstance(org, Org):
        raise HttpError(400, "Invalid org")

    res = abreq(
        "connections/update",
        {
            "connectionId": connection_info.connectionId,
            "syncCatalog": connection_info.syncCatalog,
        },
    )
    if "connectionId" not in res:
        error_message = f"Failed to update connection: {connection_info.connectionId}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res


def get_current_airbyte_version() -> str:
    """Get current airbyte version"""
    res = abreq("health", method="GET")
    if "version" not in res:
        error_message = "Failed to get airbyte version"
        logger.error(error_message)
        raise HttpError(400, error_message)

    return res["version"]
