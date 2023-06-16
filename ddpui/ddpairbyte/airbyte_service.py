import os
import json
from typing import Dict, List
import requests
from dotenv import load_dotenv
from ninja.errors import HttpError
from ddpui.ddpairbyte import schema
from ddpui.utils.ab_logger import logger
from ddpui.utils.helpers import remove_nested_attribute
from ddpui.ddpairbyte.schema import AirbyteSourceCreate, AirbyteDestinationCreate

load_dotenv()


class AirbyteError(Exception):
    """exception class with two strings"""

    def __init__(self, detail: str, errors: str):
        super().__init__(detail)
        self.errors = errors


def abreq(endpoint, req=None):
    """Request to the airbyte server"""

    abhost = os.getenv("AIRBYTE_SERVER_HOST")
    abport = os.getenv("AIRBYTE_SERVER_PORT")
    abver = os.getenv("AIRBYTE_SERVER_APIVER")
    token = os.getenv("AIRBYTE_API_TOKEN")

    logger.info("Making request to Airbyte server: %s", endpoint)

    try:
        res = requests.post(
            f"http://{abhost}:{abport}/api/{abver}/{endpoint}",
            headers={"Authorization": f"Basic {token}"},
            json=req,
            timeout=30,
        )
    except requests.exceptions.ConnectionError as e:
        logger.exception(e)
        raise HttpError(500, "Error connecting to Airbyte server")

    try:
        result_obj = remove_nested_attribute(res.json(), "icon")
        logger.info("Response from Airbyte server:")
        logger.info(json.dumps(result_obj, indent=2))
    except ValueError:
        logger.info("Response from Airbyte server: %s", res.text)

    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HttpError(res.status_code, f"Something went wrong: {error.args}")

    if "application/json" in res.headers.get("Content-Type", ""):
        return res.json()
    return {}


def get_workspaces():
    """Fetch all workspaces in airbyte server"""

    logger.info("Fetching workspaces from Airbyte server")

    res = abreq("workspaces/list")
    if "workspaces" not in res:
        logger.info("No workspaces found")
        raise HttpError(404, "no workspaces found")
    return res["workspaces"]


def get_workspace(workspace_id: str) -> dict:
    """Fetch a workspace from the airbyte server"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("workspaces/get", {"workspaceId": workspace_id})
    if "workspaceId" not in res:
        logger.info("Workspace not found: %s", workspace_id)
        raise HttpError(404, "workspace not found")
    return res


def set_workspace_name(workspace_id: str, name: str) -> dict:
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


def create_workspace(name: str) -> dict:
    """Create a workspace in the airbyte server"""

    if not isinstance(name, str):
        raise HttpError(400, "Name must be a string")

    res = abreq("workspaces/create", {"name": name})
    if "workspaceId" not in res:
        logger.info("Workspace not created: %s", name)
        raise HttpError(400, "workspace not created")
    return res


def get_source_definitions(workspace_id: str, **kwargs) -> List[Dict]:
    """Fetch source definitions for an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("source_definitions/list_for_workspace", {"workspaceId": workspace_id})
    if "sourceDefinitions" not in res:
        logger.info("Source definitions not found for workspace: %s", workspace_id)
        raise HttpError(404, "source definitions not found for workspace")
    return res["sourceDefinitions"]


def get_source_definition_specification(workspace_id: str, sourcedef_id: str) -> dict:
    """Fetch source definition specification for a source in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(sourcedef_id, str):
        raise HttpError(400, f"Invalid source definition ID")

    res = abreq(
        "source_definition_specifications/get",
        {"sourceDefinitionId": sourcedef_id, "workspaceId": workspace_id},
    )

    if "connectionSpecification" not in res:
        logger.info("Specification not found for source definition: %s", sourcedef_id)
        raise HttpError(404, "specification not found for source definition")
    return res["connectionSpecification"]


def get_sources(workspace_id: str) -> List[Dict]:
    """Fetch all sources in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("sources/list", {"workspaceId": workspace_id})
    if "sources" not in res:
        logger.info("Sources not found for workspace: %s", workspace_id)
        raise HttpError(404, "sources not found for workspace")
    return res["sources"]


def get_source(workspace_id: str, source_id: str) -> dict:
    """Fetch a source in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(source_id, str):
        raise HttpError(400, f"Invalid source ID")

    res = abreq("sources/get", {"sourceId": source_id})
    if "sourceId" not in res:
        logger.info("Source not found: %s", source_id)
        raise HttpError(404, "source not found")
    return res


def delete_source(workspace_id: str, source_id: str) -> dict:
    """Deletes a source in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(source_id, str):
        raise HttpError(400, f"Invalid source ID")

    res = abreq("sources/delete", {"sourceId": source_id})
    if "sourceId" not in res:
        logger.info("Source not found: %s", source_id)
        raise HttpError(404, "source not found")
    return res


def create_source(
    workspace_id: str, name: str, sourcedef_id: str, config: dict
) -> dict:
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
        "sources/create",
        {
            "workspaceId": workspace_id,
            "name": name,
            "sourceDefinitionId": sourcedef_id,
            "connectionConfiguration": config,
        },
    )
    if "sourceId" not in res:
        logger.info("Failed to create source: %s", res)
        raise HttpError(500, "failed to create source")
    return res


def update_source(source_id: str, name: str, config: dict, sourcedef_id: str) -> dict:
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
        logger.info("Failed to update source: %s", res)
        raise HttpError(500, "failed to update source")
    return res


def check_source_connection(workspace_id: str, data: AirbyteSourceCreate) -> dict:
    """Test a potential source's connection in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq(
        "scheduler/sources/check_connection",
        {
            "sourceDefinitionId": data.sourceDefId,
            "connectionConfiguration": data.config,
            "workspaceId": workspace_id,
        },
    )
    if "jobInfo" not in res:
        logger.info("Failed to check source connection: %s", res)
        raise HttpError(500, "failed to check source connection")
    return res


def get_source_schema_catalog(workspace_id: str, source_id: str) -> dict:
    """Fetch source schema catalog for a source in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(source_id, str):
        raise HttpError(400, "source_id must be a string")

    res = abreq("sources/discover_schema", {"sourceId": source_id})
    if "catalog" not in res and "jobInfo" in res:
        raise AirbyteError(
            "Failed to get source schema catalogs",
            res["jobInfo"]["failureReason"]["externalMessage"],
        )
    if "catalog" not in res and "jobInfo" not in res:
        raise AirbyteError("Failed to get source schema catalogs", res["message"])
    return res


def get_destination_definitions(workspace_id: str, **kwargs) -> dict:
    """Fetch destination definitions in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq(
        "destination_definitions/list_for_workspace", {"workspaceId": workspace_id}
    )
    if "destinationDefinitions" not in res:
        logger.info("Destination definitions not found for workspace: %s", workspace_id)
        raise HttpError(404, "destination definitions not found")
    return res["destinationDefinitions"]


def get_destination_definition_specification(
    workspace_id: str, destinationdef_id: str
) -> dict:
    """Fetch destination definition specification for a destination in a workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "destinationdef_id must be a string")

    res = abreq(
        "destination_definition_specifications/get",
        {"destinationDefinitionId": destinationdef_id, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        logger.info(
            "Specification not found for destination definition: %s", destinationdef_id
        )
        raise HttpError(404, "Failed to get destination definition specification")
    return res["connectionSpecification"]


def get_destinations(workspace_id: str) -> dict:
    """Fetch all desintations in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("destinations/list", {"workspaceId": workspace_id})
    if "destinations" not in res:
        logger.info("Destinations not found for workspace: %s", workspace_id)
        raise HttpError(404, "destinations not found for this workspace")
    return res["destinations"]


def get_destination(workspace_id: str, destination_id: str) -> dict:
    """Fetch a destination in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(destination_id, str):
        raise HttpError(400, "destination_id must be a string")

    res = abreq("destinations/get", {"destinationId": destination_id})
    if "destinationId" not in res:
        logger.info("Destination not found: %s", destination_id)
        raise HttpError(404, "destination not found")
    return res


def create_destination(
    workspace_id: str, name: str, destinationdef_id: str, config: dict
) -> dict:
    """Create destination in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(name, str):
        raise HttpError(400, "name must be a string")
    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "destinationdef_id must be a string")
    if not isinstance(config, dict):
        raise HttpError(400, "config must be a dict")

    res = abreq(
        "destinations/create",
        {
            "workspaceId": workspace_id,
            "name": name,
            "destinationDefinitionId": destinationdef_id,
            "connectionConfiguration": config,
        },
    )
    if "destinationId" not in res:
        logger.info("Failed to create destination: %s", res)
        raise HttpError(500, "failed to create destination")
    return res


def update_destination(
    destination_id: str, name: str, config: dict, destinationdef_id: str
) -> dict:
    """Update a destination in an airbyte workspace"""

    if not isinstance(destination_id, str):
        raise HttpError(400, "destination_id must be a string")
    if not isinstance(name, str):
        raise HttpError(400, "name must be a string")
    if not isinstance(config, dict):
        raise HttpError(400, "config must be a dict")
    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "destinationdef_id must be a string")

    res = abreq(
        "destinations/update",
        {
            "destinationId": destination_id,
            "name": name,
            "connectionConfiguration": config,
            "destinationDefinitionId": destinationdef_id,
        },
    )
    if "destinationId" not in res:
        logger.info("Failed to update destination: %s", res)
        raise HttpError(500, "failed to update destination")
    return res


def check_destination_connection(
    workspace_id: str, data: AirbyteDestinationCreate
) -> dict:
    """Test a potential destination's connection in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq(
        "scheduler/destinations/check_connection",
        {
            "destinationDefinitionId": data.destinationDefId,
            "connectionConfiguration": data.config,
            "workspaceId": workspace_id,
        },
    )
    return res


def get_connections(workspace_id: str) -> dict:
    """Fetch all connections of an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        logger.info("Connections not found for workspace: %s", workspace_id)
        raise HttpError(404, "connections not found for workspace: %s", workspace_id)
    return res["connections"]


def get_connection(workspace_id: str, connection_id: str) -> dict:
    """Fetch a connection of an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("connections/get", {"connectionId": connection_id})
    if "connectionId" not in res:
        logger.info("Connection not found: %s", connection_id)
        raise HttpError(404, "connection not found: %s", connection_id)
    return res


def create_normalization_operation(workspace_id: str) -> str:
    """create a normalization operation for this airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    # create normalization operation
    logger.info("creating normalization operation")
    response = abreq(
        "/operations/create",
        {
            "workspaceId": workspace_id,
            "name": "op-normalize",
            "operatorConfiguration": {
                "operatorType": "normalization",
                "normalization": {"option": "basic"},
            },
        },
    )
    return response["operationId"]


def create_connection(
    workspace_id: str,
    airbyte_norm_op_id: str,
    connection_info: schema.AirbyteConnectionCreate,
) -> dict:
    """Create a connection in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(airbyte_norm_op_id, str):
        raise HttpError(400, "airbyte_norm_op_id must be a string")

    if len(connection_info.streams) == 0:
        logger.info("must specify at least one stream")
        raise HttpError(400, "must specify at least one stream")

    sourceschemacatalog = get_source_schema_catalog(
        workspace_id, connection_info.sourceId
    )
    payload = {
        "sourceId": connection_info.sourceId,
        "destinationId": connection_info.destinationId,
        "sourceCatalogId": sourceschemacatalog["catalogId"],
        "syncCatalog": {
            "streams": [
                # <== we're going to put the stream configs in here in the next step below
            ]
        },
        "status": "active",
        "prefix": "",
        "namespaceDefinition": "destination",
        "namespaceFormat": "${SOURCE_NAMESPACE}",
        "nonBreakingChangesPreference": "ignore",
        "scheduleType": "manual",
        "geography": "auto",
        "name": connection_info.name,
    }
    if connection_info.destinationSchema:
        payload["namespaceDefinition"] = "customformat"
        payload["namespaceFormat"] = connection_info.destinationSchema

    if connection_info.normalize:
        payload["operationIds"] = [airbyte_norm_op_id]

    # one stream per table
    selected_streams = {x["name"]: x for x in connection_info.streams}
    for schema_cat in sourceschemacatalog["catalog"]["streams"]:
        stream_name = schema_cat["stream"]["name"]
        if (
            stream_name in selected_streams
            and selected_streams[stream_name]["selected"]
        ):
            # set schema_cat['config']['syncMode'] from schema_cat['stream']['supportedSyncModes'] here
            schema_cat["config"]["syncMode"] = selected_streams[stream_name]["syncMode"]
            schema_cat["config"]["destinationSyncMode"] = selected_streams[stream_name][
                "destinationSyncMode"
            ]
            payload["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/create", payload)
    if "connectionId" not in res:
        logger.info("Failed to create connection: %s", res)
        raise HttpError(500, "failed to create connection: %s", res)
    return res


def update_connection(
    workspace_id: str,
    connection_id: str,
    connection_info: schema.AirbyteConnectionUpdate,
) -> dict:
    """Update a connection of an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(connection_id, str):
        raise HttpError(400, "connection_id must be a string")
    if len(connection_info.streams) == 0:
        logger.info("must specify at least one stream")
        raise HttpError(400, "must specify at least one stream")

    sourceschemacatalog = get_source_schema_catalog(
        workspace_id, connection_info.sourceId
    )

    payload = {
        "connectionId": connection_id,
        "sourceId": connection_info.sourceId,
        "destinationId": connection_info.destinationId,
        "sourceCatalogId": sourceschemacatalog["catalogId"],
        "syncCatalog": {
            "streams": [
                # <== we're going to put the stream configs in here in the next step below
            ]
        },
        "prefix": "",
        "namespaceDefinition": "destination",
        "namespaceFormat": "${SOURCE_NAMESPACE}",
        "nonBreakingChangesPreference": "ignore",
        "scheduleType": "manual",
        "geography": "auto",
        "name": connection_info.name,
        "operations": [
            {
                "name": "Normalization",
                "workspaceId": workspace_id,
                "operatorConfiguration": {
                    "operatorType": "normalization",
                    "normalization": {"option": "basic"},
                },
            }
        ],
    }

    # one stream per table
    selected_streams = [{x["name"]: x} for x in connection_info.streams]
    for schema_cat in sourceschemacatalog["catalog"]["streams"]:
        stream_name = schema_cat["stream"]["name"]
        if (
            stream_name in selected_streams
            and selected_streams[stream_name]["selected"]
        ):
            # set schema_cat['config']['syncMode'] from schema_cat['stream']['supportedSyncModes'] here
            schema_cat["config"]["syncMode"] = (
                "incremental"
                if selected_streams[stream_name]["incremental"]
                else "full_refresh"
            )
            schema_cat["config"]["destinationSyncMode"] = selected_streams[stream_name][
                "destinationSyncMode"
            ]
            payload["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/update", payload)
    if "connectionId" not in res:
        logger.info("Failed to update connection: %s", res)
        raise HttpError(500, "failed to update connection: %s", res)
    return res


def delete_connection(workspace_id: str, connection_id: str) -> dict:
    """Delete a connection of an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(connection_id, str):
        raise HttpError(400, "connection_id must be a string")

    res = abreq("connections/delete", {"connectionId": connection_id})
    logger.info("Deleting connection: %s", connection_id)
    return res


def sync_connection(workspace_id: str, connection_id: str) -> dict:
    """Sync a connection in an airbyte workspace"""

    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(connection_id, str):
        raise HttpError(400, "connection_id must be a string")

    res = abreq("connections/sync", {"connectionId": connection_id})
    logger.info("Syncing connection: %s", connection_id)
    return res


def get_job_info(job_id: str) -> dict:
    """get debug info for an airbyte job"""

    if not isinstance(job_id, str):
        raise HttpError(400, "job_id must be a string")

    res = abreq("jobs/get_debug_info", {"id": job_id})
    return res
