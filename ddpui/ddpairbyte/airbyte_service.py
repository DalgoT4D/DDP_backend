"""
Airbyte service module
Functions which communicate with Airbyte
These functions do not access the Dalgo database
"""

from typing import Dict, List
import os
import json
from datetime import datetime
import pytz
import requests
from dotenv import load_dotenv
from ninja.errors import HttpError
from ddpui import settings
from ddpui.ddpairbyte import schema
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.helpers import remove_nested_attribute, nice_bytes
from ddpui.ddpairbyte.schema import (
    AirbyteSourceCreate,
    AirbyteDestinationCreate,
    AirbyteSourceUpdateCheckConnection,
    AirbyteDestinationUpdateCheckConnection,
)


load_dotenv()


logger = CustomLogger("airbyte")


def abreq(endpoint, req=None, **kwargs):
    """Request to the airbyte server"""
    method = kwargs.get("method", "POST")
    if method not in ["GET", "POST"]:
        raise HttpError(500, "method not supported")

    abhost = os.getenv("AIRBYTE_SERVER_HOST")
    abport = os.getenv("AIRBYTE_SERVER_PORT")
    abver = os.getenv("AIRBYTE_SERVER_APIVER")
    token = os.getenv("AIRBYTE_API_TOKEN")

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
        logger.info("here2222")
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


def get_workspaces():
    """Fetch all workspaces in airbyte server"""
    logger.info("Fetching workspaces from Airbyte server")

    res = abreq("workspaces/list")
    if "workspaces" not in res:
        logger.error("No workspaces found")
        raise HttpError(404, "no workspaces found")
    return res


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

    res = abreq(
        "workspaces/create",
        {"name": name, "organizationId": "00000000-0000-0000-0000-000000000000"},
    )
    if "workspaceId" not in res:
        logger.info("Workspace not created: %s", name)
        raise HttpError(400, "workspace not created")
    return res


def delete_workspace(workspace_id: str):
    """Deletes an airbyte workspace"""
    res = abreq("workspaces/delete", {"workspaceId": workspace_id})
    return res


def get_source_definition(workspace_id: str, sourcedef_id: str) -> dict:
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


def get_source_definitions(workspace_id: str) -> List[Dict]:
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


def get_source_definition_specification(workspace_id: str, sourcedef_id: str) -> dict:
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
):
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
        timeout=100,
    )
    if "sourceDefinitionId" not in res:
        error_message = f"Source definition not created: {name}"
        logger.error("Source definition not created: %s", name)
        raise HttpError(400, error_message)
    return res


def get_sources(workspace_id: str) -> List[Dict]:
    """Fetch all sources in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    res = abreq("sources/list", {"workspaceId": workspace_id})
    if "sources" not in res:
        logger.error("Sources not found for workspace: %s", workspace_id)
        raise HttpError(404, "sources not found for workspace")
    return res


def get_source(workspace_id: str, source_id: str) -> dict:
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


def delete_source(workspace_id: str, source_id: str) -> dict:
    """Deletes a source in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "Invalid workspace ID")

    if not isinstance(source_id, str):
        raise HttpError(400, "Invalid source ID")

    res = abreq("sources/delete", {"sourceId": source_id})
    return res


def create_source(workspace_id: str, name: str, sourcedef_id: str, config: dict) -> dict:
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
        error_message = "Failed to create source: " + res.get("message", json.dumps(res))
        logger.error("Failed to create source: %s", error_message)
        raise HttpError(500, error_message)
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
        error_message = "Failed to update source: " + res.get("message", json.dumps(res))
        logger.error("Failed to update source: %s", error_message)
        raise HttpError(500, error_message)
    return res


def check_source_connection(workspace_id: str, data: AirbyteSourceCreate) -> dict:
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
        error_message = "Failed to check the source connection: " + res.get(
            "message", json.dumps(res)
        )
        logger.error("Failed to check the source connection: %s", error_message)
        raise HttpError(500, error_message)
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
        error_message = "Failed to check the source connection: " + res.get(
            "message", json.dumps(res)
        )
        logger.error("Failed to check the source connection: %s", error_message)
        raise HttpError(500, error_message)

    return res


def get_source_schema_catalog(
    workspace_id: str, source_id: str
) -> dict:  # pylint: disable=unused-argument
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
            raise HttpError(400, error)
    if "catalog" not in res and "jobInfo" not in res:
        error_message = "Failed to fetch source schema catalog: " + res.get(
            "message", json.dumps(res)
        )
        logger.error("Failed to fetch source schema catalog: %s", error_message)
        raise HttpError(400, error_message)

    return res


def get_destination_definitions(workspace_id: str) -> dict:
    """Fetch destination definitions in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("destination_definitions/list_for_workspace", {"workspaceId": workspace_id})
    if "destinationDefinitions" not in res:
        logger.error("Destination definitions not found for workspace: %s", workspace_id)
        raise HttpError(404, "destination definitions not found")
    return res


def get_destination_definition(workspace_id: str, destinationdef_id: str) -> dict:
    """get the destination definition"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(destinationdef_id, str):
        raise HttpError(400, "destinationdef_id must be a string")

    res = abreq(
        "destination_definitions/get",
        {"destinationDefinitionId": destinationdef_id},
    )
    if "destinationDefinitionId" not in res:
        logger.error("Destination definition not found for workspace: %s", workspace_id)
        raise HttpError(404, "destination definition not found")
    return res


def get_destination_definition_specification(workspace_id: str, destinationdef_id: str) -> dict:
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
        logger.error("Specification not found for destination definition: %s", destinationdef_id)
        raise HttpError(404, "Failed to get destination definition specification")
    if res["connectionSpecification"]["title"] == "Postgres Destination Spec":
        res["connectionSpecification"]["properties"]["ssl_mode"][
            "title"
        ] = "SSL modes* (select 'disable' if you don't know)"
        res["connectionSpecification"]["properties"]["tunnel_method"][
            "title"
        ] = "SSH Tunnel Method* (select 'No Tunnel' if you don't know)"
    return res


def get_destinations(workspace_id: str) -> dict:
    """Fetch all desintations in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("destinations/list", {"workspaceId": workspace_id})
    if "destinations" not in res:
        logger.error("Destinations not found for workspace: %s", workspace_id)
        raise HttpError(404, "destinations not found for this workspace")
    return res


def get_destination(workspace_id: str, destination_id: str) -> dict:
    """Fetch a destination in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if not isinstance(destination_id, str):
        raise HttpError(400, "destination_id must be a string")

    res = abreq("destinations/get", {"destinationId": destination_id})
    if "destinationId" not in res:
        logger.error("Destination not found: %s", destination_id)
        raise HttpError(404, "destination not found")
    return res


def delete_destination(
    workspace_id: str, destination_id: str  # skipcq PYL-W0613
) -> dict:  # pylint: disable=unused-argument
    """Fetch a destination in an airbyte workspace"""
    res = abreq("destinations/delete", {"destinationId": destination_id})
    return res


def create_destination(workspace_id: str, name: str, destinationdef_id: str, config: dict) -> dict:
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
        timeout=120,
    )
    if "destinationId" not in res:
        error_message = "Failed to create destination: " + res.get("message", json.dumps(res))
        logger.error("Failed to create destination: %s", error_message)
        raise HttpError(500, error_message)
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
        timeout=120,
    )
    if "destinationId" not in res:
        error_message = "Failed to update destination: " + res.get("message", json.dumps(res))
        logger.error("Failed to update destination: %s", error_message)
        raise HttpError(500, error_message)
    return res


def check_destination_connection(workspace_id: str, data: AirbyteDestinationCreate) -> dict:
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
        timeout=120,
    )
    if "jobInfo" not in res or res.get("status") == "failed":
        error_message = "Failed to connect to warehouse: " + res.get("message", json.dumps(res))
        logger.error("Failed to connect to warehouse: %s", error_message)
        raise HttpError(500, error_message)
    return res


def check_destination_connection_for_update(
    destination_id: str, data: AirbyteDestinationUpdateCheckConnection
):
    """Test a potential destination's connection in an airbyte workspace"""
    if not isinstance(destination_id, str):
        raise HttpError(400, "destination_id must be a string")

    res = abreq(
        "destinations/check_connection_for_update",
        {
            "destinationId": destination_id,
            "connectionConfiguration": data.config,
            "name": data.name,
        },
        timeout=120,
    )
    if "jobInfo" not in res or res.get("status") == "failed":
        error_message = "Failed to connect to warehouse: " + res.get("message", json.dumps(res))
        logger.error("Failed to connect to warehouse: %s", error_message)
        raise HttpError(500, error_message)
    return res


def get_connections(workspace_id: str) -> dict:
    """Fetch all connections of an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        error_message = f"connections not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)
    return res


def get_webbackend_connections(workspace_id: str) -> dict:
    """Fetch all connections of an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("web_backend/connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        error_message = f"connections not found for workspace: {workspace_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)
    return res["connections"]


def get_connection(workspace_id: str, connection_id: str) -> dict:
    """Fetch a connection of an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    res = abreq("web_backend/connections/get", {"connectionId": connection_id})
    if "connectionId" not in res:
        error_message = f"Connection not found: {connection_id}"
        logger.error(error_message)
        raise HttpError(404, error_message)
    return res


def create_connection(
    workspace_id: str,
    connection_info: schema.AirbyteConnectionCreate,
) -> dict:
    """Create a connection in an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")

    if len(connection_info.streams) == 0:
        error_message = f"must specify at least one stream workspace_id={workspace_id}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    sourceschemacatalog = connection_info.syncCatalog
    payload = {
        "sourceId": connection_info.sourceId,
        "destinationId": connection_info.destinationId,
        "sourceCatalogId": connection_info.catalogId,
        "syncCatalog": {
            "streams": [
                # we're going to put the stream
                # configs in here in the next step below
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

    # one stream per table
    selected_streams = {x["name"]: x for x in connection_info.streams}
    for schema_cat in sourceschemacatalog["streams"]:
        stream_name = schema_cat["stream"]["name"]
        if stream_name in selected_streams and selected_streams[stream_name]["selected"]:
            schema_cat["config"]["selected"] = True
            schema_cat["config"]["syncMode"] = selected_streams[stream_name]["syncMode"]
            schema_cat["config"]["destinationSyncMode"] = selected_streams[stream_name][
                "destinationSyncMode"
            ]
            # update the cursorField when the mode is incremental
            # weirdly the cursor field is an array of single element eg ["created_on"] or []; same behaviour for pk
            if (
                schema_cat["config"]["syncMode"] == "incremental"
                and schema_cat["config"]["destinationSyncMode"] == "append_dedup"
            ):
                if "primaryKey" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"primaryKey is required for stream '{stream_name}' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'",
                    )

                if "cursorField" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"cursor is required for stream '{stream_name}' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'",
                    )

                schema_cat["config"]["primaryKey"] = [
                    [pk] for pk in selected_streams[stream_name]["primaryKey"]
                ]
                schema_cat["config"]["cursorField"] = [selected_streams[stream_name]["cursorField"]]

            elif schema_cat["config"]["syncMode"] == "incremental":
                if "cursorField" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"cursor is required for stream '{stream_name}' when syncMode is 'incremental'",
                    )
                schema_cat["config"]["cursorField"] = [selected_streams[stream_name]["cursorField"]]

            else:
                schema_cat["config"]["cursorField"] = []
                schema_cat["config"]["primaryKey"] = []

            if (
                "columns" in selected_streams[stream_name]
                and selected_streams[stream_name]["columns"]
            ):
                columns = selected_streams[stream_name]["columns"]
                count = 0

                for column in columns:
                    if column.get("selected", False):
                        count += 1

                if count == len(columns):
                    schema_cat["config"]["fieldSelectionEnabled"] = False
                    schema_cat["config"]["selectedFields"] = []
                else:
                    schema_cat["config"]["fieldSelectionEnabled"] = True
                    schema_cat["config"]["selectedFields"] = []

                    for column in columns:
                        if column.get("selected", False):
                            schema_cat["config"]["selectedFields"].append(
                                {"fieldPath": [column["name"]]}
                            )
            else:
                # No column information or empty columns list - default to all columns selected
                schema_cat["config"]["fieldSelectionEnabled"] = False
                schema_cat["config"]["selectedFields"] = []

            payload["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/create", payload)
    if "connectionId" not in res:
        error_message = "Failed to create connection: " + res.get("message", json.dumps(res))
        logger.error("Failed to create connection: %s", error_message)
        raise HttpError(500, error_message)
    return res


def update_connection(
    workspace_id: str,
    connection_info: schema.AirbyteConnectionUpdate,
    current_connection: dict,
) -> dict:
    """Update a connection of an airbyte workspace"""
    if not isinstance(workspace_id, str):
        raise HttpError(400, "workspace_id must be a string")
    if len(connection_info.streams) == 0:
        error_message = f"must specify at least one stream workspace_id={workspace_id}"
        logger.error(error_message)
        raise HttpError(400, error_message)

    sourceschemacatalog = connection_info.syncCatalog

    # update the name
    if connection_info.name:
        current_connection["name"] = connection_info.name

    # update the destination schema
    if connection_info.destinationSchema:
        current_connection["namespaceDefinition"] = "customformat"
        current_connection["namespaceFormat"] = connection_info.destinationSchema

    current_connection["syncCatalog"]["streams"] = []

    # one stream per table
    selected_streams = {x["name"]: x for x in connection_info.streams}
    for schema_cat in sourceschemacatalog["streams"]:
        stream_name = schema_cat["stream"]["name"]
        if stream_name in selected_streams and selected_streams[stream_name]["selected"]:
            # set schema_cat['config']['syncMode']
            # from schema_cat['stream']['supportedSyncModes'] here
            schema_cat["config"]["selected"] = True
            schema_cat["config"]["syncMode"] = selected_streams[stream_name]["syncMode"]
            schema_cat["config"]["destinationSyncMode"] = selected_streams[stream_name][
                "destinationSyncMode"
            ]
            # update the cursorField when the mode is incremental
            # weirdly the cursor field is an array of single element eg ["created_on"] or []; same behaviour for pk
            if (
                schema_cat["config"]["syncMode"] == "incremental"
                and schema_cat["config"]["destinationSyncMode"] == "append_dedup"
            ):
                if "primaryKey" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"primaryKey is required for stream '{stream_name}' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'",
                    )

                if "cursorField" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"cursor is required for stream '{stream_name}' when syncMode is 'incremental' and destinationSyncMode is 'append_dedup'",
                    )

                schema_cat["config"]["primaryKey"] = [
                    [pk] for pk in selected_streams[stream_name]["primaryKey"]
                ]
                schema_cat["config"]["cursorField"] = [selected_streams[stream_name]["cursorField"]]

            elif schema_cat["config"]["syncMode"] == "incremental":
                if "cursorField" not in selected_streams[stream_name]:
                    raise HttpError(
                        400,
                        f"cursor is required for stream '{stream_name}' when syncMode is 'incremental'",
                    )
                schema_cat["config"]["cursorField"] = [selected_streams[stream_name]["cursorField"]]

            else:
                schema_cat["config"]["cursorField"] = []
                schema_cat["config"]["primaryKey"] = []

            if (
                "columns" in selected_streams[stream_name]
                and selected_streams[stream_name]["columns"]
            ):
                columns = selected_streams[stream_name]["columns"]
                count = 0

                for column in columns:
                    if column.get("selected", False):
                        count += 1

                if count == len(columns):
                    schema_cat["config"]["fieldSelectionEnabled"] = False
                    schema_cat["config"]["selectedFields"] = []
                else:
                    schema_cat["config"]["fieldSelectionEnabled"] = True
                    schema_cat["config"]["selectedFields"] = []

                    for column in columns:
                        if column.get("selected", False):
                            schema_cat["config"]["selectedFields"].append(
                                {"fieldPath": [column["name"]]}
                            )
            else:
                # No column information or empty columns list - default to all columns selected
                schema_cat["config"]["fieldSelectionEnabled"] = False
                schema_cat["config"]["selectedFields"] = []

            current_connection["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/update", current_connection)
    if "connectionId" not in res:
        error_message = "Failed to update connection: " + res.get("message", json.dumps(res))
        logger.error("Failed to update connection: %s", error_message)
        raise HttpError(500, error_message)
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


def get_job_info_without_logs(job_id: str) -> dict:
    """get job info without logs for an airbyte job"""
    if not isinstance(job_id, str):
        raise HttpError(400, "job_id must be a string")

    res = abreq("jobs/get_without_logs", {"id": job_id})
    return res


def get_jobs_for_connection(
    connection_id: str,
    limit: int = 1,
    offset: int = 0,
    job_types: list[str] | None = None,
    created_at_start: datetime = None,
    created_at_end: datetime = None,
) -> int | None:
    """
    returns most recent job for a connection
    possible configTypes are
    - check_connection_source
    - check_connection_destination
    - discover_schema
    - get_spec
    - sync
    - reset_connection

    by default this function fetches the last job
    """
    if not isinstance(connection_id, str):
        raise HttpError(400, "connection_id must be a string")

    if job_types is None:
        job_types = ["sync"]

    payload = {
        "configTypes": job_types,
        "configId": connection_id,
        "pagination": {"rowOffset": offset, "pageSize": limit},
    }

    if created_at_start:
        payload["createdAtStart"] = (
            created_at_start.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )

    if created_at_end:
        payload["createdAtEnd"] = (
            created_at_end.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )

    result = abreq(
        "jobs/list",
        payload,
    )
    return result


def parse_job_info(jobinfo: dict) -> dict:
    """extract summary info from job and successfull attempt"""
    retval = {
        "job_type": jobinfo["job"]["configType"],
        "job_id": jobinfo["job"]["id"],
        "status": jobinfo["job"]["status"],
        "date": None,
        "recordsSynced": 0,
        "bytesSynced": nice_bytes(0),
        "recordsEmitted": 0,
        "bytesEmitted": nice_bytes(0),
        "recordsCommitted": 0,
        "totalTimeInSeconds": 0,
        "resetConfig": jobinfo["job"].get("resetConfig", None),
        "attempt_no": 0,
    }
    for attempt in jobinfo["attempts"]:
        if attempt["status"] == "succeeded":
            retval["attempt_no"] = attempt["id"]
            retval["recordsSynced"] = attempt.get("recordsSynced", 0)
            retval["bytesSynced"] = nice_bytes(attempt.get("bytesSynced", 0))
            retval["recordsEmitted"] = attempt["totalStats"]["recordsEmitted"]
            retval["bytesEmitted"] = nice_bytes(attempt["totalStats"]["bytesEmitted"])
            retval["recordsCommitted"] = attempt["totalStats"]["recordsCommitted"]
            retval["totalTimeInSeconds"] = attempt["endedAt"] - attempt["createdAt"]
            retval["date"] = datetime.fromtimestamp(attempt["endedAt"]).date()
            break
    return retval


def get_logs_for_job(job_id: int, attempt_number: int = 0) -> list[str]:
    """get logs for an airbyte job. do not make an API for this! returns an array of log messages in correct order"""
    if not isinstance(job_id, int):
        raise HttpError(400, "job_id must be an integer")

    res = abreq(
        "attempt/get_for_job",
        {"jobId": job_id, "attemptNumber": attempt_number},
    )

    # logType field is present in the newer version of airbyte (1.4.1)
    # the old logs come as logType == formatted
    # the new logs will comes as logType == structured; as events
    if "logType" not in res or res["logType"] == "formatted":
        return res["logs"]["logLines"]

    if res["logType"] == "structured":
        return [x["message"] for x in res["logs"]["events"]]

    return []


def cancel_job(job_id: str) -> dict:
    """cancel a job"""
    res = abreq("jobs/cancel", {"id": job_id})
    return res


def get_connection_catalog(connection_id: str, **kwargs) -> dict:
    """get the catalog for a connection to check/refresh for schema changes"""
    if not isinstance(connection_id, str):
        raise HttpError(400, "connection_id must be a string")
    res = abreq(
        "web_backend/connections/get",
        {"connectionId": connection_id, "withRefreshedCatalog": True},
        **kwargs,
    )
    return res


def get_current_airbyte_version():
    """Fetch airbyte version"""
    res = abreq("instance_configuration", method="GET")
    print(res, "AIRBYTE RESPONSE")
    if "version" not in res:
        logger.error("No version found")
        return None
    return res["version"]


def cancel_connection_job(
    workspace_id: str, connection_id: str, job_type: str  # pylint: disable=unused-argument
) -> dict:
    """
    cancel a connection job. job_type is one of the following:
    - check_connection_destination
    - discover_schema
    - get_spec
    - sync
    - reset_connection
    - refresh
    - clear
    """
    cancelled = False
    res = get_jobs_for_connection(connection_id, job_types=[job_type])
    for job in res["jobs"]:
        if job["job"]["status"] == "running":
            job_id = job["job"]["id"]
            cancel_job(job_id)
            logger.info("Cancelled job: %s", job_id)
            cancelled = True
            break

    return {"cancelled": cancelled}
