import os
import requests
from dotenv import load_dotenv
from ddpui.ddpairbyte import schema
from ddpui.utils.ab_logger import logger

load_dotenv()


def abreq(endpoint, req=None):
    """Request to the airbyte server"""
    abhost = os.getenv("AIRBYTE_SERVER_HOST")
    abport = os.getenv("AIRBYTE_SERVER_PORT")
    abver = os.getenv("AIRBYTE_SERVER_APIVER")
    token = os.getenv("AIRBYTE_API_TOKEN")

    logger.info("Making request to Airbyte server: %s", endpoint)

    res = requests.post(
        f"http://{abhost}:{abport}/api/{abver}/{endpoint}",
        headers={"Authorization": f"Basic {token}"},
        json=req,
        timeout=30
    )
    logger.info("Response from Airbyte server: %s", res.text)
    res.raise_for_status()
    if "application/json" in res.headers.get("Content-Type", ""):
        return res.json()
    return {}


def get_workspaces():
    """Fetch all workspaces in airbyte server"""
    return abreq("workspaces/list")


def get_workspace(workspace_id):
    """Fetch a workspace in airbyte server"""
    return abreq("workspaces/get", {"workspaceId": workspace_id})


def set_workspace_name(workspace_id, name):
    """Set workspace name in airbyte server"""
    abreq("workspaces/update_name", {"workspaceId": workspace_id, "name": name})


def create_workspace(name):
    """Create a workspace in airbyte server"""
    res = abreq("workspaces/create", {"name": name})
    if "workspaceId" not in res:
        raise Exception(res)
    return res


def get_source_definitions(workspace_id, **kwargs):
    """Fetch source definitions for an airbyte workspace"""
    res = abreq("source_definitions/list_for_workspace", {"workspaceId": workspace_id})
    if "sourceDefinitions" not in res:
        raise Exception(res)
    return res["sourceDefinitions"]


def get_source_definition_specification(workspace_id, sourcedef_id):
    """Fetch source definition specification for a source in an airbyte workspace"""
    res = abreq(
        "source_definition_specifications/get",
        {"sourceDefinitionId": sourcedef_id, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        raise Exception(res)
    return res["connectionSpecification"]


def get_sources(workspace_id):
    """Fetch all sources in an airbyte workspace"""
    res = abreq("sources/list", {"workspaceId": workspace_id})
    if "sources" not in res:
        raise Exception(res)
    return res["sources"]


def get_source(workspace_id, source_id):
    """Fetch a source in an airbyte workspace"""
    res = abreq("sources/get", {"sourceId": source_id})
    if "sourceId" not in res:
        raise Exception(res)
    return res


def create_source(workspace_id, name, sourcedef_id, config):
    """Create source in an airbyte workspace"""

    # validate against the source schema, type casting where necessary
    connection_specification = get_source_definition_specification(workspace_id, sourcedef_id)['properties']
    for parameter_key in config:
        parameter_desc = connection_specification.get(parameter_key)

        if parameter_desc:
            parameter_type = parameter_desc['type']

            if parameter_type == 'string':
                config[parameter_key] = str(config[parameter_key])

            elif parameter_type == 'integer':
                config[parameter_key] = int(config[parameter_key])

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
        raise Exception("Failed to create source")
    return res


def update_source(source_id, name=None, config=None):
    """Update source in an airbyte workspace"""
    data = {"sourceId": source_id}
    if name is not None:
        data["name"] = name
    if config is not None:
        data["connectionConfiguration"] = config
    res = abreq("sources/update", data)
    if "sourceId" not in res:
        raise Exception("Failed to update source")
    return res


def check_source_connection(workspace_id, source_id):
    """Test a source connection in an airbyte workspace"""
    res = abreq("sources/check_connection", {"sourceId": source_id})
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


def get_source_schema_catalog(workspace_id, source_id):
    """Fetch source schema catalog for a source in an airbyte workspace"""
    res = abreq("sources/discover_schema", {"sourceId": source_id})
    if "catalog" not in res:
        raise Exception("Failed to get source schema catalogs")
    return res


def get_destination_definitions(workspace_id, **kwargs):
    """Fetch destination definitions in an airbyte workspace"""
    res = abreq(
        "destination_definitions/list_for_workspace", {"workspaceId": workspace_id}
    )
    if "destinationDefinitions" not in res:
        raise Exception("Failed to get destination definitions")
    return res["destinationDefinitions"]


def get_destination_definition_specification(workspace_id, destinationdef_id):
    """Fetch destination definition specification for a destination in a workspace"""
    res = abreq(
        "destination_definition_specifications/get",
        {"destinationDefinitionId": destinationdef_id, "workspaceId": workspace_id},
    )
    if "connectionSpecification" not in res:
        raise Exception("Failed to get destination definition specification")
    return res["connectionSpecification"]


def get_destinations(workspace_id):
    """Fetch all desintations in an airbyte workspace"""
    res = abreq("destinations/list", {"workspaceId": workspace_id})
    if "destinations" not in res:
        raise Exception("Failed to get destinations")
    return res["destinations"]


def get_destination(workspace_id, destination_id):
    """Fetch a destination in an airbyte workspace"""
    res = abreq("destinations/get", {"destinationId": destination_id})
    if "destinationId" not in res:
        raise Exception("Failed to get destination")
    return res


def create_destination(workspace_id, name, destinationdef_id, config):
    """Create destination in an airbyte workspace"""

    # validate against the destination schema, type casting where necessary
    connection_specification = get_destination_definition_specification(workspace_id, destinationdef_id)['properties']
    for parameter_key in config:
        parameter_desc = connection_specification.get(parameter_key)

        if parameter_desc:
            parameter_type = parameter_desc['type']

            if parameter_type == 'string':
                config[parameter_key] = str(config[parameter_key])

            elif parameter_type == 'integer':
                config[parameter_key] = int(config[parameter_key])

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
        raise Exception(res)
    return res


def update_destination(destination_id, name, config, destination_def_id):
    """Update a destination in an airbyte workspace"""
    res = abreq(
        "destinations/update",
        {
            "destinationId": destination_id,
            "name": name,
            "connectionConfiguration": config,
            "destinationDefinitionId": destination_def_id,
        },
    )
    if "destinationId" not in res:
        raise Exception(f"Failed to update destination {destination_id}: {res}")
    return res


def check_destination_connection(workspace_id, destination_id):
    """Test connection to a destination in an airbyte workspace"""
    res = abreq("destinations/check_connection", {"destinationId": destination_id})
    return res


def get_connections(workspace_id):
    """Fetch all connections of an airbyte workspace"""
    res = abreq("connections/list", {"workspaceId": workspace_id})
    if "connections" not in res:
        raise Exception(res)
    return res["connections"]


def get_connection(workspace_id, connection_id):
    """Fetch a connection of an airbyte workspace"""
    res = abreq("connections/get", {"connectionId": connection_id})
    if "connectionId" not in res:
        raise Exception(res)
    return res


def create_connection(workspace_id, connection_info: schema.AirbyteConnectionCreate):
    """Create a connection in an airbyte workspace"""
    if len(connection_info.streamNames) == 0:
        raise Exception("must specify stream names")

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
    for schema_cat in sourceschemacatalog["catalog"]["streams"]:
        if schema_cat["stream"]["name"] in connection_info.streamNames:
            # set schema_cat['config']['syncMode'] from schema_cat['stream']['supportedSyncModes'] here
            payload["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/create", payload)
    if "connectionId" not in res:
        raise Exception(res)
    return res


def update_connection(
    workspace_id, connection_id, connection_info: schema.AirbyteConnectionUpdate
):
    """Update a connection of an airbyte workspace"""
    if len(connection_info.streamnames) == 0:
        raise Exception("must specify stream names")

    sourceschemacatalog = get_source_schema_catalog(
        workspace_id, connection_info.source_id
    )

    payload = {
        "connectionId": connection_id,
        "sourceId": connection_info.source_id,
        "destinationId": connection_info.destination_id,
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
    for schema_cat in sourceschemacatalog["catalog"]["streams"]:
        if schema_cat["stream"]["name"] in connection_info.streamnames:
            # set schema_cat['config']['syncMode'] from schema_cat['stream']['supportedSyncModes'] here
            payload["syncCatalog"]["streams"].append(schema_cat)

    res = abreq("connections/update", payload)
    if "connectionId" not in res:
        raise Exception(res)
    return res


def delete_connection(workspace_id, connection_id):
    """Delete a connection of an airbyte workspace"""
    res = abreq("connections/delete", {"connectionId": connection_id})
    return res


def sync_connection(workspace_id, connection_id):
    """Sync a connection in an airbyte workspace"""
    res = abreq("connections/sync", {"connectionId": connection_id})
    return res
