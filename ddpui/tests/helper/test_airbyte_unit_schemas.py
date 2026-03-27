# pylint: disable=missing-class-docstring
from typing import List
from ninja import Schema
from pydantic import RootModel, ConfigDict


class StorageConfig(Schema):
    storage: str


class ConnectionConfiguration(Schema):
    url: str
    format: str
    provider: StorageConfig
    dataset_name: str


class CreateSourceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    sourceDefinitionId: str
    connectionConfiguration: ConnectionConfiguration
    workspaceId: str
    name: str
    sourceId: str
    sourceName: str


class CreateSourceTestPayload(Schema):
    sourcedef_id: str
    config: ConnectionConfiguration
    workspace_id: str
    name: str


class GetWorkspaceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    workspaceId: str
    customerId: str
    name: str
    slug: str


class GetSourceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    sourceDefinitionId: str
    connectionConfiguration: ConnectionConfiguration
    workspaceId: str
    name: str
    sourceId: str
    sourceName: str


class GetSourceDefinitions(Schema):
    sourceDefinitionId: str
    name: str


class GetSourceDefinitionsTestResponse(RootModel[List[GetSourceDefinitions]]):
    pass


class GetSourceSchemaCatalogTestResponse(Schema):
    catalog: dict


class GetWorkspacesTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    workspaces: List[GetWorkspaceTestResponse]


class SetWorkspaceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    workspaceId: str
    customerId: str
    name: str
    slug: str


class CreateWorkspaceTestPayload(Schema):
    name: str


class CreateWorkspaceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    name: str
    workspaceId: str


class GetSourcesTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    sources: List[GetSourceTestResponse]


class User(Schema):
    type: str


class ConnectionSpecification(Schema):
    user: User


class GetSourceDefinitionSpecificationTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    connectionSpecification: List[ConnectionSpecification]


class DestinationConfiguration(Schema):
    host: str
    port: int
    database: str
    username: str


class CreateDestinationTestPayload(Schema):
    model_config = ConfigDict(extra="forbid")

    destinationdef_id: str
    config: DestinationConfiguration
    name: str
    workspace_id: str


class CreateDestinationTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    destinationDefinitionId: str
    destinationId: str
    workspaceId: str
    connectionConfiguration: DestinationConfiguration
    name: str
    destinationName: str


class GetDestinationConfiguration(Schema):
    host: str
    port: int
    database: str
    username: str


class GetDestinationTestResponse(Schema):
    model_config = ConfigDict(extra="forbid")

    destinationDefinitionId: str
    destinationId: str
    workspaceId: str
    connectionConfiguration: GetDestinationConfiguration
    name: str
    destinationName: str
    icon: str


class UpdateSourceTestPayload(Schema):
    source_id: str
    sourcedef_id: str
    config: ConnectionConfiguration
    name: str


class UpdateSourceTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    sourceId: str
    name: str
    connectionConfiguration: ConnectionConfiguration
    sourceDefinitionId: str
    sourceName: str
    workspaceId: str


class UpdateDestinationTestPayload(Schema):
    destination_id: str
    config: DestinationConfiguration
    name: str


class UpdateDestinationTestResponse(Schema):
    model_config = ConfigDict(extra="forbid")

    destinationId: str
    name: str
    connectionConfiguration: DestinationConfiguration
    destinationDefinitionId: str
    destinationName: str
    workspaceId: str
    icon: str


class GetConnectionTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    connectionId: str
    name: str
    sourceId: str
    destinationId: str


# class CreateConnectionTestPayload(Schema)


class CreateConnectionTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    connectionId: str
    name: str
    sourceId: str
    destinationId: str


class UpdateConnectionTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    connectionId: str
    name: str
    sourceId: str
    destinationId: str


class CheckSourceConnectionTestResponse(Schema):
    model_config = ConfigDict(extra="allow")

    jobInfo: dict


class CheckDestinationConnectionTestResponse(Schema):
    model_config = ConfigDict(extra="allow")
