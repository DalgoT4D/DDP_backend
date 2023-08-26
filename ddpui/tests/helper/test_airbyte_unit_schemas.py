# pylint: disable=missing-class-docstring
from typing import List
from ninja import Schema


class StorageConfig(Schema):
    storage: str


class ConnectionConfiguration(Schema):
    url: str
    format: str
    provider: StorageConfig
    dataset_name: str


class CreateSourceTestResponse(Schema):
    sourceDefinitionId: str
    connectionConfiguration: ConnectionConfiguration
    workspaceId: str
    name: str
    sourceId: str
    sourceName: str

    class Config:
        extra = "allow"


class CreateSourceTestPayload(Schema):
    sourcedef_id: str
    config: ConnectionConfiguration
    workspace_id: str
    name: str


class GetWorkspaceTestResponse(Schema):
    workspaceId: str
    customerId: str
    name: str
    slug: str

    class Config:
        extra = "allow"


class GetSourceTestResponse(Schema):
    sourceDefinitionId: str
    connectionConfiguration: ConnectionConfiguration
    workspaceId: str
    name: str
    sourceId: str
    sourceName: str

    class Config:
        extra = "allow"


class GetSourceDefinitions(Schema):
    sourceDefinitionId: str
    name: str


class GetSourceDefinitionsTestResponse(Schema):
    __root__: List[GetSourceDefinitions]

    class Config:
        extra = "allow"


class GetSourceSchemaCatalogTestResponse(Schema):
    catalog: dict


class GetWorkspacesTestResponse(Schema):
    workspaces: List[GetWorkspaceTestResponse]

    class Config:
        extra = "allow"


class SetWorkspaceTestResponse(Schema):
    workspaceId: str
    customerId: str
    name: str
    slug: str

    class Config:
        extra = "allow"


class CreateWorkspaceTestPayload(Schema):
    name: str


class CreateWorkspaceTestResponse(Schema):
    name: str
    workspaceId: str

    class Config:
        extra = "allow"


class GetSourcesTestResponse(Schema):
    sources: List[GetSourceTestResponse]

    class Config:
        extra = "allow"


class User(Schema):
    type: str


class ConnectionSpecification(Schema):
    user: User


class GetSourceDefinitionSpecificationTestResponse(Schema):
    connectionSpecification: List[ConnectionSpecification]

    class Config:
        extra = "allow"


class DestinationConfiguration(Schema):
    host: str
    port: int
    database: str
    username: str


class CreateDestinationTestPayload(Schema):
    destinationdef_id: str
    config: DestinationConfiguration
    name: str
    workspace_id: str

    class Config:
        extra = "forbid"


class CreateDestinationTestResponse(Schema):
    destinationDefinitionId: str
    destinationId: str
    workspaceId: str
    connectionConfiguration: DestinationConfiguration
    name: str
    destinationName: str

    class Config:
        extra = "allow"


class GetDestinationConfiguration(Schema):
    host: str
    port: int
    database: str
    username: str


class GetDestinationTestResponse(Schema):
    destinationDefinitionId: str
    destinationId: str
    workspaceId: str
    connectionConfiguration: GetDestinationConfiguration
    name: str
    destinationName: str
    icon: str

    class Config:
        extra = "forbid"


class UpdateSourceTestPayload(Schema):
    source_id: str
    sourcedef_id: str
    config: ConnectionConfiguration
    name: str


class UpdateSourceTestResponse(Schema):
    sourceId: str
    name: str
    connectionConfiguration: ConnectionConfiguration
    sourceDefinitionId: str
    sourceName: str
    workspaceId: str

    class Config:
        extra = "allow"


class UpdateDestinationTestPayload(Schema):
    destination_id: str
    config: DestinationConfiguration
    name: str


class UpdateDestinationTestResponse(Schema):
    destinationId: str
    name: str
    connectionConfiguration: DestinationConfiguration
    destinationDefinitionId: str
    destinationName: str
    workspaceId: str
    icon: str

    class Config:
        extra = "forbid"


class GetConnectionTestResponse(Schema):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str

    class Config:
        extra = "allow"


# class CreateConnectionTestPayload(Schema)


class CreateConnectionTestResponse(Schema):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str

    class Config:
        extra = "allow"


class UpdateConnectionTestResponse(Schema):
    connectionId: str
    name: str
    sourceId: str
    destinationId: str

    class Config:
        extra = "allow"


class CheckSourceConnectionTestResponse(Schema):
    jobInfo: dict

    class Config:
        extra = "allow"


class CheckDestinationConnectionTestResponse(Schema):
    class Config:
        extra = "allow"
