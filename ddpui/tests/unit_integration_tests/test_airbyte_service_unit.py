import os
from unittest.mock import patch, MagicMock
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ninja import Schema
from ddpui.ddpairbyte.airbyte_service import *
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from uuid import UUID


class ConnectionConfiguration(BaseModel):
    user: str
    start_date: str
    api_key: str


class CreateSourceTestResponse(Schema):
    sourceDefinitionId: str
    connectionConfiguration: ConnectionConfiguration
    workspaceId: str
    name: str
    sourceId: str
    sourceName: str

    class Config:
        extra = "forbid"


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
        extra = "forbid"


class GetSourceDefinitions(Schema):
    sourceDefinitionId: str
    name: str

    class Config:
        extra = "allow"


class GetSourceDefinitionsTestResponse(Schema):
    sourceDefinitions: List[GetSourceDefinitions]

    class Config:
        extra = "allow"


class GetWorkspacesTestResponse(Schema):
    workspaces: List[GetWorkspaceTestResponse]

    class Config:
        extra = "allow"


class SetWorkspaceTestResponse(BaseModel):
    workspaceId: str
    customerId: str
    email: str
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
    sourceDefinitionId: str
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

    class Config:
        extra = "forbid"


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


TEST_WORKSPACE_ID = "980a3c41-bcdb-4988-b3e4-de7b10c1faba"


class TestWorkspace:
    def test_get_workspace(self):
        test_workspace_id = TEST_WORKSPACE_ID

        try:
            res = get_workspace(workspace_id=test_workspace_id)
            GetWorkspaceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_workspaces(self):
        try:
            res = get_workspaces()
            GetWorkspacesTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    # def test_set_workspace_name(self):
    #     workspace_id = TEST_WORKSPACE_ID
    #     new_name = "test"

    #     try:
    #         res = set_workspace_name(workspace_id=workspace_id, name=new_name)
    #         SetWorkspaceTestResponse(**res)
    #     except ValidationError as e:
    #         raise ValueError(f"Response validation failed: {e.errors()}")

    def test_create_workspace(self):
        payload = {"name": "workspace1"}

        try:
            CreateWorkspaceTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in the payload: {e.errors()}")

        try:
            res = create_workspace(**payload)
            CreateWorkspaceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


class TestAirbyteSource:
    # def test_get_source_definitions(self):
    #     workspace_id = TEST_WORKSPACE_ID

    #     try:
    #         res = get_source_definitions(workspace_id)
    #         GetSourceDefinitionsTestResponse(**res)
    #     except ValidationError as e:
    #         raise ValueError(f"Response validation failed: {e.errors()}")

    # def test_get_source_definition_specification(self):
    #     workspace_id = "6dd532ee-6bd4-446f-ae29-121a8d08d0ca"
    #     sourcedef_id = "f95337f1-2ad1-4baf-922f-2ca9152de630"

    #     try:
    #         res = get_source_definition_specification(workspace_id, sourcedef_id)
    #         GetSourceDefinitionSpecificationTestResponse(**res)
    #     except ValidationError as e:
    #         raise ValueError(f"Response validation failed: {e.errors()}")

    def test_get_source(self):
        test_workspace_id = TEST_WORKSPACE_ID

        try:
            res = get_source(
                workspace_id=test_workspace_id,
                source_id="cd5eabbc-fd7c-4b58-86d6-f0b74d8a9a0b",
            )
            GetSourceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    # def test_get_sources(self):
    #     test_workspace_id = TEST_WORKSPACE_ID

    #     try:
    #         res = get_sources(workspace_id=test_workspace_id)
    #         GetSourcesTestResponse(**res)
    #     except ValidationError as e:
    #         raise ValueError(f"Response validation failed: {e.errors()}")

    def test_create_source(self):
        payload = {
            "sourcedef_id": "f95337f1-2ad1-4baf-922f-2ca9152de630",
            "config": {
                "user": "abhis@gmail.com",
                "start_date": "2023-02-01T00:00:00Z",
                "api_key": "YWlyYnl0ZTpwYXNzd29yZA==",
            },
            "workspace_id": TEST_WORKSPACE_ID,
            "name": "source1",
        }

        try:
            CreateSourceTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")

        try:
            res = create_source(**payload)
            CreateSourceTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    # @patch("requests.post")
    # def test_abreq_failure(self, mock_post):
    #     mock_post.side_effect = Exception("error")

    #     # test the abreq function for failure
    #     with pytest.raises(Exception) as excinfo:
    #         abreq("test_endpoint", {"test_key": "test_value"})
    #     assert str(excinfo.value) == "error"


class TestAirbyteDestination:
    def test_get_destination(self):
        test_workspace_id = TEST_WORKSPACE_ID

        try:
            res = get_destination(
                workspace_id=test_workspace_id,
                destination_id="603cb3a1-d232-45cd-98a5-4312d6243c2c",
            )
            # breakpoint()
            GetDestinationTestResponse(**res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_create_destination(self):
        payload = {
            "name": "destination1",
            "destinationdef_id": "ce0d828e-1dc4-496c-b122-2da42e637e48",
            "config": {
                "host": "dev-test.c4hvhyuxrcet.ap-south-1.rds.amazonaws.com",
                "port": 5432,
                "database": "ddpabhis",
                "username": "abhis",
            },
        }
        try:
            CreateDestinationTestPayload(**payload)
        except ValidationError as e:
            raise ValueError(f"Field do not match in payload: {e.errors()}")
