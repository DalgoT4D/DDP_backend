import pytest
from unittest.mock import Mock, patch

from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.tests.api_tests.test_user_org_api import (
    seed_db,
    orguser,
    mock_request,
    authuser,
    org_without_workspace,
)
from ddpui.api.warehouse_api import (
    get_schema,
    get_table,
    get_table_columns,
    get_table_data,
)


pytestmark = pytest.mark.django_db


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["table1", "table2"]),
)
def test_get_table_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    response = get_table(request, schema_name)

    assert response is not None
    assert response == ["table1", "table2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["schema1", "schema2"]),
)
def test_get_schema_success(orguser):
    request = mock_request(orguser)
    response = get_schema(request)

    assert response is not None
    assert response == ["schema1", "schema2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["column1", "column2"]),
)
def test_get_table_columns_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_columns(request, schema_name, table_name)

    assert response is not None
    assert response == ["column1", "column2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(
        return_value=[{"column_1": "value_1"}, {"column2": "value2}"}]
    ),
)
def test_get_table_data_success(orguser):
    request = mock_request(orguser)
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_data(request, schema_name, table_name)

    assert response is not None
    assert response == [{"column_1": "value_1"}, {"column2": "value2}"}]
