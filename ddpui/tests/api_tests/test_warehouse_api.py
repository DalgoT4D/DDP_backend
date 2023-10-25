from unittest.mock import Mock

from ddpui.api.client.warehouse_api import (
    get_schema,
    get_table,
    get_table_columns,
    get_table_data,
)


def test_get_table_success():
    mock_request = Mock()
    mock_request.orguser = "test"

    schema_name = "test_schema"
    response = get_table(mock_request, schema_name)

    assert response is not None
    assert "tables" in response


def test_get_schema_success():
    mock_request = Mock()
    mock_request.orguser = "test"

    response = get_schema(mock_request)

    assert response is not None
    assert "schemas" in response


def test_get_table_data_success():
    mock_request = Mock()
    mock_request.orguser = "test"

    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_data(mock_request, schema_name, table_name)

    assert response is not None
    assert "data" in response


def test_get_table_column():
    mock_request = Mock()
    mock_request.orguser = "test"

    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_columns(mock_request, schema_name, table_name)

    assert response is not None
    assert "columns" in response
