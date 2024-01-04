from unittest.mock import Mock, patch

from ddpui.api.warehouse_api import (
    get_schema,
    get_table,
    get_table_columns,
    get_table_data,
)


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["table1", "table2"]),
)
def test_get_table_success():
    mock_request = Mock()
    mock_request.orguser = "test"
    schema_name = "test_schema"
    response = get_table(mock_request, schema_name)

    assert response is not None
    assert response == ["table1", "table2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["schema1", "schema2"]),
)
def test_get_schema_success():
    mock_request = Mock()
    mock_request.orguser = "test"
    response = get_schema(mock_request)

    assert response is not None
    assert response == ["schema1", "schema2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(return_value=["column1", "column2"]),
)
def test_get_table_columns_success():
    mock_request = Mock()
    mock_request.orguser = "test"
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_columns(mock_request, schema_name, table_name)

    assert response is not None
    assert response == ["column1", "column2"]


@patch.multiple(
    "ddpui.api.warehouse_api",
    get_warehouse_data=Mock(
        return_value=[{"column_1": "value_1"}, {"column2": "value2}"}]
    ),
)
def test_get_table_data_success():
    mock_request = Mock()
    mock_request.orguser = "test"
    schema_name = "test_schema"
    table_name = "test_table"
    response = get_table_data(mock_request, schema_name, table_name)

    assert response is not None
    assert response == [{"column_1": "value_1"}, {"column2": "value2}"}]
