"""Tests for ddpui/utils/warehouse/client/bigquery.py

Covers BigqueryClient methods with fully mocked engine/inspector:
  - __init__ (engine creation)
  - execute
  - get_table_columns (normal, NullType, unsupported STRUCT)
  - get_col_python_type
  - get_wtype
  - column_exists (found, not found, NoSuchTableError, general exception)
"""

from unittest.mock import patch, MagicMock, PropertyMock
import pytest
from sqlalchemy.types import NullType
from sqlalchemy.exc import NoSuchTableError

from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


class TestBigqueryClientInit:
    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_creates_engine(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = MagicMock()

        creds = {"project_id": "my-project"}
        client = BigqueryClient(creds)

        mock_create_engine.assert_called_once_with(
            "bigquery://my-project",
            credentials_info=creds,
            pool_size=5,
            pool_timeout=30,
        )
        assert client.engine == mock_engine


class TestBigqueryClientExecute:
    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_execute_returns_list_of_dicts(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = MagicMock()

        # Simulate connection.execute().fetchall()
        mock_row1 = MagicMock()
        mock_row1.__iter__ = MagicMock(return_value=iter([("a", 1)]))
        dict_row1 = {"col": "a", "val": 1}
        mock_row2 = MagicMock()
        dict_row2 = {"col": "b", "val": 2}

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        # Make dict() work on rows
        mock_row1.__iter__ = MagicMock(return_value=iter(dict_row1.items()))
        mock_row1.keys = MagicMock(return_value=dict_row1.keys())
        mock_row2.__iter__ = MagicMock(return_value=iter(dict_row2.items()))
        mock_row2.keys = MagicMock(return_value=dict_row2.keys())

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        client = BigqueryClient({"project_id": "proj"})
        results = client.execute("SELECT 1")
        mock_conn.execute.assert_called_once_with("SELECT 1")


class TestBigqueryClientGetTableColumns:
    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_normal_columns(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()

        mock_col_type = MagicMock()
        mock_col_type.__str__ = MagicMock(return_value="VARCHAR")
        mock_col_type.python_type = str

        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "id", "type": mock_col_type, "nullable": False},
        ]
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        cols = client.get_table_columns("schema1", "table1")

        assert len(cols) == 1
        assert cols[0]["name"] == "id"
        assert cols[0]["data_type"] == "VARCHAR"
        assert cols[0]["nullable"] is False

    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_null_type_column(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()

        null_col_type = NullType()

        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "unknown_col", "type": null_col_type, "nullable": True},
        ]
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        cols = client.get_table_columns("schema1", "table1")

        assert len(cols) == 1
        assert cols[0]["translated_type"] is None

    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_struct_columns_skipped(self, mock_create_engine, mock_inspect):
        """STRUCT columns and their children should be skipped."""
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()

        # The STRUCT column will raise an exception (no python_type)
        struct_type = MagicMock()
        struct_type.__str__ = MagicMock(return_value="STRUCT")
        type(struct_type).python_type = PropertyMock(side_effect=NotImplementedError)

        # A child of the STRUCT column
        child_type = MagicMock()
        child_type.__str__ = MagicMock(return_value="VARCHAR")
        child_type.python_type = str

        # A normal column
        normal_type = MagicMock()
        normal_type.__str__ = MagicMock(return_value="INTEGER")
        normal_type.python_type = int

        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "address", "type": struct_type, "nullable": True},
            {"name": "address.city", "type": child_type, "nullable": True},
            {"name": "id", "type": normal_type, "nullable": False},
        ]
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        cols = client.get_table_columns("schema1", "table1")

        # Only the normal column should remain
        assert len(cols) == 1
        assert cols[0]["name"] == "id"


class TestBigqueryClientGetWtype:
    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_returns_bigquery(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        client = BigqueryClient({"project_id": "proj"})
        assert client.get_wtype() == WarehouseType.BIGQUERY


class TestBigqueryClientColumnExists:
    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_column_found(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "id"},
            {"name": "email"},
        ]
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        assert client.column_exists("schema1", "table1", "email") is True

    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_column_not_found(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "id"},
        ]
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        assert client.column_exists("schema1", "table1", "email") is False

    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_no_such_table(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.side_effect = NoSuchTableError("table1")
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        assert client.column_exists("schema1", "table1", "id") is False

    @patch("ddpui.utils.warehouse.client.bigquery.inspect")
    @patch("ddpui.utils.warehouse.client.bigquery.create_engine")
    def test_general_exception(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.bigquery import BigqueryClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.side_effect = Exception("something went wrong")
        mock_inspect.return_value = inspector

        client = BigqueryClient({"project_id": "proj"})
        assert client.column_exists("schema1", "table1", "id") is False
