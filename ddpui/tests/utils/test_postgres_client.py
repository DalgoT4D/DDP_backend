"""Tests for ddpui/utils/warehouse/client/postgres.py

Covers PostgresClient methods with fully mocked engine/inspector:
  - __init__ with various SSL configs (string, bool, dict with ca_certificate)
  - execute
  - get_table_columns (normal, NullType)
  - get_wtype
  - column_exists (found, not found, NoSuchTableError, general exception)
"""

from unittest.mock import patch, MagicMock
import pytest
from sqlalchemy.types import NullType
from sqlalchemy.exc import NoSuchTableError

from ddpui.utils.warehouse.client.warehouse_interface import WarehouseType


class TestPostgresClientInit:
    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_basic_creds(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)

        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == "postgresql+psycopg2://"
        connect_args = call_args[1]["connect_args"]
        assert connect_args["host"] == "localhost"
        assert connect_args["port"] == 5432
        assert connect_args["dbname"] == "testdb"

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_ssl_mode_string(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "sslmode": "verify-full",
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["sslmode"] == "verify-full"

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_ssl_mode_bool_true(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "sslmode": True,
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["sslmode"] == "require"

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_ssl_mode_bool_false(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "sslmode": False,
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["sslmode"] == "disable"

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_ssl_mode_dict_with_ca_certificate(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "sslmode": {
                "ca_certificate": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
            },
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        # sslrootcert should be a file path (tempfile)
        assert "sslrootcert" in connect_args
        assert connect_args["sslrootcert"]  # not empty

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_ssl_mode_from_ssl_mode_key(self, mock_create_engine, mock_inspect):
        """ssl_mode key gets mapped to sslmode."""
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "ssl_mode": "require",
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["sslmode"] == "require"

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_sslrootcert_in_creds(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
            "sslrootcert": "/path/to/cert.pem",
        }
        client = PostgresClient(creds)

        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["sslrootcert"] == "/path/to/cert.pem"


class TestPostgresClientExecute:
    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_execute_returns_list_of_dicts(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = MagicMock()

        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_result.fetchall.return_value = [mock_row]
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        client.execute("SELECT 1")
        mock_conn.execute.assert_called_once_with("SELECT 1")


class TestPostgresClientGetTableColumns:
    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_normal_columns(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()

        col_type = MagicMock()
        col_type.__str__ = MagicMock(return_value="INTEGER")
        col_type.python_type = int

        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "id", "type": col_type, "nullable": False},
        ]
        mock_inspect.return_value = inspector

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        cols = client.get_table_columns("public", "users")

        assert len(cols) == 1
        assert cols[0]["name"] == "id"
        assert cols[0]["data_type"] == "INTEGER"
        assert cols[0]["nullable"] is False

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_null_type_column(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()

        null_col_type = NullType()

        inspector = MagicMock()
        inspector.get_columns.return_value = [
            {"name": "unknown_col", "type": null_col_type, "nullable": True},
        ]
        mock_inspect.return_value = inspector

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        cols = client.get_table_columns("public", "users")

        assert len(cols) == 1
        assert cols[0]["translated_type"] is None


class TestPostgresClientGetWtype:
    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_returns_postgres(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        mock_inspect.return_value = MagicMock()

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        assert client.get_wtype() == WarehouseType.POSTGRES


class TestPostgresClientColumnExists:
    def _make_client(self, mock_create_engine, mock_inspect, columns=None):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        if columns is not None:
            inspector.get_columns.return_value = columns
        mock_inspect.return_value = inspector

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        return PostgresClient(creds), inspector

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_column_found(self, mock_create_engine, mock_inspect):
        client, _ = self._make_client(
            mock_create_engine,
            mock_inspect,
            columns=[{"name": "id"}, {"name": "email"}],
        )
        assert client.column_exists("public", "users", "email") is True

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_column_not_found(self, mock_create_engine, mock_inspect):
        client, _ = self._make_client(
            mock_create_engine,
            mock_inspect,
            columns=[{"name": "id"}],
        )
        assert client.column_exists("public", "users", "email") is False

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_no_such_table(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.side_effect = NoSuchTableError("users")
        mock_inspect.return_value = inspector

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        assert client.column_exists("public", "users", "id") is False

    @patch("ddpui.utils.warehouse.client.postgres.inspect")
    @patch("ddpui.utils.warehouse.client.postgres.create_engine")
    def test_general_exception(self, mock_create_engine, mock_inspect):
        from ddpui.utils.warehouse.client.postgres import PostgresClient

        mock_create_engine.return_value = MagicMock()
        inspector = MagicMock()
        inspector.get_columns.side_effect = Exception("db error")
        mock_inspect.return_value = inspector

        creds = {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        client = PostgresClient(creds)
        assert client.column_exists("public", "users", "id") is False
