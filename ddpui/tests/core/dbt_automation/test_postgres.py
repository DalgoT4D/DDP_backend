import os
import tempfile
from unittest.mock import patch, ANY, Mock, MagicMock
import pytest
from ddpui.utils.warehouse.old_client.postgres import PostgresClient


@pytest.fixture
def mock_tunnel():
    """Mock the SSHTunnelForwarder class."""
    with patch("ddpui.core.dbt_automation.utils.postgres.SSHTunnelForwarder") as MockTunnel:
        instance = MagicMock()
        instance.local_bind_port = 6543
        MockTunnel.return_value = instance
        yield MockTunnel


@pytest.fixture
def mock_connection():
    """Mock the PostgresClient.get_connection method."""
    with patch(
        "ddpui.core.dbt_automation.utils.postgres.PostgresClient.get_connection"
    ) as mock_get_conn:
        mock_get_conn.return_value = "MOCK_CONNECTION"
        yield mock_get_conn


def test_get_connection_1():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            host="HOST",
            port=1234,
            user="USER",
            password="PASSWORD",
        )


def test_get_connection_2():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {
                "host": "HOST",
                "port": 1234,
                "user": "USER",
                "password": "PASSWORD",
                "database": "DATABASE",
            }
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            host="HOST",
            port=1234,
            user="USER",
            password="PASSWORD",
            database="DATABASE",
        )


def test_get_connection_3():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {
                "sslmode": "verify-ca",
                "sslrootcert": "/path/to/cert",
            }
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            sslmode="verify-ca",
            sslrootcert="/path/to/cert",
        )


def test_get_connection_4():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {
                "sslmode": True,
                "sslrootcert": "/path/to/cert",
            }
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            sslmode="require",
            sslrootcert="/path/to/cert",
        )


def test_get_connection_5():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {
                "sslmode": False,
                "sslrootcert": "/path/to/cert",
            }
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            sslmode="disable",
            sslrootcert="/path/to/cert",
        )


def test_get_connection_6():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {
                "sslmode": {
                    "mode": "disable",
                }
            }
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(
            sslmode="disable",
        )


def test_get_connection_7():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {"sslmode": {"mode": "disable", "ca_certificate": "LONG-CERTIFICATE"}}
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(sslmode="disable", sslrootcert=ANY)


def test_init_with_ssh_pkey_writes_tempfile_and_starts_tunnel(mock_tunnel, mock_connection):
    """tests PostgresClient.__init__ with ssh_pkey"""
    fake_key_content = "FAKE_PRIVATE_KEY_CONTENT"

    conn_info = {
        "host": "db.internal",
        "port": 5432,
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
        "ssh_host": "bastion.internal",
        "ssh_port": 22,
        "ssh_username": "sshuser",
        "ssh_pkey": fake_key_content,
    }

    with patch("tempfile.NamedTemporaryFile", wraps=tempfile.NamedTemporaryFile) as mock_tempfile:
        client = PostgresClient(conn_info)

        # Ensure temporary file was created
        assert mock_tempfile.called
        # Ensure the content of the file matches the ssh_pkey
        temp_file_path = client.conn_info["ssh_pkey"]
        with open(temp_file_path, "r", encoding="utf8") as f:
            assert f.read() == fake_key_content

        # Clean up the tempfile
        os.remove(temp_file_path)

    # Ensure the SSH tunnel was started
    mock_tunnel.return_value.start.assert_called_once()

    # Ensure connection was made with correct redirected host/port
    args = mock_connection.call_args[0][0]
    assert args["host"] == "localhost"
    assert args["port"] == 6543
    assert client.connection == "MOCK_CONNECTION"


def test_drop_table():
    """tests PostgresClient.drop_table"""
    with patch.object(PostgresClient, "runcmd") as mock_runcmd, patch(
        "ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"
    ):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        client.drop_table("schema", "table")
        mock_runcmd.assert_called_once_with("DROP TABLE IF EXISTS schema.table;")


def test_insert_row():
    """tests PostgresClient.insert_row"""
    with patch.object(PostgresClient, "runcmd") as mock_runcmd, patch(
        "ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"
    ):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        client.insert_row("schema", "table", {"col1": "val1", "col2": "val2"})
        mock_runcmd.assert_called_once_with(
            """
            INSERT INTO schema.table (col1,col2)
            VALUES ('val1','val2');
            """
        )


def test_json_extract_op():
    """tests PostgresClient.json_extract_op"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        result = client.json_extract_op("json_column", "json_field", "sql_column")
        assert result == """"json_column"::json->>'json_field' as \"sql_column\""""


def test_close():
    """tests PostgresClient.close"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        mock_cursor = Mock(close=Mock())
        client.cursor = mock_cursor
        mock_tunnel = Mock(stop=Mock())
        client.tunnel = mock_tunnel
        mock_connection = Mock(close=Mock())
        client.connection = mock_connection

        assert client.close() is True

        mock_cursor.close.assert_called_once()
        assert client.cursor is None
        mock_tunnel.stop.assert_called_once()
        assert client.tunnel is None
        mock_connection.close.assert_called_once()
        assert client.connection is None


def test_generate_profiles_yaml_dbt():
    """tests PostgresClient.generate_profiles_yaml_dbt"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"):
        client = PostgresClient(
            {
                "host": "HOST",
                "port": 1234,
                "user": "USER",
                "password": "PASSWORD",
                "database": "DB",
            }
        )
        result = client.generate_profiles_yaml_dbt("project_name", "default_schema")
        expected = {
            "project_name": {
                "outputs": {
                    "prod": {
                        "dbname": "DB",
                        "host": "HOST",
                        "password": "PASSWORD",
                        "port": 1234,
                        "user": "USER",
                        "schema": "default_schema",
                        "threads": 4,
                        "type": "postgres",
                    }
                },
                "target": "prod",
            }
        }
        assert result == expected


def test_get_total_rows():
    """tests PostgresClient.get_total_rows"""
    with patch.object(PostgresClient, "execute", return_value=[[10]]) as mock_execute, patch(
        "ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"
    ):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        result = client.get_total_rows("schema", "table")
        mock_execute.assert_called_once_with(
            """
                SELECT COUNT(*) 
                FROM "schema"."table";
                """
        )
        assert result == 10


def test_get_column_data_types():
    """tests PostgresClient.get_column_data_types"""
    with patch("ddpui.core.dbt_automation.utils.postgres.psycopg2.connect"):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        result = client.get_column_data_types()
        expected = [
            "boolean",
            "char",
            "character varying",
            "date",
            "double precision",
            "float",
            "integer",
            "jsonb",
            "numeric",
            "text",
            "time",
            "timestamp",
            "timestamp with time zone",
            "uuid",
            "varchar",
        ]
        assert result == expected
