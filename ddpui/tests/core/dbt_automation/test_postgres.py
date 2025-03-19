from unittest.mock import patch, ANY, Mock
from ddpui.dbt_automation.utils.postgres import PostgresClient


def test_get_connection_1():
    """tests PostgresClient.get_connection"""
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {"sslmode": {"mode": "disable", "ca_certificate": "LONG-CERTIFICATE"}}
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(sslmode="disable", sslrootcert=ANY)


def test_drop_table():
    """tests PostgresClient.drop_table"""
    with patch.object(PostgresClient, "runcmd") as mock_runcmd, patch(
        "ddpui.dbt_automation.utils.postgres.psycopg2.connect"
    ):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        client.drop_table("schema", "table")
        mock_runcmd.assert_called_once_with("DROP TABLE IF EXISTS schema.table;")


def test_insert_row():
    """tests PostgresClient.insert_row"""
    with patch.object(PostgresClient, "runcmd") as mock_runcmd, patch(
        "ddpui.dbt_automation.utils.postgres.psycopg2.connect"
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect"):
        client = PostgresClient(
            {"host": "HOST", "port": 1234, "user": "USER", "password": "PASSWORD"}
        )
        result = client.json_extract_op("json_column", "json_field", "sql_column")
        assert result == """"json_column"::json->>'json_field' as \"sql_column\""""


def test_close():
    """tests PostgresClient.close"""
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect"):
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect"):
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
        "ddpui.dbt_automation.utils.postgres.psycopg2.connect"
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
    with patch("ddpui.dbt_automation.utils.postgres.psycopg2.connect"):
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
