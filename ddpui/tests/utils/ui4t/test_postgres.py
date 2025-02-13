from unittest.mock import patch, ANY
from dbt_automation.utils.postgres import PostgresClient


def test_get_connection_1():
    """tests PostgresClient.get_connection"""
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
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
    with patch("dbt_automation.utils.postgres.psycopg2.connect") as mock_connect:
        PostgresClient.get_connection(
            {"sslmode": {"mode": "disable", "ca_certificate": "LONG-CERTIFICATE"}}
        )
        mock_connect.assert_called_once()
        mock_connect.assert_called_with(sslmode="disable", sslrootcert=ANY, sslcert=ANY)
