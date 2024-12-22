import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from unittest.mock import patch

pytestmark = pytest.mark.django_db

from ddpui.datainsights.warehouse.warehouse_factory import (
    WarehouseFactory,
    WarehouseType,
)
from ddpui.datainsights.warehouse.bigquery import BigqueryClient
from ddpui.datainsights.warehouse.postgres import PostgresClient


class MockClass:
    def __ini__(sefl):
        pass


def test_warehouse_factory():
    """Tests supported/unsupported warehouses"""

    with patch(
        "ddpui.datainsights.warehouse.postgres.PostgresClient.__init__",
        return_value=None,
    ) as MockPostgresClient:
        wobj = WarehouseFactory.connect({"some_creds_dict": {}}, WarehouseType.POSTGRES)
        assert isinstance(wobj, PostgresClient)

    with patch(
        "ddpui.datainsights.warehouse.bigquery.BigqueryClient.__init__",
        return_value=None,
    ) as MockBigqueryClient:
        wobj = WarehouseFactory.connect({}, WarehouseType.BIGQUERY)
        assert isinstance(wobj, BigqueryClient)

    with pytest.raises(ValueError):
        WarehouseFactory.connect({}, "some-no-supported-warehouse-type")


def test_connect_args_1():
    """tests creation on connect_args parameter to create_engine"""
    with patch("ddpui.datainsights.warehouse.postgres.inspect"):
        with patch("ddpui.datainsights.warehouse.postgres.create_engine") as mock_create_engine:
            PostgresClient(
                {
                    "username": "user name",
                    "password": "pass word",
                    "host": "host",
                    "port": 1234,
                    "database": "db",
                    "sslrootcert": "sslrootcert",
                    "sslmode": "require",
                }
            )
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://",
                connect_args={
                    "host": "host",
                    "port": 1234,
                    "dbname": "db",
                    "user": "user%20name",
                    "password": "pass%20word",
                    "sslrootcert": "sslrootcert",
                    "sslmode": "require",
                },
                pool_size=5,
                pool_timeout=30,
            )


def test_connect_args_2():
    """tests creation on connect_args parameter to create_engine"""
    with patch("ddpui.datainsights.warehouse.postgres.inspect"):
        with patch("ddpui.datainsights.warehouse.postgres.create_engine") as mock_create_engine:
            PostgresClient(
                {
                    "username": "user name",
                    "password": "pass word",
                    "host": "host",
                    "port": 1234,
                    "database": "db",
                    "sslrootcert": "sslrootcert",
                    "sslmode": True,
                }
            )
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://",
                connect_args={
                    "host": "host",
                    "port": 1234,
                    "dbname": "db",
                    "user": "user%20name",
                    "password": "pass%20word",
                    "sslrootcert": "sslrootcert",
                    "sslmode": "require",
                },
                pool_size=5,
                pool_timeout=30,
            )


def test_connect_args_3():
    """tests creation on connect_args parameter to create_engine"""
    with patch("ddpui.datainsights.warehouse.postgres.inspect"):
        with patch("ddpui.datainsights.warehouse.postgres.create_engine") as mock_create_engine:
            PostgresClient(
                {
                    "username": "user name",
                    "password": "pass word",
                    "host": "host",
                    "port": 1234,
                    "database": "db",
                    "sslrootcert": "sslrootcert",
                    "sslmode": False,
                }
            )
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://",
                connect_args={
                    "host": "host",
                    "port": 1234,
                    "dbname": "db",
                    "user": "user%20name",
                    "password": "pass%20word",
                    "sslrootcert": "sslrootcert",
                    "sslmode": "disable",
                },
                pool_size=5,
                pool_timeout=30,
            )


def test_url_encoding():
    """tests url encoding of username and password"""
    with patch("ddpui.datainsights.warehouse.postgres.inspect"):
        with patch("ddpui.datainsights.warehouse.postgres.create_engine") as mock_create_engine:
            PostgresClient(
                {
                    "username": "user name",
                    "password": "pass word",
                    "host": "host",
                    "port": 1234,
                    "database": "db",
                }
            )
            mock_create_engine.assert_called_with(
                "postgresql+psycopg2://",
                connect_args={
                    "host": "host",
                    "port": 1234,
                    "dbname": "db",
                    "user": "user%20name",
                    "password": "pass%20word",
                },
                pool_size=5,
                pool_timeout=30,
            )
