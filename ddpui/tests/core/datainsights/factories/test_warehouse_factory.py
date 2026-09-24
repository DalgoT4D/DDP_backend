import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
from unittest.mock import patch

pytestmark = pytest.mark.django_db

from ddpui.utils.warehouse.client.warehouse_factory import (
    WarehouseFactory,
    WarehouseType,
)
from ddpui.utils.warehouse.client import postgres_engine_registry
from ddpui.utils.warehouse.client.bigquery import BigqueryClient
from ddpui.utils.warehouse.client.postgres import PostgresClient


class MockClass:
    def __ini__(sefl):
        pass


def test_warehouse_factory():
    """Tests supported/unsupported warehouses"""

    with patch(
        "ddpui.utils.warehouse.client.postgres.PostgresClient.__init__",
        return_value=None,
    ) as MockPostgresClient:
        wobj = WarehouseFactory.connect({"some_creds_dict": {}}, WarehouseType.POSTGRES)
        assert isinstance(wobj, PostgresClient)

    with patch(
        "ddpui.utils.warehouse.client.bigquery.BigqueryClient.__init__",
        return_value=None,
    ) as MockBigqueryClient:
        wobj = WarehouseFactory.connect({}, WarehouseType.BIGQUERY)
        assert isinstance(wobj, BigqueryClient)

    with pytest.raises(ValueError):
        WarehouseFactory.connect({}, "some-no-supported-warehouse-type")


BASE_PG_CREDS = {
    "username": "user name",
    "password": "pass word",
    "host": "host",
    "port": 1234,
    "database": "db",
    "sslrootcert": "sslrootcert",
}

EXPECTED_BASE_ARGS = {
    "host": "host",
    "port": 1234,
    "dbname": "db",
    "user": "user name",
    "password": "pass word",
    "sslrootcert": "sslrootcert",
}


def test_connect_args_1():
    """sslmode given as a string is passed through verbatim"""
    args = PostgresClient.build_connection_args({**BASE_PG_CREDS, "sslmode": "require"})

    assert args == {**EXPECTED_BASE_ARGS, "sslmode": "require"}


def test_connect_args_2():
    """sslmode given as boolean True means 'require'"""
    args = PostgresClient.build_connection_args({**BASE_PG_CREDS, "sslmode": True})

    assert args == {**EXPECTED_BASE_ARGS, "sslmode": "require"}


def test_connect_args_3():
    """sslmode given as boolean False means 'disable'"""
    args = PostgresClient.build_connection_args({**BASE_PG_CREDS, "sslmode": False})

    assert args == {**EXPECTED_BASE_ARGS, "sslmode": "disable"}


def test_connect_args_writes_a_ca_certificate_to_disk():
    """
    psycopg2 wants sslrootcert as a path, but the credentials carry the certificate
    inline, so it gets written to a temp file. The file outlives the call (delete=False)
    because psycopg2 reads it at connect time, not here.
    """
    creds = {**BASE_PG_CREDS, "sslmode": {"ca_certificate": "---CERT BODY---"}}
    del creds["sslrootcert"]

    args = PostgresClient.build_connection_args(creds)

    assert os.path.isfile(args["sslrootcert"])
    with open(args["sslrootcert"], encoding="utf-8") as fp:
        assert fp.read() == "---CERT BODY---"

    os.unlink(args["sslrootcert"])


def test_connect_args_does_not_mutate_the_caller_creds():
    """
    ssl_mode is aliased to sslmode while building connect_args; the credentials
    dict handed in must come back untouched, since the caller's copy is what the
    engine cache was fingerprinted from.
    """
    creds = {**BASE_PG_CREDS, "ssl_mode": "require"}

    args = PostgresClient.build_connection_args(creds)

    assert args["sslmode"] == "require"
    assert "sslmode" not in creds


def test_postgres_client_builds_its_engine_with_the_registry_pool_settings():
    """
    The engine must carry the bounded pool settings, not SQLAlchemy's defaults --
    max_overflow especially, which defaults to 10 and set the old per-warehouse
    ceiling at 15 connections.
    """
    postgres_engine_registry._engines.clear()

    with patch("ddpui.utils.warehouse.client.postgres.inspect"):
        with patch("ddpui.utils.warehouse.client.postgres.create_engine") as mock_create_engine:
            PostgresClient({**BASE_PG_CREDS, "sslmode": "require"})

    mock_create_engine.assert_called_once_with(
        "postgresql+psycopg2://",
        connect_args={**EXPECTED_BASE_ARGS, "sslmode": "require"},
        **postgres_engine_registry.pool_kwargs(),
    )

    postgres_engine_registry._engines.clear()


def test_repeated_clients_for_one_warehouse_share_a_single_engine():
    """
    The fix for the connection leak: constructing a client per request -- which
    is what the chart, dashboard, KPI and filter endpoints all do -- must not
    build a new pool per request.
    """
    postgres_engine_registry._engines.clear()

    with patch("ddpui.utils.warehouse.client.postgres.inspect"):
        with patch("ddpui.utils.warehouse.client.postgres.create_engine") as mock_create_engine:
            first = PostgresClient(dict(BASE_PG_CREDS))
            second = PostgresClient(dict(BASE_PG_CREDS))

    assert mock_create_engine.call_count == 1
    assert first.engine is second.engine

    postgres_engine_registry._engines.clear()


def test_connect_args_are_built_only_on_a_cache_miss():
    """
    build_connection_args runs inside the callback the registry invokes on a miss,
    never on the per-request path. It writes a CA certificate to a temp file for
    warehouses configured with one -- once per engine here, once per request if this
    ever slips back out of the callback.
    """
    postgres_engine_registry._engines.clear()

    with patch("ddpui.utils.warehouse.client.postgres.inspect"):
        with patch("ddpui.utils.warehouse.client.postgres.create_engine"):
            with patch.object(
                PostgresClient, "build_connection_args", return_value={}
            ) as mock_build:
                PostgresClient(dict(BASE_PG_CREDS))
                PostgresClient(dict(BASE_PG_CREDS))
                PostgresClient(dict(BASE_PG_CREDS))

    assert mock_build.call_count == 1

    postgres_engine_registry._engines.clear()
