import os
import django
from django.core.management import call_command
from django.apps import apps

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
