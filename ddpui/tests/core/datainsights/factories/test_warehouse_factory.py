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

    with patch.object(PostgresClient, "__new__") as MockPostgresClient:
        MockPostgresClient.return_value = MockClass()
        wobj = WarehouseFactory.connect({"some_creds_dict": {}}, WarehouseType.POSTGRES)
        MockPostgresClient.assert_called_once()
        assert isinstance(wobj, MockClass)

    with patch.object(BigqueryClient, "__new__") as MockBigqueryClient:
        MockBigqueryClient.return_value = MockClass()
        wobj = WarehouseFactory.connect({}, WarehouseType.BIGQUERY)
        MockBigqueryClient.assert_called_once()
        assert isinstance(wobj, MockClass)

    with pytest.raises(ValueError):
        WarehouseFactory.connect({}, "some-no-supported-warehouse-type")
