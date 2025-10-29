import os
import django
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

pytestmark = pytest.mark.django_db

from ddpui.datainsights.warehouse.warehouse_interface import Warehouse


class UnimplementedMethodWarehouse(Warehouse):
    def __init__(self):
        pass


class DummyWarehouse(Warehouse):
    def __init__(self):
        pass

    def execute(self, sql_statement: str):
        pass

    def get_table_columns(self, db_schema: str, db_table: str) -> dict:
        pass

    def get_col_python_type(self, db_schema: str, db_table: str, column_name: str):
        pass

    def get_wtype(self):
        pass

    def column_exists(self, db_schema, db_table, column_name):
        pass


def test_unimplemented_methods_warehouse_interface():
    """Each warehouse client should implement all abstract methods in Warehouse interface"""

    with pytest.raises(TypeError):
        UnimplementedMethodWarehouse()


def test_dummy_warehouse_with_all_methods_implemented():
    """Success test with all abstract methods implemented in Warehouse interface"""
    obj = DummyWarehouse()

    assert "execute" in dir(obj)
    assert "get_col_python_type" in dir(obj)
    assert "get_table_columns" in dir(obj)
    assert "get_wtype" in dir(obj)
