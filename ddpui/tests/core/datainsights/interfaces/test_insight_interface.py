import os
import django
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

pytestmark = pytest.mark.django_db

from sqlalchemy.sql.selectable import Select

from ddpui.datainsights.insights.insight_interface import (
    ColInsight,
    TranslateColDataType,
    DataTypeColInsights,
    ColumnConfig,
)


class UnimplementMethodsColInsight(ColInsight):
    def chart_type(self) -> str:
        return super().chart_type()


class DummyTestColInsight(ColInsight):
    def chart_type(self) -> str:
        return super().chart_type()

    def query_id(self) -> str:
        return "query_id"

    def generate_sql(self) -> Select:
        pass

    def parse_results(self, result: list[dict]):
        pass

    def validate_query_results(self, parsed_results) -> bool:
        pass

    def query_data_type(self) -> TranslateColDataType:
        pass


def test_unimplemented_methods():
    """Any class extending the ColInsight interface should implement all abstract methods"""

    with pytest.raises(TypeError):
        UnimplementMethodsColInsight([], "test_table", "test_schema")


def test_successful_implementation_colinsight():
    """Successful implementation of ColInsights class with all abstract methods defined"""
    obj = DummyTestColInsight([], "test_table", "test_schema", None, "postgres")

    assert obj.wtype == "postgres"
    assert obj.db_schema == "test_schema"
    assert obj.db_table == "test_table"
    assert type(obj.columns) == list


class SomeDataTypeColInsightsParentClass(DataTypeColInsights):
    def __init__(
        self,
        columns: list[dict],
        db_table: str,
        db_schema: str,
        filter_: dict = None,
        wtype: str = None,
    ):
        super().__init__(columns, db_table, db_schema, filter_, wtype)
        self.insights: list[ColInsight] = [
            DummyTestColInsight(
                self.columns, self.db_table, self.db_schema, self.filter, self.wtype
            ),
        ]


def test_invalid_column_config():
    """Failure test to make sure correct column dictionary is passed in DataTypeColInsights"""
    col = {"name": "test_col"}

    with pytest.raises(KeyError):
        obj = SomeDataTypeColInsightsParentClass([col], "test_table", "test_schema", None)

    col = {"name": "test_col", "data_type": "some_type"}

    with pytest.raises(KeyError):
        obj = SomeDataTypeColInsightsParentClass([col], "test_table", "test_schema", None)


def test_valid_column_config():
    """Success test to create object of DataTypeColInsights"""
    col = {"name": "test_col", "data_type": "some_type", "translated_type": "Numeric"}

    obj = SomeDataTypeColInsightsParentClass([col], "test_table", "test_schema", None)

    assert len(obj.columns) == 1
    assert type(obj.columns[0]) == ColumnConfig
    assert obj.db_table == "test_table"
    assert obj.db_schema == "test_schema"
