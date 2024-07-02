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

from ddpui.datainsights.insights.insight_factory import InsightsFactory
from ddpui.datainsights.insights.insight_interface import (
    TranslateColDataType,
    DataTypeColInsights,
)
from ddpui.datainsights.insights.numeric_type.numeric_insight import NumericColInsights
from ddpui.datainsights.insights.string_type.string_insights import StringColInsights
from ddpui.datainsights.insights.boolean_type.boolean_insights import BooleanColInsights
from ddpui.datainsights.insights.datetime_type.datetime_insight import (
    DatetimeColInsights,
)


class MockClass:
    def __ini__(sefl):
        pass


@pytest.fixture
def dummy_insight_payload():
    columns = []
    db_table = "test_table"
    db_schema = "test_schema"
    filter_ = {}
    wtype = "postgres"
    return {
        "columns": columns,
        "db_table": db_table,
        "db_schema": db_schema,
        "filter_": filter_,
        "wtype": wtype,
    }


def test_insight_factory(dummy_insight_payload):
    """Tests supported/unsupported data types"""

    with patch.object(NumericColInsights, "__new__") as MockNumericColInsights:
        MockNumericColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.NUMERIC
        )
        MockNumericColInsights.assert_called_once()
        assert isinstance(obj, MockClass)

    with patch.object(StringColInsights, "__new__") as MockStringColInsights:
        MockStringColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.STRING
        )
        MockStringColInsights.assert_called_once()
        assert isinstance(obj, MockClass)

    with patch.object(BooleanColInsights, "__new__") as MockBooleanColInsights:
        MockBooleanColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.BOOL
        )
        MockBooleanColInsights.assert_called_once()
        assert isinstance(obj, MockClass)

    with patch.object(DatetimeColInsights, "__new__") as MockDatetimeColInsights:
        MockDatetimeColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.DATETIME
        )
        MockDatetimeColInsights.assert_called_once()
        assert isinstance(obj, MockClass)

    # json calls the general parent class
    with patch.object(DataTypeColInsights, "__new__") as MockJsonColInsights:
        MockJsonColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.JSON
        )
        MockJsonColInsights.assert_called_once()
        assert isinstance(obj, MockClass)

    with pytest.raises(ValueError):
        InsightsFactory.initiate_insight(
            **dummy_insight_payload,
            col_type=TranslateColDataType("some-unsupported-type")
        )
