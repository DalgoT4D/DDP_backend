import os
import django
from django.core.management import call_command
from django.apps import apps

import pytest
from unittest.mock import patch

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from ddpui.core.datainsights.insights.insight_factory import InsightsFactory
from ddpui.core.datainsights.insights.insight_interface import (
    TranslateColDataType,
    DataTypeColInsights,
)
from ddpui.core.datainsights.insights.numeric_type.numeric_insight import NumericColInsights
from ddpui.core.datainsights.insights.string_type.string_insights import StringColInsights
from ddpui.core.datainsights.insights.boolean_type.boolean_insights import BooleanColInsights
from ddpui.core.datainsights.insights.datetime_type.datetime_insight import (
    DatetimeColInsights,
)


class MockClass:
    def __ini__(self):
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

    with patch(
        "ddpui.core.datainsights.insights.numeric_type.numeric_insight.NumericColInsights",
        return_value=MockClass(),
    ) as MockNumericColInsights:
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.NUMERIC
        )
        assert isinstance(obj, NumericColInsights)

    with patch(
        "ddpui.core.datainsights.insights.string_type.string_insights.StringColInsights",
        return_value=MockClass(),
    ) as MockStringColInsights:
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.STRING
        )
        assert isinstance(obj, StringColInsights)

    with patch(
        "ddpui.core.datainsights.insights.boolean_type.boolean_insights.BooleanColInsights",
        return_value=MockClass(),
    ) as MockBooleanColInsights:
        MockBooleanColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.BOOL
        )
        assert isinstance(obj, BooleanColInsights)

    with patch(
        "ddpui.core.datainsights.insights.datetime_type.datetime_insight.DatetimeColInsights",
        return_value=MockClass(),
    ) as MockDatetimeColInsights:
        MockDatetimeColInsights.return_value = MockClass()
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.DATETIME
        )
        assert isinstance(obj, DatetimeColInsights)

    # json calls the general parent class
    with patch(
        "ddpui.core.datainsights.insights.insight_interface.DataTypeColInsights",
        return_value=MockClass(),
    ) as MockJsonColInsights:
        obj = InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType.JSON
        )
        assert isinstance(obj, DataTypeColInsights)

    with pytest.raises(ValueError):
        InsightsFactory.initiate_insight(
            **dummy_insight_payload, col_type=TranslateColDataType("some-unsupported-type")
        )
