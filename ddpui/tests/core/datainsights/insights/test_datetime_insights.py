import os
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

from ddpui.datainsights.insights.datetime_type.queries import DistributionChart
from ddpui.datainsights.insights.insight_interface import (
    MAP_TRANSLATE_TYPES,
    TranslateColDataType,
)
from ddpui.datainsights.insights.datetime_type.datetime_insight import (
    DatetimeColInsights,
)


@pytest.fixture
def datetime_payload():
    """
    Fixture to create an instance of DatetimeColInsights
    """
    columns = [
        {
            "name": "col1",
            "data_type": int,
            "translated_type": TranslateColDataType.DATETIME,
        }
    ]
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


@pytest.fixture
def invalid_datetime_payload():
    """
    Fixture to create an instance of DatetimeColInsights
    """
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


def test_numeric_insights_num_of_queries(datetime_payload):
    """
    Success test
    - checking no of queries to run
    - checking the type of queries
    """
    obj = DatetimeColInsights(**datetime_payload)

    assert len(obj.insights) == 1
    assert isinstance(obj.insights[0], DistributionChart)


# ============================== Distributio chart query tests ================================


def test_distribution_chart_query_when_no_col_specified(invalid_datetime_payload):
    """Failure case: numeric insights should have atleast one column to generate sql"""
    obj = DatetimeColInsights(**invalid_datetime_payload)

    with pytest.raises(ValueError):
        obj.insights[0].generate_sql()


def test_distribution_chart_query_data_type(datetime_payload):
    obj = DatetimeColInsights(**datetime_payload)

    distribution_chart_query = obj.insights[0]
    assert distribution_chart_query.query_data_type() == TranslateColDataType.DATETIME


def test_distribution_chart_query_parse_results(datetime_payload):
    """Success test case of parsing the results of the query"""
    obj = DatetimeColInsights(**datetime_payload)

    distribution_chart_query = obj.insights[0]

    mock_results = [{"frequency": 10, "year": 2021, "month": 1, "day": 1}]
    output = distribution_chart_query.parse_results(mock_results)

    assert distribution_chart_query.columns[0].name in output
    assert "charts" in output[distribution_chart_query.columns[0].name]
    assert len(output[distribution_chart_query.columns[0].name]["charts"]) == 1
    assert output[distribution_chart_query.columns[0].name]["charts"][0]["data"] == mock_results


def test_distribution_chart_query_validate_results(datetime_payload):
    """Success test case of validating the parsed results of the query"""
    obj = DatetimeColInsights(**datetime_payload)

    distribution_chart_query = obj.insights[0]

    mock_results = [{"frequency": 10, "year": 2021, "month": 1, "day": 1}]
    mock_output = distribution_chart_query.parse_results(mock_results)
    result_to_be_validated = mock_output[distribution_chart_query.columns[0].name]
    assert distribution_chart_query.validate_query_results(result_to_be_validated)


def test_distribution_chart_query_uniqueness_of_query_id(datetime_payload):
    """Different payload should generated different query ids & hence a different hash"""
    obj = DatetimeColInsights(**datetime_payload)
    distribution_chart_query = obj.insights[0]

    # for a differnet col under same table, schema; a new query id (hash) should be generated
    datetime_payload["columns"][0]["name"] = "col2"

    obj = DatetimeColInsights(**datetime_payload)
    distribution_chart_query1 = obj.insights[0]

    assert distribution_chart_query.query_id() != distribution_chart_query1.query_id()

    # if i add a filter, the query id should change
    datetime_payload["filter_"] = {"range": "month", "limit": 10, "offset": 0}

    obj = DatetimeColInsights(**datetime_payload)
    distribution_chart_query2 = obj.insights[0]

    assert distribution_chart_query1.query_id() != distribution_chart_query2.query_id()


def test_distribution_chart_query_invalid_filter_keys(datetime_payload):
    """Failure case: Invalid filter keys"""

    # missing keys
    datetime_payload["filter_"] = {"limit": 10, "offset": 0}

    with pytest.raises(TypeError):
        DatetimeColInsights(**datetime_payload)

    # unexpected key
    datetime_payload["filter_"] = {
        "range": "month",
        "limit": 10,
        "offset": 0,
        "some_random_key": "random_value",
    }

    with pytest.raises(TypeError):
        DatetimeColInsights(**datetime_payload)


def test_distribution_chart_query_generate_sql():
    """TODO"""
    pass
