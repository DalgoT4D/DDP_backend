import os
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

from ddpui.datainsights.insights.boolean_type.queries import DataStats
from ddpui.datainsights.insights.insight_interface import (
    MAP_TRANSLATE_TYPES,
    TranslateColDataType,
)
from ddpui.datainsights.insights.boolean_type.boolean_insights import BooleanColInsights


@pytest.fixture
def boolean_payload():
    """
    Fixture to create an instance of BooleanColInsights
    """
    columns = [
        {
            "name": "col1",
            "data_type": int,
            "translated_type": TranslateColDataType.NUMERIC,
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
def invalid_boolean_payload():
    """
    Fixture to create an instance of BooleanColInsights
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


def test_numeric_insights_num_of_queries(boolean_payload):
    """
    Success test
    - checking no of queries to run
    - checking the type of queries
    """
    obj = BooleanColInsights(**boolean_payload)

    assert len(obj.insights) == 1
    assert isinstance(obj.insights[0], DataStats)


# ============================== Data stats query tests ================================


def test_data_stats_query_when_no_col_specified(invalid_boolean_payload):
    """Failure case: numeric insights should have atleast one column to generate sql"""
    obj = BooleanColInsights(**invalid_boolean_payload)

    with pytest.raises(ValueError):
        obj.insights[0].generate_sql()


def test_data_stats_query_data_type(boolean_payload):
    obj = BooleanColInsights(**boolean_payload)

    data_stats_insights = obj.insights[0]
    assert data_stats_insights.query_data_type() == TranslateColDataType.BOOL


def test_data_stats_query_parse_results(boolean_payload):
    """Success test case of parsing the results of the query"""
    obj = BooleanColInsights(**boolean_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "countTrue": 50,
            "countFalse": 20,
        }
    ]
    output = data_stats_insights.parse_results(mock_results)

    assert data_stats_insights.columns[0].name in output
    assert output[data_stats_insights.columns[0].name]["countTrue"] == 50
    assert output[data_stats_insights.columns[0].name]["countFalse"] == 20


def test_data_stats_query_parse_results_failure(boolean_payload):
    """Failure test case of parsing the results of the query; missing keys"""

    obj = BooleanColInsights(**boolean_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "some_wrong_key": 50,
        }
    ]
    with pytest.raises(KeyError):
        data_stats_insights.parse_results(mock_results)


def test_data_stats_query_validate_results(boolean_payload):
    """Success test case of validating the parsed results of the query"""
    obj = BooleanColInsights(**boolean_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "countTrue": 50,
            "countFalse": 20,
        }
    ]
    mock_output = data_stats_insights.parse_results(mock_results)
    result_to_be_validated = mock_output[data_stats_insights.columns[0].name]
    assert data_stats_insights.validate_query_results(result_to_be_validated)


def test_data_stats_query_uniqueness_of_query_id(boolean_payload):
    """Different payload should generated different query ids & hence a different hash"""
    obj = BooleanColInsights(**boolean_payload)
    data_stats_insights = obj.insights[0]

    # for a differnet col under same table, schema; a new query id (hash) should be generated
    boolean_payload["columns"][0]["name"] = "col2"

    obj = BooleanColInsights(**boolean_payload)
    data_stats_insights1 = obj.insights[0]

    assert data_stats_insights.query_id() != data_stats_insights1.query_id()

    # if i add a filter, the query id should change
    boolean_payload["filter_"] = {"condition": 1}

    obj = BooleanColInsights(**boolean_payload)
    data_stats_insights2 = obj.insights[0]

    assert data_stats_insights1.query_id() != data_stats_insights2.query_id()


def test_data_stats_query_generate_sql():
    """TODO"""
    pass
