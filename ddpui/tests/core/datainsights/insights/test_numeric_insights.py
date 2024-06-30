import os
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

from ddpui.datainsights.insights.numeric_type.queries import DataStats
from ddpui.datainsights.insights.insight_interface import (
    MAP_TRANSLATE_TYPES,
    TranslateColDataType,
)
from ddpui.datainsights.insights.numeric_type.numeric_insight import NumericColInsights


@pytest.fixture
def numeric_payload():
    """
    Fixture to create an instance of NumericColInsights
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
def invalid_numeric_payload():
    """
    Fixture to create an instance of NumericColInsights
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


def test_numeric_insights_num_of_queries(numeric_payload):
    """
    Success test
    - checking no of queries to run
    - checking the type of queries
    """
    obj = NumericColInsights(**numeric_payload)

    assert len(obj.insights) == 1
    assert isinstance(obj.insights[0], DataStats)


def test_data_stats_query_when_no_col_specified(invalid_numeric_payload):
    """Failure case: numeric insights should have atleast one column to generate sql"""
    obj = NumericColInsights(**invalid_numeric_payload)

    with pytest.raises(ValueError):
        obj.insights[0].generate_sql()


def test_data_stats_query_data_type(numeric_payload):
    obj = NumericColInsights(**numeric_payload)

    data_stats_insights = obj.insights[0]
    assert data_stats_insights.query_data_type() == TranslateColDataType.NUMERIC


def test_data_stats_query_parse_results(numeric_payload):
    """Success test case of parsing the results of the query"""
    obj = NumericColInsights(**numeric_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "mode": Decimal(2),
            "other_modes": [],
        }
    ]
    output = data_stats_insights.parse_results(mock_results)

    assert data_stats_insights.columns[0].name in output
    assert output[data_stats_insights.columns[0].name]["mean"] == 1.2
    assert output[data_stats_insights.columns[0].name]["median"] == 1.5
    assert output[data_stats_insights.columns[0].name]["mode"] == 2


def test_data_stats_query_parse_results_failure(numeric_payload):
    """Failure test case of parsing the results of the query; missing keys"""

    obj = NumericColInsights(**numeric_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "other_modes": [],
        }
    ]
    with pytest.raises(KeyError):
        data_stats_insights.parse_results(mock_results)


def test_data_stats_query_validate_results(numeric_payload):
    """Success test case of validating the parsed results of the query"""
    obj = NumericColInsights(**numeric_payload)

    data_stats_insights = obj.insights[0]

    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "mode": Decimal(2),
            "other_modes": [],
        }
    ]
    mock_output = data_stats_insights.parse_results(mock_results)
    result_to_be_validated = mock_output[data_stats_insights.columns[0].name]
    assert data_stats_insights.validate_query_results(result_to_be_validated)


def test_data_stats_query_uniqueness_of_query_id(numeric_payload):
    """Different payload should generated different query ids & hence a different hash"""
    obj = NumericColInsights(**numeric_payload)
    data_stats_insights = obj.insights[0]

    # for a differnet col under same table, schema; a new query id (hash) should be generated
    numeric_payload["columns"][0]["name"] = "col2"

    obj = NumericColInsights(**numeric_payload)
    data_stats_insights1 = obj.insights[0]

    assert data_stats_insights.query_id() != data_stats_insights1.query_id()

    # if i add a filter, the query id should change
    numeric_payload["filter_"] = {"condition": 1}

    obj = NumericColInsights(**numeric_payload)
    data_stats_insights2 = obj.insights[0]

    assert data_stats_insights1.query_id() != data_stats_insights2.query_id()


def test_data_stats_query_generate_sql():
    """TODO"""
    pass
