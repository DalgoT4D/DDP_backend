import os
from datetime import datetime
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

from ddpui.datainsights.insights.common.queries import BaseDataStats
from ddpui.datainsights.insights.insight_interface import (
    MAP_TRANSLATE_TYPES,
    TranslateColDataType,
)
from ddpui.datainsights.insights.common.base_insights import BaseInsights


@pytest.fixture
def base_payload():
    """
    Fixture to create an instance of NumericColInsights
    """
    columns = [
        {
            "name": "col1",
            "data_type": int,
            "translated_type": TranslateColDataType.NUMERIC,
        },
        {
            "name": "col2",
            "data_type": int,
            "translated_type": TranslateColDataType.STRING,
        },
        {
            "name": "col3",
            "data_type": datetime,
            "translated_type": TranslateColDataType.DATETIME,
        },
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
def invalid_base_payload():
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


def test_base_insights_num_of_queries(base_payload):
    """
    Success test
    - checking no of queries to run
    - checking the type of queries
    """
    obj = BaseInsights(**base_payload)

    assert len(obj.insights) == 1
    assert isinstance(obj.insights[0], BaseDataStats)


# ============================== Data stats query tests ================================


def test_data_stats_query_data_type(base_payload):
    obj = BaseInsights(**base_payload)

    data_stats_insights = obj.insights[0]
    assert data_stats_insights.query_data_type() == TranslateColDataType.BASE


def test_data_stats_query_parse_results(base_payload):
    """Success test case of parsing the results of the query"""

    obj = BaseInsights(**base_payload)

    data_stats_insights = obj.insights[0]

    mock_results_pre = [
        {
            f"count_{col['name']}": 10,
            f"countNull_{col['name']}": 20,
            f"countDistinct__{col['name']}": 10,
            f"maxVal_{col['name']}": (
                datetime.now()
                if col["translated_type"] == TranslateColDataType.DATETIME
                else 20
            ),
            f"minVal_{col['name']}": (
                datetime.now()
                if col["translated_type"] == TranslateColDataType.DATETIME
                else 20
            ),
        }
        for col in base_payload["columns"]
    ]
    # query output will be one huge dictionary
    # merge the mock dictionaries above
    mock_results = [{key: value for d in mock_results_pre for key, value in d.items()}]

    output = data_stats_insights.parse_results(mock_results)

    for col in data_stats_insights.columns:
        assert col.name in output
        assert output[col.name]["count"] == mock_results[0][f"count_{col.name}"]
        assert output[col.name]["countNull"] == mock_results[0][f"countNull_{col.name}"]
        assert (
            output[col.name]["countDistinct"]
            == mock_results[0][f"countDistinct__{col.name}"]
        )
        assert str(output[col.name]["maxVal"]) == str(
            mock_results[0][f"maxVal_{col.name}"]
        )
        assert str(output[col.name]["minVal"]) == str(
            mock_results[0][f"minVal_{col.name}"]
        )


def test_data_stats_query_parse_results_failure(base_payload):
    """Failure test case of parsing the results of the query; missing keys"""

    obj = BaseInsights(**base_payload)

    data_stats_insights = obj.insights[0]

    mock_results_pre = [
        {
            f"count_{col['name']}": 10,
            f"countNull_{col['name']}": 20,
            f"countDistinct__{col['name']}": 10,
            f"maxVal_{col['name']}": (
                datetime.now()
                if col["translated_type"] == TranslateColDataType.DATETIME
                else 20
            ),
        }
        for col in base_payload["columns"]
    ]
    # query output will be one huge dictionary
    # merge the mock dictionaries above
    mock_results = [{key: value for d in mock_results_pre for key, value in d.items()}]

    with pytest.raises(KeyError):
        data_stats_insights.parse_results(mock_results)


def test_data_stats_query_validate_results(base_payload):
    """Success test case of validating the parsed results of the query"""
    obj = BaseInsights(**base_payload)

    data_stats_insights = obj.insights[0]

    mock_results_pre = [
        {
            f"count_{col['name']}": 10,
            f"countNull_{col['name']}": 20,
            f"countDistinct__{col['name']}": 10,
            f"maxVal_{col['name']}": (
                datetime.now()
                if col["translated_type"] == TranslateColDataType.DATETIME
                else 20
            ),
            f"minVal_{col['name']}": (
                datetime.now()
                if col["translated_type"] == TranslateColDataType.DATETIME
                else 20
            ),
        }
        for col in base_payload["columns"]
    ]

    # query output will be one huge dictionary
    # merge the mock dictionaries above
    mock_results = [{key: value for d in mock_results_pre for key, value in d.items()}]

    mock_output = data_stats_insights.parse_results(mock_results)
    result_to_be_validated = mock_output[data_stats_insights.columns[0].name]
    assert data_stats_insights.validate_query_results(result_to_be_validated)


def test_data_stats_query_uniqueness_of_query_id(base_payload):
    """Different payload should generated different query ids & hence a different hash"""
    obj = BaseInsights(**base_payload)
    data_stats_insights = obj.insights[0]

    # if i add a filter, the query id should change
    base_payload["filter_"] = {"condition": 1}

    obj = BaseInsights(**base_payload)
    data_stats_insights2 = obj.insights[0]

    assert data_stats_insights.query_id() != data_stats_insights2.query_id()


# def test_data_stats_query_generate_sql():
#     """TODO"""
#     pass
