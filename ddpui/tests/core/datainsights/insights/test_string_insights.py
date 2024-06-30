import os
import django
from decimal import Decimal
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest

from ddpui.datainsights.insights.string_type.queries import (
    DistributionChart,
    StringLengthStats,
)
from ddpui.datainsights.insights.insight_interface import (
    MAP_TRANSLATE_TYPES,
    TranslateColDataType,
)
from ddpui.datainsights.insights.string_type.string_insights import StringColInsights


@pytest.fixture
def string_payload():
    """
    Fixture to create an instance of NumericColInsights
    """
    columns = [
        {
            "name": "col1",
            "data_type": int,
            "translated_type": TranslateColDataType.STRING,
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
def invalid_string_payload():
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


@pytest.fixture
def distribution_chart_query(string_payload):
    obj = StringColInsights(**string_payload)
    return obj.insights[0]


@pytest.fixture
def string_length_stats_query(string_payload):
    obj = StringColInsights(**string_payload)
    return obj.insights[1]


def test_string_insights_num_of_queries(string_payload):
    """
    Success test
    - checking no of queries to run
    - checking the type of queries
    """
    obj = StringColInsights(**string_payload)

    assert len(obj.insights) == 2
    assert isinstance(obj.insights[0], DistributionChart)
    assert isinstance(obj.insights[1], StringLengthStats)


def test_distribution_chart_query_when_no_col_specified(
    distribution_chart_query: DistributionChart,
):
    """Failure case: string insights should have atleast one column to generate sql"""
    distribution_chart_query.columns = []

    with pytest.raises(ValueError):
        distribution_chart_query.generate_sql()


def test_distribution_chart_query_data_type(
    distribution_chart_query: DistributionChart,
):
    assert distribution_chart_query.query_data_type() == TranslateColDataType.STRING


def test_string_length_stats_query_when_no_col_specified(
    string_length_stats_query: StringLengthStats,
):
    """Failure case: numeric insights should have atleast one column to generate sql"""
    string_length_stats_query.columns = []

    with pytest.raises(ValueError):
        string_length_stats_query.generate_sql()


def test_string_length_stats_query_data_type(
    string_length_stats_query: StringLengthStats,
):
    assert string_length_stats_query.query_data_type() == TranslateColDataType.STRING


def test_distribution_chart_query_parse_results(
    distribution_chart_query: DistributionChart,
):
    """Success test case of parsing the results of the query"""
    mock_results = [
        {"category": "NGO1", "count": 10},
        {"category": "NGO2", "count": 20},
    ]
    output = distribution_chart_query.parse_results(mock_results)

    assert distribution_chart_query.columns[0].name in output
    assert "charts" in output[distribution_chart_query.columns[0].name]
    assert len(output[distribution_chart_query.columns[0].name]["charts"]) == 1
    assert (
        output[distribution_chart_query.columns[0].name]["charts"][0]["data"]
        == mock_results
    )


def test_string_length_stats_query_validate_results(
    string_length_stats_query: StringLengthStats,
):
    """Success test case of validating the parsed results of the query"""
    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "mode": Decimal(2),
            "other_modes": [],
        }
    ]
    mock_output = string_length_stats_query.parse_results(mock_results)
    result_to_be_validated = mock_output[string_length_stats_query.columns[0].name]
    assert string_length_stats_query.validate_query_results(result_to_be_validated)


def test_data_stats_query_parse_results_failure(
    string_length_stats_query: StringLengthStats,
):
    """Failure test case of parsing the results of the query; missing keys"""

    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "other_modes": [],
        }
    ]
    with pytest.raises(KeyError):
        string_length_stats_query.parse_results(mock_results)


def test_string_length_stats_query_validate_results(
    string_length_stats_query: StringLengthStats,
):
    """Success test case of validating the parsed results of the query"""
    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "mode": Decimal(2),
            "other_modes": [],
        }
    ]
    mock_output = string_length_stats_query.parse_results(mock_results)
    result_to_be_validated = mock_output[string_length_stats_query.columns[0].name]
    assert string_length_stats_query.validate_query_results(result_to_be_validated)


def test_distribution_chart_query_validate_results(
    distribution_chart_query: DistributionChart,
):
    """Success test case of validating the parsed results of the query"""
    mock_results = [
        {
            "mean": Decimal(1.2),
            "median": Decimal(1.5),
            "mode": Decimal(2),
            "other_modes": [],
        }
    ]
    mock_output = distribution_chart_query.parse_results(mock_results)
    result_to_be_validated = mock_output[distribution_chart_query.columns[0].name]
    assert distribution_chart_query.validate_query_results(result_to_be_validated)


def test_distribution_chart_query_uniqueness_of_query_id(
    distribution_chart_query: DistributionChart,
):
    """Different payload should generated different query ids & hence a different hash"""
    query_id1 = distribution_chart_query.query_id()

    # for a differnet col under same table, schema; a new query id (hash) should be generated
    distribution_chart_query.columns[0].name = "col2"
    query_id2 = distribution_chart_query.query_id()

    assert query_id1 != query_id2

    # if i add a filter, the query id should change
    distribution_chart_query.filter = {"condition": 1}
    query_id3 = distribution_chart_query.query_id()

    assert query_id2 != query_id3


def test_string_length_stats_query_uniqueness_of_query_id(
    string_length_stats_query: StringLengthStats,
):
    """Different payload should generated different query ids & hence a different hash"""
    query_id1 = string_length_stats_query.query_id()

    # for a differnet col under same table, schema; a new query id (hash) should be generated
    string_length_stats_query.columns[0].name = "col2"
    query_id2 = string_length_stats_query.query_id()

    assert query_id1 != query_id2

    # if i add a filter, the query id should change
    string_length_stats_query.filter = {"condition": 1}
    query_id3 = string_length_stats_query.query_id()

    assert query_id2 != query_id3


# def test_data_stats_query_generate_sql():
#     """TODO"""
#     pass
