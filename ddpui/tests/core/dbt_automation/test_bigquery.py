import os
import pytest
from unittest.mock import patch, MagicMock, mock_open
from ddpui.dbt_automation.utils.bigquery import BigQueryClient


@pytest.fixture
def mock_bigquery_client():
    with patch("ddpui.dbt_automation.utils.bigquery.bigquery.Client") as mock_client:
        yield mock_client


@pytest.fixture
def mock_open_env():
    with patch("builtins.open", mock_open(read_data='{"project_id": "test_project"}')), patch.dict(
        os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "fake_path"}
    ):
        yield


@pytest.fixture
def mock_from_service_account_info():
    with patch("google.oauth2.service_account.Credentials.from_service_account_info") as mock_creds:
        yield mock_creds


def test_get_table_data_default(
    mock_bigquery_client, mock_open_env, mock_from_service_account_info
):
    mock_client_instance = mock_bigquery_client.return_value
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]
    mock_client_instance.query.return_value = mock_query_job

    bq_client = BigQueryClient()
    result = bq_client.get_table_data("test_schema", "test_table", 10)

    assert len(result) == 2
    assert result[0]["column1"] == "value1"
    assert result[1]["column1"] == "value3"
    assert result[0]["column2"] == "value2"
    assert result[1]["column2"] == "value4"

    # Verify SQL construction
    expected_sql = """
            SELECT * 
            FROM `test_schema`.`test_table`
            
            LIMIT 10 OFFSET 0
            """
    mock_client_instance.query.assert_called_once_with(expected_sql, location="asia-south1")


def test_get_table_data_with_order(
    mock_bigquery_client, mock_open_env, mock_from_service_account_info
):
    mock_client_instance = mock_bigquery_client.return_value
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]
    mock_client_instance.query.return_value = mock_query_job

    bq_client = BigQueryClient()
    result = bq_client.get_table_data("test_schema", "test_table", 10, order_by="column1", order=1)

    assert len(result) == 2
    assert result[0]["column1"] == "value1"
    assert result[1]["column1"] == "value3"
    assert result[0]["column2"] == "value2"
    assert result[1]["column2"] == "value4"

    # Verify SQL construction with ORDER BY
    expected_sql = """
            SELECT * 
            FROM `test_schema`.`test_table`
            
            ORDER BY `column1` ASC
            
            LIMIT 10 OFFSET 0
            """
    mock_client_instance.query.assert_called_once_with(expected_sql, location="asia-south1")


def test_get_table_data_with_pagination(
    mock_bigquery_client, mock_open_env, mock_from_service_account_info
):
    mock_client_instance = mock_bigquery_client.return_value
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]
    mock_client_instance.query.return_value = mock_query_job

    bq_client = BigQueryClient()
    result = bq_client.get_table_data("test_schema", "test_table", 10, page=2)

    assert len(result) == 2
    assert result[0]["column1"] == "value1"
    assert result[1]["column1"] == "value3"
    assert result[0]["column2"] == "value2"
    assert result[1]["column2"] == "value4"

    # Verify SQL construction with pagination
    expected_sql = """
            SELECT * 
            FROM `test_schema`.`test_table`
            
            LIMIT 10 OFFSET 10
            """
    mock_client_instance.query.assert_called_once_with(expected_sql, location="asia-south1")
