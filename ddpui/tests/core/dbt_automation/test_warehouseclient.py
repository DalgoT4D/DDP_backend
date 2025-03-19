import pytest
from unittest.mock import patch, Mock

from ddpui.dbt_automation.utils.warehouseclient import get_client


@patch("ddpui.dbt_automation.utils.warehouseclient.BigQueryClient")
@patch("ddpui.dbt_automation.utils.warehouseclient.PostgresClient")
def test_dbt_automation_warehouse_client(MockPostgresClient: Mock, MockBigqueryClient: Mock):
    """test the warehouse client"""
    conn_info = {
        "host": "localhost",
        "port": 5432,
        "user": "test",
        "password": "test",
        "database": "test",
    }

    warehouse_type = "postgres"
    get_client(warehouse_type, conn_info)
    MockPostgresClient.assert_called_once_with(conn_info)

    warehouse_type = "bigquery"
    location = "US"
    get_client(warehouse_type, conn_info, location)
    MockBigqueryClient.assert_called_once_with(conn_info, location)

    warehouse_type = "unknown"
    with pytest.raises(ValueError, match="unknown warehouse"):
        get_client(warehouse_type, conn_info)
