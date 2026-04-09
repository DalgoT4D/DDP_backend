import os
import json
from unittest.mock import Mock, patch, MagicMock, call
import pytest
from ninja.errors import HttpError

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.warehousefunctions import (
    get_warehouse_data,
    fetch_warehouse_tables,
    get_table_columns,
    determine_filter_type_from_column,
)
from ddpui.models.dashboard import DashboardFilterType

pytestmark = pytest.mark.django_db


# =========================================================
# Tests for get_warehouse_data
# =========================================================
class TestGetWarehouseData:
    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    @patch("ddpui.core.warehousefunctions.convert_to_standard_types", side_effect=lambda x: x)
    def test_get_schemas(self, mock_convert, mock_get_wclient):
        """Test fetching schemas from the warehouse."""
        mock_client = Mock()
        mock_client.get_schemas.return_value = ["public", "analytics"]
        mock_get_wclient.return_value = mock_client

        mock_request = Mock()
        org_wh = Mock()
        result = get_warehouse_data(mock_request, "schemas", org_warehouse=org_wh)

        assert result == ["public", "analytics"]
        mock_client.get_schemas.assert_called_once()

    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    @patch("ddpui.core.warehousefunctions.convert_to_standard_types", side_effect=lambda x: x)
    def test_get_tables(self, mock_convert, mock_get_wclient):
        """Test fetching tables from the warehouse."""
        mock_client = Mock()
        mock_client.get_tables.return_value = ["users", "orders"]
        mock_get_wclient.return_value = mock_client

        mock_request = Mock()
        org_wh = Mock()
        result = get_warehouse_data(
            mock_request, "tables", org_warehouse=org_wh, schema_name="public"
        )

        assert result == ["users", "orders"]
        mock_client.get_tables.assert_called_once_with("public")

    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    @patch("ddpui.core.warehousefunctions.convert_to_standard_types", side_effect=lambda x: x)
    def test_get_table_columns_data_type(self, mock_convert, mock_get_wclient):
        """Test fetching table_columns data type."""
        mock_client = Mock()
        mock_client.get_table_columns.return_value = [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "varchar"},
        ]
        mock_get_wclient.return_value = mock_client

        mock_request = Mock()
        org_wh = Mock()
        result = get_warehouse_data(
            mock_request,
            "table_columns",
            org_warehouse=org_wh,
            schema_name="public",
            table_name="users",
        )

        assert len(result) == 2
        mock_client.get_table_columns.assert_called_once_with("public", "users")

    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    @patch("ddpui.core.warehousefunctions.convert_to_standard_types", side_effect=lambda x: x)
    def test_get_table_data(self, mock_convert, mock_get_wclient):
        """Test fetching table_data."""
        mock_client = Mock()
        mock_client.get_table_data.return_value = [{"id": 1}]
        mock_get_wclient.return_value = mock_client

        mock_request = Mock()
        org_wh = Mock()
        result = get_warehouse_data(
            mock_request,
            "table_data",
            org_warehouse=org_wh,
            schema_name="public",
            table_name="users",
            limit=10,
            page=1,
            order_by="id",
            order=1,
        )

        assert result == [{"id": 1}]
        mock_client.get_table_data.assert_called_once_with(
            schema="public", table="users", limit=10, page=1, order_by="id", order=1
        )

    @patch("ddpui.core.warehousefunctions.OrgWarehouse.objects")
    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    @patch("ddpui.core.warehousefunctions.convert_to_standard_types", side_effect=lambda x: x)
    def test_get_warehouse_data_uses_orguser_when_no_org_warehouse(
        self, mock_convert, mock_get_wclient, mock_ow_objects
    ):
        """When org_warehouse is not passed, it fetches from request.orguser."""
        mock_client = Mock()
        mock_client.get_schemas.return_value = ["default"]
        mock_get_wclient.return_value = mock_client

        org_wh = Mock()
        mock_ow_objects.filter.return_value.first.return_value = org_wh

        mock_request = Mock()
        mock_request.orguser.org = Mock()

        result = get_warehouse_data(mock_request, "schemas")
        assert result == ["default"]

    @patch("ddpui.core.warehousefunctions.dbtautomation_service._get_wclient")
    def test_get_warehouse_data_exception(self, mock_get_wclient):
        """Test that exceptions raise HttpError 500."""
        mock_get_wclient.side_effect = Exception("Connection failed")

        mock_request = Mock()
        org_wh = Mock()
        with pytest.raises(HttpError) as excinfo:
            get_warehouse_data(mock_request, "schemas", org_warehouse=org_wh)
        assert excinfo.value.status_code == 500


# =========================================================
# Tests for fetch_warehouse_tables
# =========================================================
class TestFetchWarehouseTables:
    @patch("ddpui.core.warehousefunctions.RedisClient")
    @patch("ddpui.core.warehousefunctions.get_warehouse_data")
    def test_fetch_warehouse_tables_basic(self, mock_get_data, mock_redis):
        """Fetch tables from all schemas."""

        def side_effect(req, data_type, **kwargs):
            if data_type == "schemas":
                return ["public", "staging"]
            if data_type == "tables":
                schema = kwargs.get("schema_name")
                if schema == "public":
                    return ["users", "orders"]
                return ["stg_users"]

        mock_get_data.side_effect = side_effect
        mock_request = Mock()
        org_wh = Mock()

        result = fetch_warehouse_tables(mock_request, org_wh)

        assert len(result) == 3
        assert result[0]["schema"] == "public"
        assert result[0]["name"] == "users"
        assert result[0]["type"] == "source"
        assert result[0]["id"] == "public-users"

    @patch("ddpui.core.warehousefunctions.RedisClient")
    @patch("ddpui.core.warehousefunctions.get_warehouse_data")
    def test_fetch_warehouse_tables_with_cache(self, mock_get_data, mock_redis):
        """Caches results in redis when cache_key is provided."""
        mock_get_data.side_effect = lambda req, dt, **kw: (
            ["public"] if dt == "schemas" else ["t1"]
        )

        mock_redis_instance = Mock()
        mock_redis.get_instance.return_value = mock_redis_instance

        mock_request = Mock()
        org_wh = Mock()

        result = fetch_warehouse_tables(mock_request, org_wh, cache_key="my-cache-key")

        assert len(result) == 1
        mock_redis_instance.set.assert_called_once()
        args = mock_redis_instance.set.call_args
        assert args[0][0] == "my-cache-key"

    @patch("ddpui.core.warehousefunctions.RedisClient")
    @patch("ddpui.core.warehousefunctions.get_warehouse_data")
    def test_fetch_warehouse_tables_no_schemas(self, mock_get_data, mock_redis):
        """Empty schemas returns empty list."""
        mock_get_data.side_effect = lambda req, dt, **kw: []

        result = fetch_warehouse_tables(Mock(), Mock())
        assert result == []


# =========================================================
# Tests for get_table_columns
# =========================================================
class TestGetTableColumns:
    @patch("ddpui.core.warehousefunctions.execute_query")
    def test_postgres_table_columns(self, mock_execute_query):
        """Test building query for postgres warehouse."""
        mock_execute_query.return_value = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"}
        ]
        wh = Mock()
        wh.wtype = "postgres"
        client = Mock()

        result = get_table_columns(client, wh, "public", "users")

        assert result == [{"column_name": "id", "data_type": "integer", "is_nullable": "NO"}]
        mock_execute_query.assert_called_once()

    @patch("ddpui.core.warehousefunctions.execute_query")
    def test_bigquery_table_columns(self, mock_execute_query):
        """Test building query for bigquery warehouse."""
        mock_execute_query.return_value = [
            {"column_name": "name", "data_type": "STRING", "is_nullable": "YES"}
        ]
        wh = Mock()
        wh.wtype = "bigquery"
        wh.bq_location = "us-central1"
        client = Mock()

        result = get_table_columns(client, wh, "dataset", "my_table")

        assert result == [{"column_name": "name", "data_type": "STRING", "is_nullable": "YES"}]
        mock_execute_query.assert_called_once()

    def test_unsupported_warehouse_type(self):
        """Unsupported warehouse type returns empty list."""
        wh = Mock()
        wh.wtype = "snowflake"
        client = Mock()

        result = get_table_columns(client, wh, "schema", "table")
        assert result == []


# =========================================================
# Tests for determine_filter_type_from_column
# =========================================================
class TestDetermineFilterTypeFromColumn:
    @pytest.mark.parametrize(
        "data_type",
        ["timestamp", "datetime", "date", "timestamptz", "time", "TIMESTAMP", "DATE"],
    )
    def test_datetime_types(self, data_type):
        assert determine_filter_type_from_column(data_type) == DashboardFilterType.DATETIME.value

    @pytest.mark.parametrize(
        "data_type",
        [
            "integer",
            "bigint",
            "numeric",
            "decimal",
            "double",
            "real",
            "float",
            "money",
            "FLOAT",
            "NUMERIC",
        ],
    )
    def test_numerical_types(self, data_type):
        assert determine_filter_type_from_column(data_type) == DashboardFilterType.NUMERICAL.value

    @pytest.mark.parametrize(
        "data_type",
        ["varchar", "text", "char", "boolean", "json", "TEXT"],
    )
    def test_value_types(self, data_type):
        assert determine_filter_type_from_column(data_type) == DashboardFilterType.VALUE.value
