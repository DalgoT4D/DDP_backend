"""Tests for filter_api.py endpoints"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError
from django.contrib.auth.models import User

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.filter_api import (
    list_schemas,
    list_tables,
    list_columns,
    get_filter_preview,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="filter_api_testuser",
        email="filter_api_testuser@test.com",
        password="testpassword",
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Filter API Test Org",
        slug="filter-api-test",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def warehouse(org):
    wh = OrgWarehouse.objects.create(
        wtype="postgres",
        credentials="test-creds",
        org=org,
    )
    yield wh
    wh.delete()


@pytest.fixture
def bq_warehouse(org):
    wh = OrgWarehouse.objects.create(
        wtype="bigquery",
        credentials="test-creds",
        org=org,
        bq_location="my-project",
    )
    yield wh
    wh.delete()


# ================================================================================
# Tests for list_schemas
# ================================================================================


class TestListSchemas:
    def test_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_schemas(request)
        assert exc.value.status_code == 404
        assert "Warehouse not configured" in str(exc.value)

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_postgres_success(self, mock_get_client, mock_execute, orguser, warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {"schema_name": "public"},
            {"schema_name": "analytics"},
        ]
        request = mock_request(orguser)
        result = list_schemas(request)
        assert len(result) == 2
        assert result[0].name == "public"
        assert result[1].name == "analytics"

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_bigquery_success(self, mock_get_client, mock_execute, orguser, bq_warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [{"schema_name": "dataset1"}]
        request = mock_request(orguser)
        result = list_schemas(request)
        assert len(result) == 1
        assert result[0].name == "dataset1"

    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_unsupported_warehouse_type(self, mock_get_client, orguser, seed_db):
        wh = OrgWarehouse.objects.create(
            wtype="snowflake",
            credentials="test-creds",
            org=orguser.org,
        )
        mock_get_client.return_value = MagicMock()
        request = mock_request(orguser)
        # The snowflake type falls through to exception handler
        with pytest.raises(HttpError) as exc:
            list_schemas(request)
        assert exc.value.status_code in (400, 500)
        wh.delete()

    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_execution_error(self, mock_get_client, orguser, warehouse, seed_db):
        mock_get_client.side_effect = Exception("Connection failed")
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_schemas(request)
        assert exc.value.status_code == 500


# ================================================================================
# Tests for list_tables
# ================================================================================


class TestListTables:
    def test_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_tables(request, "public")
        assert exc.value.status_code == 404

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_postgres_success(self, mock_get_client, mock_execute, orguser, warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {"table_name": "orders"},
            {"table_name": "users"},
        ]
        request = mock_request(orguser)
        result = list_tables(request, "public")
        assert len(result) == 2
        assert result[0].name == "orders"
        assert result[1].name == "users"

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_bigquery_success(self, mock_get_client, mock_execute, orguser, bq_warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [{"table_name": "events"}]
        request = mock_request(orguser)
        result = list_tables(request, "dataset1")
        assert len(result) == 1
        assert result[0].name == "events"

    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_execution_error(self, mock_get_client, orguser, warehouse, seed_db):
        mock_get_client.side_effect = Exception("Connection error")
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_tables(request, "public")
        assert exc.value.status_code == 500


# ================================================================================
# Tests for list_columns
# ================================================================================


class TestListColumns:
    def test_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_columns(request, "public", "orders")
        assert exc.value.status_code == 404

    @patch("ddpui.api.filter_api._wh_funcs.determine_filter_type_from_column")
    @patch("ddpui.api.filter_api._wh_funcs.get_table_columns")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_success(
        self, mock_get_client, mock_get_columns, mock_filter_type, orguser, warehouse, seed_db
    ):
        mock_get_client.return_value = MagicMock()
        mock_get_columns.return_value = [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": "NO",
            },
            {
                "column_name": "name",
                "data_type": "character varying",
                "is_nullable": "YES",
            },
            {
                "column_name": "created_at",
                "data_type": "timestamp without time zone",
                "is_nullable": "YES",
            },
        ]
        mock_filter_type.side_effect = ["numerical", "value", "datetime"]

        request = mock_request(orguser)
        result = list_columns(request, "public", "orders")
        assert len(result) == 3
        assert result[0].name == "id"
        assert result[0].type == "number"
        assert result[0].nullable is False
        assert result[1].name == "name"
        assert result[1].type == "string"
        assert result[1].nullable is True
        assert result[2].name == "created_at"
        assert result[2].type == "date"

    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_execution_error(self, mock_get_client, orguser, warehouse, seed_db):
        mock_get_client.side_effect = Exception("Connection error")
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            list_columns(request, "public", "orders")
        assert exc.value.status_code == 500


# ================================================================================
# Tests for get_filter_preview
# ================================================================================


class TestGetFilterPreview:
    def test_no_warehouse(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_filter_preview(
                request,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="value",
            )
        assert exc.value.status_code == 404

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_value_filter_type(self, mock_get_client, mock_execute, orguser, warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {"value": "active", "count": 100},
            {"value": "inactive", "count": 50},
        ]
        request = mock_request(orguser)
        result = get_filter_preview(
            request,
            schema_name="public",
            table_name="orders",
            column_name="status",
            filter_type="value",
        )
        assert result.options is not None
        assert len(result.options) == 2
        assert result.options[0].label == "active"
        assert result.options[0].count == 100

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_numerical_filter_type(
        self, mock_get_client, mock_execute, orguser, warehouse, seed_db
    ):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {
                "min_value": 10.0,
                "max_value": 1000.0,
                "avg_value": 250.5,
                "distinct_count": 50,
            }
        ]
        request = mock_request(orguser)
        result = get_filter_preview(
            request,
            schema_name="public",
            table_name="orders",
            column_name="amount",
            filter_type="numerical",
        )
        assert result.stats is not None
        assert result.stats["min_value"] == 10.0
        assert result.stats["max_value"] == 1000.0
        assert result.stats["distinct_count"] == 50

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_datetime_filter_type_postgres(
        self, mock_get_client, mock_execute, orguser, warehouse, seed_db
    ):
        from datetime import date

        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {
                "min_date": date(2023, 1, 1),
                "max_date": date(2024, 12, 31),
                "distinct_days": 365,
                "total_records": 10000,
            }
        ]
        request = mock_request(orguser)
        result = get_filter_preview(
            request,
            schema_name="public",
            table_name="orders",
            column_name="created_at",
            filter_type="datetime",
        )
        assert result.stats is not None
        assert result.stats["min_date"] == "2023-01-01"
        assert result.stats["max_date"] == "2024-12-31"
        assert result.stats["distinct_days"] == 365
        assert result.stats["total_records"] == 10000

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_invalid_filter_type(self, mock_get_client, mock_execute, orguser, warehouse, seed_db):
        mock_get_client.return_value = MagicMock()
        request = mock_request(orguser)
        # The invalid filter type falls through to the exception handler
        with pytest.raises(HttpError) as exc:
            get_filter_preview(
                request,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="invalid",
            )
        assert exc.value.status_code in (400, 500)

    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_execution_error(self, mock_get_client, orguser, warehouse, seed_db):
        mock_get_client.side_effect = Exception("DB error")
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_filter_preview(
                request,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="value",
            )
        assert exc.value.status_code == 500

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_value_filter_with_null(
        self, mock_get_client, mock_execute, orguser, warehouse, seed_db
    ):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {"value": None, "count": 5},
            {"value": "active", "count": 100},
        ]
        request = mock_request(orguser)
        result = get_filter_preview(
            request,
            schema_name="public",
            table_name="orders",
            column_name="status",
            filter_type="value",
        )
        assert result.options[0].label == "NULL"
        assert result.options[0].value == ""

    @patch("ddpui.api.filter_api.execute_query")
    @patch("ddpui.api.filter_api.get_warehouse_client")
    def test_numerical_filter_with_nulls(
        self, mock_get_client, mock_execute, orguser, warehouse, seed_db
    ):
        mock_get_client.return_value = MagicMock()
        mock_execute.return_value = [
            {
                "min_value": None,
                "max_value": None,
                "avg_value": None,
                "distinct_count": None,
            }
        ]
        request = mock_request(orguser)
        result = get_filter_preview(
            request,
            schema_name="public",
            table_name="orders",
            column_name="amount",
            filter_type="numerical",
        )
        assert result.stats["min_value"] == 0.0
        assert result.stats["max_value"] == 100.0
        assert result.stats["avg_value"] == 50.0
        assert result.stats["distinct_count"] == 0


def test_seed_data(seed_db):
    """Test seed data loaded"""
    assert Role.objects.count() == 5
