"""Tests for public report API endpoints (no authentication)

Tests:
1. get_public_report — valid token, invalid token, private report, increments access count
2. get_public_report_chart_data — valid, invalid token, no warehouse
3. get_public_report_table_data — valid, invalid token, no warehouse
4. get_public_report_table_total_rows — valid, invalid token
5. get_public_report_map_data — invalid token
"""

import os
import json
import django
from datetime import date
from unittest.mock import patch, MagicMock
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.test import RequestFactory
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot, SnapshotStatus
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.reports.report_service import ReportService
from ddpui.api.public_api import (
    get_public_report,
    get_public_report_chart_data,
    get_public_report_table_data,
    get_public_report_table_total_rows,
    get_public_report_map_data,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

rf = RequestFactory()


def _make_public_request(body=None):
    """Create a simple mock request for public endpoints (no auth needed)"""
    if body:
        request = rf.post(
            "/api/v1/public/reports/",
            data=json.dumps(body),
            content_type="application/json",
        )
    else:
        request = rf.get("/api/v1/public/reports/")
    request.META["REMOTE_ADDR"] = "127.0.0.1"
    request.META["HTTP_USER_AGENT"] = "TestAgent"
    return request


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="pubreportuser", email="pubreportuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Public Report Test Org",
        slug="pub-rpt-test-org",
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
def sample_dashboard(orguser, org):
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": 1, "chartType": "bar", "title": "Bar"},
            }
        },
        created_by=orguser,
        org=org,
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


@pytest.fixture
def sample_filter(sample_dashboard):
    f = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="Date Filter",
        filter_type="datetime",
        schema_name="public",
        table_name="orders",
        column_name="created_at",
        settings={},
        order=0,
    )
    yield f
    try:
        f.refresh_from_db()
        f.delete()
    except DashboardFilter.DoesNotExist:
        pass


@pytest.fixture
def sample_chart(orguser, org):
    chart = Chart.objects.create(
        id=1,
        title="Bar Chart",
        chart_type="bar",
        schema_name="public",
        table_name="orders",
        extra_config={"x_axis": "created_at"},
        created_by=orguser,
        org=org,
    )
    yield chart
    try:
        chart.refresh_from_db()
        chart.delete()
    except Chart.DoesNotExist:
        pass


@pytest.fixture
def public_snapshot(orguser, org, sample_dashboard, sample_filter, sample_chart):
    """A snapshot that has been made public"""
    from ddpui.api.report_api import toggle_report_sharing
    from ddpui.schemas.dashboard_schema import ShareToggle
    from ddpui.tests.api_tests.test_user_org_api import mock_request

    snapshot = ReportService.create_snapshot(
        title="Public Report",
        dashboard_id=sample_dashboard.id,
        date_column={
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "created_at",
        },
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 31),
        orguser=orguser,
    )

    # Make it public via the API
    request = mock_request(orguser)
    toggle_report_sharing(request, snapshot.id, ShareToggle(is_public=True))
    snapshot.refresh_from_db()

    yield snapshot
    try:
        snapshot.refresh_from_db()
        snapshot.delete()
    except ReportSnapshot.DoesNotExist:
        pass


@pytest.fixture
def private_snapshot(orguser, org, sample_dashboard, sample_filter, sample_chart):
    """A snapshot that is NOT public"""
    snapshot = ReportService.create_snapshot(
        title="Private Report",
        dashboard_id=sample_dashboard.id,
        date_column={
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "created_at",
        },
        period_start=date(2025, 1, 1),
        period_end=date(2025, 1, 31),
        orguser=orguser,
    )
    yield snapshot
    try:
        snapshot.refresh_from_db()
        snapshot.delete()
    except ReportSnapshot.DoesNotExist:
        pass


# ================================================================================
# Test get_public_report
# ================================================================================


class TestGetPublicReport:
    """Tests for get_public_report endpoint"""

    def test_valid_token(self, public_snapshot, seed_db):
        """Valid public token returns report view data"""
        request = _make_public_request()
        response = get_public_report(request, public_snapshot.public_share_token)

        assert response["is_valid"] is True
        assert response["org_name"] == "Public Report Test Org"
        assert "dashboard_data" in response
        assert "report_metadata" in response
        assert response["report_metadata"]["title"] == "Public Report"

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request()
        status, response = get_public_report(request, "nonexistent-token")

        assert status == 404
        assert response.is_valid is False
        assert "not found" in response.error.lower()

    def test_private_report_not_accessible(self, private_snapshot, seed_db):
        """A private report (no token) returns 404"""
        request = _make_public_request()
        # Private snapshot has no token, so any token lookup will fail
        status, response = get_public_report(request, "any-token")

        assert status == 404
        assert response.is_valid is False

    def test_increments_access_count(self, public_snapshot, seed_db):
        """Each public view increments access count"""
        initial_count = public_snapshot.public_access_count
        request = _make_public_request()

        get_public_report(request, public_snapshot.public_share_token)

        public_snapshot.refresh_from_db()
        assert public_snapshot.public_access_count == initial_count + 1

    def test_double_access_increments_twice(self, public_snapshot, seed_db):
        """Two views increment count by 2"""
        initial_count = public_snapshot.public_access_count
        request = _make_public_request()

        get_public_report(request, public_snapshot.public_share_token)
        get_public_report(request, public_snapshot.public_share_token)

        public_snapshot.refresh_from_db()
        assert public_snapshot.public_access_count == initial_count + 2


# ================================================================================
# Test get_public_report_chart_data
# ================================================================================


class TestGetPublicReportChartData:
    """Tests for get_public_report_chart_data endpoint"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request(
            body={"chart_type": "bar", "schema_name": "public", "table_name": "orders"}
        )
        status, response = get_public_report_chart_data(request, "bad-token")

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, seed_db):
        """No warehouse configured returns 404 error"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request(
                body={"chart_type": "bar", "schema_name": "public", "table_name": "orders"}
            )
            status, response = get_public_report_chart_data(
                request, public_snapshot.public_share_token
            )

            assert status == 404
            assert response.is_valid is False

    def test_valid_token_with_mocked_chart_data(self, public_snapshot, seed_db):
        """Valid token returns chart data (with mocked warehouse)"""
        mock_chart_result = {
            "data": [{"x": "Jan", "y": 100}],
            "config": {"type": "bar"},
        }

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.charts_api.generate_chart_data_and_config"
        ) as mock_gen:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_gen.return_value = mock_chart_result

            request = _make_public_request(
                body={"chart_type": "bar", "schema_name": "public", "table_name": "orders"}
            )
            response = get_public_report_chart_data(
                request, public_snapshot.public_share_token
            )

            assert response["is_valid"] is True
            assert response["data"] == [{"x": "Jan", "y": 100}]


# ================================================================================
# Test get_public_report_table_data
# ================================================================================


class TestGetPublicReportTableData:
    """Tests for get_public_report_table_data endpoint"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request(
            body={"chart_type": "table", "schema_name": "public", "table_name": "orders"}
        )
        status, response = get_public_report_table_data(request, "bad-token")

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, seed_db):
        """No warehouse configured returns 404"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request(
                body={"chart_type": "table", "schema_name": "public", "table_name": "orders"}
            )
            status, response = get_public_report_table_data(
                request, public_snapshot.public_share_token
            )

            assert status == 404
            assert response.is_valid is False

    def test_valid_token_with_mocked_data(self, public_snapshot, seed_db):
        """Valid token returns table preview data"""
        mock_preview = {
            "columns": ["id", "name"],
            "column_types": ["int", "str"],
            "data": [{"id": 1, "name": "Alice"}],
            "page": 0,
            "limit": 100,
        }

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_table_preview"
        ) as mock_preview_fn:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_preview_fn.return_value = mock_preview

            request = _make_public_request(
                body={"chart_type": "table", "schema_name": "public", "table_name": "orders"}
            )
            response = get_public_report_table_data(
                request, public_snapshot.public_share_token
            )

            assert response["is_valid"] is True
            assert response["columns"] == ["id", "name"]
            assert len(response["data"]) == 1


# ================================================================================
# Test get_public_report_table_total_rows
# ================================================================================


class TestGetPublicReportTableTotalRows:
    """Tests for get_public_report_table_total_rows endpoint"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request(
            body={"chart_type": "table", "schema_name": "public", "table_name": "orders"}
        )
        status, response = get_public_report_table_total_rows(request, "bad-token")

        assert status == 404
        assert response.is_valid is False

    def test_valid_token_with_mocked_total(self, public_snapshot, seed_db):
        """Valid token returns total row count"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_total_rows"
        ) as mock_total:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_total.return_value = 42

            request = _make_public_request(
                body={"chart_type": "table", "schema_name": "public", "table_name": "orders"}
            )
            response = get_public_report_table_total_rows(
                request, public_snapshot.public_share_token
            )

            assert response["is_valid"] is True
            assert response["total_rows"] == 42


# ================================================================================
# Test get_public_report_map_data
# ================================================================================


class TestGetPublicReportMapData:
    """Tests for get_public_report_map_data endpoint"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request(body={"schema_name": "public", "table_name": "orders"})
        status, response = get_public_report_map_data(request, "bad-token")

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, seed_db):
        """No warehouse configured returns 404"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request(
                body={
                    "schema_name": "public",
                    "table_name": "orders",
                    "geographic_column": "region",
                    "value_column": "amount",
                }
            )
            status, response = get_public_report_map_data(
                request, public_snapshot.public_share_token
            )

            assert status == 404
            assert response.is_valid is False
