"""Tests for the public dashboard API endpoint (no authentication)

Tests:
1. get_public_dashboard — valid token returns org identity (org_slug + org_name),
   increments the access count, and rejects invalid / non-public tokens
"""

import os
import json
import django
import pytest
from unittest.mock import patch, MagicMock

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.test import RequestFactory
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.public_api import get_public_dashboard, get_public_map_data_overlay
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

rf = RequestFactory()


def _make_public_request(body=None):
    """A public endpoint request — no auth, but the handler reads IP / user agent"""
    if body:
        request = rf.post(
            "/api/v1/public/dashboards/", data=json.dumps(body), content_type="application/json"
        )
    else:
        request = rf.get("/api/v1/public/dashboards/")
    request.META["REMOTE_ADDR"] = "127.0.0.1"
    request.META["HTTP_USER_AGENT"] = "TestAgent"
    return request


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="pubdashuser", email="pubdashuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Public Dashboard Test Org",
        slug="pub-dash-test-org",
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


def _create_dashboard(orguser, org, **kwargs):
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [],
                "components": {},
            }
        ],
        created_by=orguser,
        org=org,
        **kwargs,
    )
    return dashboard


@pytest.fixture
def public_dashboard(orguser, org):
    dashboard = _create_dashboard(orguser, org, is_public=True, public_share_token="pub-dash-token")
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


@pytest.fixture
def private_dashboard(orguser, org):
    dashboard = _create_dashboard(orguser, org, is_public=False, public_share_token="priv-token")
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


class TestGetPublicDashboard:
    """Tests for the get_public_dashboard endpoint"""

    def test_valid_token_returns_org_identity(self, public_dashboard, seed_db):
        """org_slug rides along with org_name — anonymous public views have no
        person or group, so the slug is the only org attribution available."""
        request = _make_public_request()
        response = get_public_dashboard(request, public_dashboard.public_share_token)

        assert response.is_valid is True
        assert response.org_slug == "pub-dash-test-org"
        assert response.org_name == "Public Dashboard Test Org"
        assert response.id == public_dashboard.id

    def test_valid_token_increments_access_count(self, public_dashboard, seed_db):
        request = _make_public_request()
        get_public_dashboard(request, public_dashboard.public_share_token)
        get_public_dashboard(request, public_dashboard.public_share_token)

        public_dashboard.refresh_from_db()
        assert public_dashboard.public_access_count == 2
        assert public_dashboard.last_public_accessed is not None

    def test_invalid_token(self, seed_db):
        request = _make_public_request()
        status, response = get_public_dashboard(request, "nonexistent-token")

        assert status == 404
        assert response.is_valid is False
        assert "not found" in response.error.lower()

    def test_private_dashboard_not_accessible(self, private_dashboard, seed_db):
        """A dashboard with a token but is_public=False stays inaccessible"""
        request = _make_public_request()
        status, response = get_public_dashboard(request, private_dashboard.public_share_token)

        assert status == 404
        assert response.is_valid is False


@pytest.fixture
def map_chart(orguser, org):
    chart = Chart.objects.create(
        title="Public Map Chart",
        chart_type="map",
        schema_name="public",
        table_name="orders",
        extra_config={},
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
def org_warehouse(org):
    warehouse = OrgWarehouse.objects.create(wtype="postgres", credentials="{}", org=org)
    yield warehouse
    warehouse.delete()


class TestGetPublicMapDataOverlay:
    """Tests for get_public_map_data_overlay endpoint"""

    def test_dashboard_not_found(self, seed_db):
        request = _make_public_request(body={"geographic_column": "region"})
        status, response = get_public_map_data_overlay(request, "bad-token", chart_id=1)

        assert status == 404
        assert response.is_valid is False

    def test_chart_not_map_type_returns_404(self, public_dashboard, org, orguser, seed_db):
        """A chart_id belonging to a non-map chart is rejected."""
        bar_chart = Chart.objects.create(
            title="Bar Chart",
            chart_type="bar",
            schema_name="public",
            table_name="orders",
            extra_config={},
            created_by=orguser,
            org=org,
        )
        request = _make_public_request(body={})

        status, response = get_public_map_data_overlay(
            request, public_dashboard.public_share_token, chart_id=bar_chart.id
        )

        assert status == 404
        assert response.is_valid is False

    def test_success_overwrites_schema_and_table_from_chart(
        self, public_dashboard, map_chart, org_warehouse, seed_db
    ):
        """schema_name/table_name in the request body are ignored — the chart's
        own values are always used (closes the arbitrary-table gap)."""
        request = _make_public_request(
            body={
                "schema_name": "someone_elses_schema",
                "table_name": "someone_elses_table",
                "geographic_column": "region",
                "value_column": "amount",
                "aggregate_function": "sum",
            }
        )

        with patch(
            "ddpui.api.public_api.WarehouseFactory.get_warehouse_client"
        ) as mock_get_client, patch(
            "ddpui.api.public_api.charts_service.execute_map_data_overlay"
        ) as mock_execute:
            mock_get_client.return_value = MagicMock()
            mock_execute.return_value = {"data": [{"name": "Karnataka", "value": 5.0}], "count": 1}
            response = get_public_map_data_overlay(
                request, public_dashboard.public_share_token, chart_id=map_chart.id
            )

        assert response.get("is_valid") is True
        sent_map_payload = mock_execute.call_args[0][0]
        assert sent_map_payload.schema_name == "public"
        assert sent_map_payload.table_name == "orders"

    def test_dashboard_filters_resolved_and_passed(
        self, public_dashboard, map_chart, org_warehouse, seed_db
    ):
        request = _make_public_request(
            body={
                "geographic_column": "region",
                "value_column": "amount",
                "aggregate_function": "sum",
                "dashboard_filters": {"5": "2025-01-15"},
            }
        )

        with patch(
            "ddpui.api.public_api.WarehouseFactory.get_warehouse_client"
        ) as mock_get_client, patch(
            "ddpui.api.public_api.DashboardService.resolve_dashboard_filters_for_chart"
        ) as mock_resolve, patch(
            "ddpui.api.public_api.charts_service.execute_map_data_overlay"
        ) as mock_execute:
            mock_get_client.return_value = MagicMock()
            mock_resolve.return_value = [{"filter_id": "5", "value": "2025-01-15"}]
            mock_execute.return_value = {"data": [], "count": 0}
            get_public_map_data_overlay(
                request, public_dashboard.public_share_token, chart_id=map_chart.id
            )

        mock_resolve.assert_called_once()
        resolved_filters_arg = mock_execute.call_args[0][3]
        assert resolved_filters_arg == [{"filter_id": "5", "value": "2025-01-15"}]
