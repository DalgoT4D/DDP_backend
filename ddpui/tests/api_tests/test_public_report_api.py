"""Tests for public report API endpoints (no authentication)

Tests:
1. get_public_report — valid token, invalid token, private report, increments access count
2. get_public_report_chart_data — valid, invalid token, no warehouse
3. get_public_report_table_data — valid, invalid token, no warehouse
4. get_public_report_table_total_rows — valid, invalid token
5. get_public_report_map_data — invalid token
6. get_public_filter_preview — dashboard token, report token, invalid token, private report
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
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.reports.report_service import ReportService
from ddpui.api.public_api import (
    get_public_report,
    get_public_report_chart_data,
    get_public_report_table_data,
    get_public_report_table_total_rows,
    get_public_report_map_data,
    get_public_filter_preview,
    get_public_report_filter_preview,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

rf = RequestFactory()


def _make_public_request(body=None, query_params=None):
    """Create a simple mock request for public endpoints (no auth needed)"""
    if body:
        request = rf.post(
            "/api/v1/public/reports/",
            data=json.dumps(body),
            content_type="application/json",
        )
    else:
        request = rf.get("/api/v1/public/reports/", data=query_params or {})
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
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
                "components": {
                    "chart-1": {
                        "id": "chart-1",
                        "type": "chart",
                        "config": {"chartId": 1, "chartType": "bar", "title": "Bar"},
                    }
                },
            }
        ],
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
    """A snapshot that has been made public via the unified general-access endpoint."""
    from ddpui.api.access_api import update_general_access
    from ddpui.schemas.access.resource_share_schema import GeneralAccessPayload
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

    request = mock_request(orguser)
    update_general_access(request, "report", str(snapshot.id), GeneralAccessPayload(mode="public"))
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

    @patch("ddpui.core.reports.report_service.ReportService._inject_period_into_chart_configs")
    def test_valid_token(self, mock_inject, public_snapshot, seed_db):
        """Valid public token returns report view data"""
        request = _make_public_request()
        response = get_public_report(request, public_snapshot.public_share_token)

        assert response["is_valid"] is True
        assert response["org_name"] == "Public Report Test Org"
        assert "dashboard_data" in response
        assert "report_metadata" in response
        assert response["report_metadata"]["title"] == "Public Report"

    @patch("ddpui.core.reports.report_service.ReportService._inject_period_into_chart_configs")
    def test_returns_org_slug(self, mock_inject, public_snapshot, seed_db):
        """The response carries the org's stable slug, not just its display name.

        A public view is anonymous — no user, no org group — so the payload is the only
        way analytics can attribute the read to an org. org_name can be renamed and is
        not a stable key; the slug is. Matches PublicDashboardResponse.
        """
        request = _make_public_request()
        response = get_public_report(request, public_snapshot.public_share_token)

        assert response["org_slug"] == "pub-rpt-test-org"

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
    """Tests for get_public_report_chart_data endpoint (GET with chart_id)"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request()
        status, response = get_public_report_chart_data(request, "bad-token", chart_id=1)

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, seed_db):
        """No warehouse configured returns 404 error"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request()
            status, response = get_public_report_chart_data(
                request, public_snapshot.public_share_token, chart_id=1
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
        ) as mock_gen, patch(
            "ddpui.core.reports.report_service.WarehouseFactory.get_warehouse_client"
        ):
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_gen.return_value = mock_chart_result

            request = _make_public_request()
            response = get_public_report_chart_data(
                request, public_snapshot.public_share_token, chart_id=1
            )

            assert response["is_valid"] is True
            assert response["data"] == [{"x": "Jan", "y": 100}]


# ================================================================================
# Test get_public_report_table_data
# ================================================================================


class TestGetPublicReportTableData:
    """Tests for get_public_report_table_data endpoint (GET with chart_id)"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request()
        status, response = get_public_report_table_data(request, "bad-token", chart_id=1)

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, seed_db):
        """No warehouse configured returns 404"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request()
            status, response = get_public_report_table_data(
                request, public_snapshot.public_share_token, chart_id=1
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
        ) as mock_preview_fn, patch(
            "ddpui.core.reports.report_service.WarehouseFactory.get_warehouse_client"
        ):
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_preview_fn.return_value = mock_preview

            request = _make_public_request()
            response = get_public_report_table_data(
                request, public_snapshot.public_share_token, chart_id=1
            )

            assert response["is_valid"] is True
            assert response["columns"] == ["id", "name"]
            assert len(response["data"]) == 1

    def test_dashboard_filters_resolved_and_passed(self, public_snapshot, seed_db):
        """A valid {filter_id: value} dashboard_filters query param is parsed
        and resolved against the frozen dashboard config."""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.WarehouseFactory.get_warehouse_client"
        ), patch(
            "ddpui.api.public_api.DashboardService.resolve_dashboard_filters_for_chart"
        ) as mock_resolve, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_table_preview"
        ) as mock_preview_fn:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_resolve.return_value = [{"filter_id": "5", "value": "2025-01-15"}]
            mock_preview_fn.return_value = {
                "columns": [],
                "column_types": {},
                "data": [],
                "page": 0,
                "limit": 100,
            }

            request = _make_public_request()
            get_public_report_table_data(
                request,
                public_snapshot.public_share_token,
                chart_id=1,
                dashboard_filters='{"5": "2025-01-15"}',
            )

            mock_resolve.assert_called_once()

    def test_non_dict_json_dashboard_filters_skips_resolution(self, public_snapshot, seed_db):
        """dashboard_filters='[1,2,3]' is valid JSON but not a dict — treated
        as no filters, same as the private report endpoints."""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.DashboardService.resolve_dashboard_filters_for_chart"
        ) as mock_resolve, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_table_preview"
        ) as mock_preview_fn:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_preview_fn.return_value = {
                "columns": [],
                "column_types": {},
                "data": [],
                "page": 0,
                "limit": 100,
            }

            request = _make_public_request()
            get_public_report_table_data(
                request,
                public_snapshot.public_share_token,
                chart_id=1,
                dashboard_filters="[1,2,3]",
            )

            mock_resolve.assert_not_called()


# ================================================================================
# Test get_public_report_table_total_rows
# ================================================================================


class TestGetPublicReportTableTotalRows:
    """Tests for get_public_report_table_total_rows endpoint (GET with chart_id)"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request()
        status, response = get_public_report_table_total_rows(request, "bad-token", chart_id=1)

        assert status == 404
        assert response.is_valid is False

    def test_valid_token_with_mocked_total(self, public_snapshot, seed_db):
        """Valid token returns total row count"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_total_rows"
        ) as mock_total, patch(
            "ddpui.core.reports.report_service.WarehouseFactory.get_warehouse_client"
        ):
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_total.return_value = 42

            request = _make_public_request()
            response = get_public_report_table_total_rows(
                request, public_snapshot.public_share_token, chart_id=1
            )

            assert response["is_valid"] is True
            assert response["total_rows"] == 42

    def test_dashboard_filters_resolved_and_passed(self, public_snapshot, seed_db):
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.WarehouseFactory.get_warehouse_client"
        ), patch(
            "ddpui.api.public_api.DashboardService.resolve_dashboard_filters_for_chart"
        ) as mock_resolve, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_total_rows"
        ) as mock_total:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_resolve.return_value = [{"filter_id": "5", "value": "2025-01-15"}]
            mock_total.return_value = 3

            request = _make_public_request()
            get_public_report_table_total_rows(
                request,
                public_snapshot.public_share_token,
                chart_id=1,
                dashboard_filters='{"5": "2025-01-15"}',
            )

            mock_resolve.assert_called_once()

    def test_non_dict_json_dashboard_filters_skips_resolution(self, public_snapshot, seed_db):
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.DashboardService.resolve_dashboard_filters_for_chart"
        ) as mock_resolve, patch(
            "ddpui.api.public_api.charts_service.get_chart_data_total_rows"
        ) as mock_total:
            mock_ow.filter.return_value.first.return_value = MagicMock()
            mock_total.return_value = 0

            request = _make_public_request()
            get_public_report_table_total_rows(
                request,
                public_snapshot.public_share_token,
                chart_id=1,
                dashboard_filters="[1,2,3]",
            )

            mock_resolve.assert_not_called()


# ================================================================================
# Test get_public_report_map_data
# ================================================================================


class TestGetPublicReportMapData:
    """Tests for get_public_report_map_data endpoint"""

    def test_invalid_token(self, seed_db):
        """Invalid token returns 404"""
        request = _make_public_request(body={"schema_name": "public", "table_name": "orders"})
        status, response = get_public_report_map_data(request, "bad-token", chart_id=1)

        assert status == 404
        assert response.is_valid is False

    def test_no_warehouse(self, public_snapshot, sample_chart, seed_db):
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
                request, public_snapshot.public_share_token, chart_id=sample_chart.id
            )

            assert status == 404
            assert response.is_valid is False

    def test_geographic_column_and_metrics_from_frozen_config_not_request(
        self, orguser, org, seed_db
    ):
        """schema_name/table_name/geographic_column/value_column in the request
        body are ignored — the frozen chart config always wins, including for
        a count-only chart whose saved value_column is None."""
        from ddpui.api.access_api import update_general_access
        from ddpui.schemas.access.resource_share_schema import GeneralAccessPayload
        from ddpui.tests.api_tests.test_user_org_api import mock_request

        map_chart = Chart.objects.create(
            title="Count Only Map",
            chart_type="map",
            schema_name="public",
            table_name="orders",
            extra_config={
                "geographic_column": "region",
                "value_column": None,
                "aggregate_function": "count",
            },
            created_by=orguser,
            org=org,
        )
        dashboard = Dashboard.objects.create(
            title="Map Dashboard",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "Tab 1",
                    "layout_config": [],
                    "components": {
                        "chart-map": {
                            "id": "chart-map",
                            "type": "chart",
                            "config": {"chartId": map_chart.id, "chartType": "map"},
                        }
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        OrgWarehouse.objects.create(wtype="postgres", credentials="{}", org=org)
        snapshot = ReportService.create_snapshot(
            title="Map Report",
            dashboard_id=dashboard.id,
            date_column={},  # no period-locking needed for this test
            orguser=orguser,
        )
        request = mock_request(orguser)
        update_general_access(
            request, "report", str(snapshot.id), GeneralAccessPayload(mode="public")
        )
        snapshot.refresh_from_db()

        request = _make_public_request(
            body={
                "schema_name": "someone_elses_schema",
                "table_name": "someone_elses_table",
                "geographic_column": "someone_elses_column",
                "value_column": "someone_elses_secret_column",
            }
        )
        with patch(
            "ddpui.api.public_api.WarehouseFactory.get_warehouse_client"
        ) as mock_get_client, patch(
            "ddpui.api.public_api.charts_service.execute_map_data_overlay"
        ) as mock_execute:
            mock_get_client.return_value = MagicMock()
            mock_execute.return_value = {"data": [], "count": 0}
            response = get_public_report_map_data(
                request, snapshot.public_share_token, chart_id=map_chart.id
            )

        assert response.get("is_valid") is True
        sent_map_payload = mock_execute.call_args[0][0]
        assert sent_map_payload.schema_name == "public"
        assert sent_map_payload.table_name == "orders"
        assert sent_map_payload.geographic_column == "region"
        assert sent_map_payload.value_column == "region"  # fallback placeholder
        assert sent_map_payload.metrics[0].column is None  # true COUNT(*)
        assert sent_map_payload.metrics[0].aggregation == "count"


# ================================================================================
# Fixtures for filter preview tests
# ================================================================================


@pytest.fixture
def public_dashboard(orguser, org):
    """A dashboard that has been made public with a share token"""
    import uuid

    dashboard = Dashboard.objects.create(
        title="Public Dashboard",
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
                "components": {
                    "chart-1": {
                        "id": "chart-1",
                        "type": "chart",
                        "config": {"chartId": 1, "chartType": "bar", "title": "Bar"},
                    }
                },
            }
        ],
        created_by=orguser,
        org=org,
        is_public=True,
        public_share_token=str(uuid.uuid4()),
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


# ================================================================================
# Test get_public_filter_preview
# ================================================================================


class TestGetPublicFilterPreview:
    """Tests for get_public_filter_preview endpoint — dashboard and report tokens"""

    def test_invalid_token_returns_404(self, seed_db):
        """Nonexistent token returns 404"""
        request = _make_public_request()
        status, response = get_public_filter_preview(
            request,
            token="nonexistent-token",
            schema_name="public",
            table_name="orders",
            column_name="status",
            filter_type="value",
        )

        assert status == 404
        assert response.is_valid is False

    def test_private_report_token_returns_404(self, private_snapshot, seed_db):
        """Private report snapshot token returns 404"""
        # Private snapshots have no token set, use a made-up one
        request = _make_public_request()
        status, response = get_public_filter_preview(
            request,
            token="private-nonexistent-token",
            schema_name="public",
            table_name="orders",
            column_name="status",
            filter_type="value",
        )

        assert status == 404
        assert response.is_valid is False

    def test_dashboard_token_value_filter(self, public_dashboard, seed_db):
        """Public dashboard token resolves org and returns value filter options"""
        mock_results = [{"value": "shipped", "count": 10}, {"value": "pending", "count": 5}]

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.execute_query"
        ) as mock_exec, patch("ddpui.api.public_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = _make_public_request()
            response = get_public_filter_preview(
                request,
                token=public_dashboard.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="value",
            )

            assert response.is_valid is True
            assert len(response.options) == 2
            assert response.options[0].value == "shipped"
            assert response.options[0].count == 10

    def test_report_token_value_filter(self, public_snapshot, seed_db):
        """Public report snapshot token resolves org and returns value filter options"""
        mock_results = [{"value": "active", "count": 20}, {"value": "inactive", "count": 3}]

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.execute_query"
        ) as mock_exec, patch("ddpui.api.public_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = _make_public_request()
            response = get_public_report_filter_preview(
                request,
                token=public_snapshot.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="value",
            )

            assert response.is_valid is True
            assert len(response.options) == 2
            assert response.options[0].value == "active"

    def test_report_token_numerical_filter(self, public_snapshot, seed_db):
        """Public report token works for numerical filter type"""
        mock_results = [
            {"min_value": 10.0, "max_value": 500.0, "avg_value": 120.5, "distinct_count": 45}
        ]

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.execute_query"
        ) as mock_exec, patch("ddpui.api.public_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = _make_public_request()
            response = get_public_report_filter_preview(
                request,
                token=public_snapshot.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="amount",
                filter_type="numerical",
            )

            assert response.is_valid is True
            assert response.stats["min_value"] == 10.0
            assert response.stats["max_value"] == 500.0

    def test_report_token_datetime_filter(self, public_snapshot, seed_db):
        """Public report token works for datetime filter type"""
        from datetime import date as dt_date

        mock_results = [
            {
                "min_date": dt_date(2024, 1, 1),
                "max_date": dt_date(2025, 6, 30),
                "distinct_days": 180,
                "total_records": 5000,
            }
        ]

        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.api.public_api.execute_query"
        ) as mock_exec, patch("ddpui.api.public_api.get_warehouse_client") as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()
            mock_exec.return_value = mock_results

            request = _make_public_request()
            response = get_public_report_filter_preview(
                request,
                token=public_snapshot.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="created_at",
                filter_type="datetime",
            )

            assert response.is_valid is True
            assert response.stats["min_date"] == "2024-01-01"
            assert response.stats["max_date"] == "2025-06-30"
            assert response.stats["distinct_days"] == 180
            assert response.stats["total_records"] == 5000

    def test_report_token_no_warehouse_returns_404(self, public_snapshot, seed_db):
        """Public report token with no warehouse configured returns error"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow:
            mock_ow.filter.return_value.first.return_value = None

            request = _make_public_request()
            status, response = get_public_filter_preview(
                request,
                token=public_snapshot.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="value",
            )

            assert status == 404
            assert response.is_valid is False

    def test_invalid_filter_type_returns_404(self, public_snapshot, seed_db):
        """Invalid filter_type returns 404"""
        with patch("ddpui.api.public_api.OrgWarehouse.objects") as mock_ow, patch(
            "ddpui.core.charts.charts_service.get_warehouse_client"
        ) as mock_wc:
            mock_ow.filter.return_value.first.return_value = MagicMock(wtype="postgres")
            mock_wc.return_value = MagicMock()

            request = _make_public_request()
            status, response = get_public_filter_preview(
                request,
                token=public_snapshot.public_share_token,
                schema_name="public",
                table_name="orders",
                column_name="status",
                filter_type="unknown_type",
            )

            assert status == 404
            assert response.is_valid is False


# ================================================================================
# allow_public_sharing runtime gate — spec: admin can revoke without touching
# individual resources; existing links must return 404 immediately.
# ================================================================================


class TestAllowPublicSharingGate:
    """Anonymous endpoints must respect the org-level allow_public_sharing toggle."""

    def test_dashboard_hidden_when_org_disallows(self, public_dashboard, org, seed_db):
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_dashboard

        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        request = _make_public_request()
        result = get_public_dashboard(request, public_dashboard.public_share_token)
        status, _ = result
        assert status == 404

    def test_dashboard_visible_when_org_allows(self, public_dashboard, org, seed_db):
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_dashboard

        OrgPreferences.objects.create(org=org, allow_public_sharing=True)
        request = _make_public_request()
        result = get_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(result, tuple)

    def test_report_hidden_when_org_disallows(self, public_snapshot, org, seed_db):
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_report

        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        request = _make_public_request()
        result = get_public_report(request, public_snapshot.public_share_token)
        status, _ = result
        assert status == 404

    def test_render_secret_bypasses_org_toggle(self, public_snapshot, org, seed_db):
        """Spec: server-side PDF rendering must work even when org disallows public sharing."""
        from django.conf import settings
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import _get_public_report_snapshot

        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        settings.RENDER_SECRET = "test-secret"
        request = _make_public_request()
        request.META["HTTP_X_RENDER_SECRET"] = "test-secret"
        result = _get_public_report_snapshot(public_snapshot.public_share_token, request=request)
        assert result.id == public_snapshot.id


# ================================================================================
# Story 7: Public link (O01, O02, O04, Q03)
# ================================================================================


class TestPublicLinkStory7:
    """Story 7 — Public link on/off + org toggle + anonymous inner chart access."""

    def test_O01_dashboard_with_public_link_and_org_allow_returns_200(
        self, public_dashboard, org, seed_db
    ):
        """Public link enabled on resource + allow_public_sharing=True → anonymous 200.
        Alias for test_dashboard_visible_when_org_allows in the gate class."""
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_dashboard

        OrgPreferences.objects.create(org=org, allow_public_sharing=True)
        request = _make_public_request()
        result = get_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(result, tuple)  # success is the response object, not (status, body)

    def test_O02_dashboard_public_link_disabled_returns_not_found(self, orguser, org, seed_db):
        """Resource-level is_public=False → anonymous GET returns 404."""
        from ddpui.api.public_api import get_public_dashboard

        d = Dashboard.objects.create(
            title="Not Public",
            org=org,
            created_by=orguser,
            is_public=False,
            public_share_token="token-nopub-1",
            dashboard_type="native",
            grid_columns=12,
        )
        try:
            request = _make_public_request()
            status, _ = get_public_dashboard(request, d.public_share_token)
            assert status == 404
        finally:
            d.delete()

    def test_O04_org_toggle_off_then_on_token_still_valid(self, public_dashboard, org, seed_db):
        """Toggle allow_public_sharing off → 404, then back on → same token works again."""
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_dashboard

        prefs = OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        request = _make_public_request()
        status, _ = get_public_dashboard(request, public_dashboard.public_share_token)
        assert status == 404

        prefs.allow_public_sharing = True
        prefs.save()
        result = get_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(result, tuple)  # visible again

    def test_Q03_public_dashboard_inner_chart_metadata_accessible_anonymously(
        self, public_dashboard, org, orguser, seed_db
    ):
        """Anonymous viewer of a public dashboard can fetch metadata for its
        inner charts regardless of any chart-level access controls — the
        chart is 'inherited' by being inside the public dashboard."""
        from ddpui.models.org_preferences import OrgPreferences
        from ddpui.api.public_api import get_public_chart_metadata

        OrgPreferences.objects.create(org=org, allow_public_sharing=True)
        # Create a chart in the dashboard's org (public_dashboard's tabs point at chartId=1
        # but there's no real chart 1 — create one with id=1 for the fixture layout).
        chart = Chart.objects.create(
            id=1,
            title="Inner Chart",
            org=org,
            chart_type="bar",
            schema_name="s",
            table_name="t",
            computation_type="raw",
            extra_config={},
            created_by=orguser,
        )
        try:
            request = _make_public_request()
            result = get_public_chart_metadata(
                request, public_dashboard.public_share_token, chart.id
            )
            # Success returns a dict (not the tuple error form)
            assert not isinstance(result, tuple)
            assert result["is_valid"] is True
        finally:
            chart.delete()
