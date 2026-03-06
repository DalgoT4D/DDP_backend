"""API Tests for Report/Snapshot endpoints

Tests:
1. list_snapshots - empty, with data, search
2. create_snapshot - success, without start date, invalid date range, dashboard not found,
                     invalid date column
3. get_snapshot_view - success, not found
4. update_snapshot_summary - success, not found
5. delete_snapshot - success, not found
"""

import os
import django
from datetime import date
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot, SnapshotStatus
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.report_api import (
    list_snapshots,
    create_snapshot,
    get_snapshot_view,
    update_snapshot,
    delete_snapshot,
)
from ddpui.schemas.report_schema import SnapshotCreate, SnapshotUpdate, DateColumnSchema
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="reportapiuser", email="reportapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Report API Test Org",
        slug="rpt-api-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    """An OrgUser with account manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_dashboard(orguser, org):
    """A sample dashboard with components and filters for testing"""
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test Description",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "chart-123", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={
            "chart-123": {
                "id": "chart-123",
                "type": "chart",
                "config": {"chartId": 1, "chartType": "bar", "title": "Test Bar Chart"},
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
    """A sample datetime filter for testing"""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="Date Filter",
        filter_type="datetime",
        schema_name="public",
        table_name="orders",
        column_name="created_at",
        settings={"default_start_date": "2024-01-01"},
        order=0,
    )
    yield filter_obj
    try:
        filter_obj.refresh_from_db()
        filter_obj.delete()
    except DashboardFilter.DoesNotExist:
        pass


@pytest.fixture
def sample_chart(orguser, org):
    """A sample chart referenced by the dashboard"""
    chart = Chart.objects.create(
        id=1,
        title="Test Bar Chart",
        description="A test chart",
        chart_type="bar",
        schema_name="public",
        table_name="orders",
        extra_config={
            "x_axis": "created_at",
            "metrics": [{"column": "amount", "aggregation": "SUM"}],
        },
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
def sample_snapshot(orguser, org, sample_dashboard, sample_filter, sample_chart):
    """A pre-created snapshot for testing"""
    from ddpui.core.reports.report_service import ReportService

    snapshot = ReportService.create_snapshot(
        title="January 2025 Report",
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
# Test list_snapshots endpoint
# ================================================================================


class TestListSnapshots:
    """Tests for list_snapshots endpoint"""

    def test_list_empty(self, orguser, seed_db):
        """Test listing when no snapshots exist"""
        request = mock_request(orguser)
        response = list_snapshots(request)
        assert len(response) == 0

    def test_list_with_data(self, orguser, sample_snapshot, seed_db):
        """Test listing returns snapshots"""
        request = mock_request(orguser)
        response = list_snapshots(request)
        assert len(response) == 1
        assert response[0].title == "January 2025 Report"
        assert response[0].date_column is not None
        assert response[0].date_column["column_name"] == "created_at"

    def test_list_with_search(self, orguser, sample_snapshot, seed_db):
        """Test listing with search filter"""
        request = mock_request(orguser)

        response = list_snapshots(request, search="January")
        assert len(response) == 1

        response = list_snapshots(request, search="nonexistent")
        assert len(response) == 0


# ================================================================================
# Test create_snapshot endpoint
# ================================================================================


class TestCreateSnapshot:
    """Tests for create_snapshot endpoint"""

    def test_create_with_both_dates(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test creating a snapshot with both start and end date"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Q1 Report",
            dashboard_id=sample_dashboard.id,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_start=date(2025, 1, 1),
            period_end=date(2025, 3, 31),
        )
        response = create_snapshot(request, payload)
        assert response.title == "Q1 Report"
        assert response.period_start == date(2025, 1, 1)
        assert response.period_end == date(2025, 3, 31)
        assert response.dashboard_title == "Test Dashboard"
        assert response.date_column["column_name"] == "created_at"
        # Cleanup
        ReportSnapshot.objects.filter(id=response.id).delete()

    def test_create_without_start_date(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test creating a snapshot without a start date (no lower bound)"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Ongoing Report",
            dashboard_id=sample_dashboard.id,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_start=None,
            period_end=date(2025, 3, 31),
        )
        response = create_snapshot(request, payload)
        assert response.title == "Ongoing Report"
        assert response.period_start is None
        assert response.period_end == date(2025, 3, 31)
        # Cleanup
        ReportSnapshot.objects.filter(id=response.id).delete()

    def test_create_invalid_date_range(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test creating snapshot with end before start"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Bad Dates",
            dashboard_id=sample_dashboard.id,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_start=date(2025, 3, 31),
            period_end=date(2025, 1, 1),
        )
        with pytest.raises(HttpError) as exc_info:
            create_snapshot(request, payload)
        assert exc_info.value.status_code == 400

    def test_create_dashboard_not_found(self, orguser, seed_db):
        """Test creating snapshot with nonexistent dashboard"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Ghost Dashboard",
            dashboard_id=99999,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_start=date(2025, 1, 1),
            period_end=date(2025, 1, 31),
        )
        with pytest.raises(HttpError) as exc_info:
            create_snapshot(request, payload)
        assert exc_info.value.status_code == 400

    def test_create_invalid_date_column(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test creating snapshot with a date column that doesn't match any datetime filter"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Bad Column",
            dashboard_id=sample_dashboard.id,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="nonexistent_col"
            ),
            period_start=date(2025, 1, 1),
            period_end=date(2025, 1, 31),
        )
        with pytest.raises(HttpError) as exc_info:
            create_snapshot(request, payload)
        assert exc_info.value.status_code == 400

    def test_freezes_chart_configs(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test that chart configs and filters are properly frozen"""
        request = mock_request(orguser)
        payload = SnapshotCreate(
            title="Freeze Test",
            dashboard_id=sample_dashboard.id,
            date_column=DateColumnSchema(
                schema_name="public", table_name="orders", column_name="created_at"
            ),
            period_start=date(2025, 1, 1),
            period_end=date(2025, 1, 31),
        )
        response = create_snapshot(request, payload)

        snapshot = ReportSnapshot.objects.get(id=response.id)
        # Check frozen_dashboard (layout + filters merged)
        assert snapshot.frozen_dashboard["title"] == "Test Dashboard"
        assert snapshot.frozen_dashboard["grid_columns"] == 12
        assert "chart-123" in snapshot.frozen_dashboard["components"]

        # Check frozen filters inside frozen_dashboard
        assert len(snapshot.frozen_dashboard["filters"]) == 1
        assert snapshot.frozen_dashboard["filters"][0]["filter_type"] == "datetime"
        assert snapshot.frozen_dashboard["filters"][0]["column_name"] == "created_at"

        # Check frozen chart configs
        assert str(sample_chart.id) in snapshot.frozen_chart_configs
        frozen_chart = snapshot.frozen_chart_configs[str(sample_chart.id)]
        assert frozen_chart["chart_type"] == "bar"
        assert frozen_chart["schema_name"] == "public"
        assert frozen_chart["table_name"] == "orders"

        # Check date_column stored
        assert snapshot.date_column["column_name"] == "created_at"

        # Cleanup
        snapshot.delete()


# ================================================================================
# Test get_snapshot_view endpoint
# ================================================================================


class TestGetSnapshotView:
    """Tests for get_snapshot_view endpoint"""

    def test_view_success(self, orguser, sample_snapshot, seed_db):
        """Test successfully viewing a snapshot"""
        request = mock_request(orguser)
        response = get_snapshot_view(request, sample_snapshot.id)

        assert response.report_metadata["title"] == "January 2025 Report"
        assert response.report_metadata["period_start"] == "2025-01-01"
        assert response.report_metadata["period_end"] == "2025-01-31"
        assert response.report_metadata["date_column"]["column_name"] == "created_at"
        assert response.dashboard_data["title"] == "Test Dashboard"
        assert response.dashboard_data["dashboard_type"] == "native"

    def test_view_marks_as_viewed(self, orguser, sample_snapshot, seed_db):
        """Test that viewing a snapshot marks it as viewed"""
        assert sample_snapshot.status == SnapshotStatus.GENERATED.value
        request = mock_request(orguser)
        get_snapshot_view(request, sample_snapshot.id)

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.status == SnapshotStatus.VIEWED.value

    def test_view_not_found(self, orguser, seed_db):
        """Test viewing a nonexistent snapshot"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_snapshot_view(request, 99999)
        assert exc_info.value.status_code == 404


# ================================================================================
# Test update_snapshot_summary endpoint
# ================================================================================


class TestUpdateSnapshot:
    """Tests for update_snapshot endpoint"""

    def test_update_summary(self, orguser, sample_snapshot, seed_db):
        """Test updating snapshot summary"""
        request = mock_request(orguser)
        payload = SnapshotUpdate(summary="Key findings: revenue up 15%")
        response = update_snapshot(request, sample_snapshot.id, payload)

        assert response["success"] is True
        assert response["summary"] == "Key findings: revenue up 15%"

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.summary == "Key findings: revenue up 15%"

    def test_update_not_found(self, orguser, seed_db):
        """Test updating a nonexistent snapshot"""
        request = mock_request(orguser)
        payload = SnapshotUpdate(summary="test")
        with pytest.raises(HttpError) as exc_info:
            update_snapshot(request, 99999, payload)
        assert exc_info.value.status_code == 404


# ================================================================================
# Test delete_snapshot endpoint
# ================================================================================


class TestDeleteSnapshot:
    """Tests for delete_snapshot endpoint"""

    def test_delete_success(self, orguser, sample_snapshot, seed_db):
        """Test deleting a snapshot"""
        snapshot_id = sample_snapshot.id
        request = mock_request(orguser)
        response = delete_snapshot(request, snapshot_id)

        assert response["success"] is True
        assert not ReportSnapshot.objects.filter(id=snapshot_id).exists()

    def test_delete_not_found(self, orguser, seed_db):
        """Test deleting a nonexistent snapshot"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_snapshot(request, 99999)
        assert exc_info.value.status_code == 404
