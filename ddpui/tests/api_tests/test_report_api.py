"""API Tests for Report/Snapshot endpoints

Tests:
1. list_snapshots - empty, with data, search
2. create_snapshot - success, without start date, invalid date range, dashboard not found,
                     invalid date column
3. get_snapshot_view - success, not found, already viewed stays viewed
4. update_snapshot_summary - success, not found
5. delete_snapshot - success, not found, non-creator forbidden
6. toggle_report_sharing - enable, disable, not found, non-creator forbidden
7. get_report_sharing_status - public, private, not found, non-creator forbidden
8. list_dashboard_datetime_columns - success, dashboard not found, no charts, includes filters
"""

import os
import django
from datetime import date
from unittest.mock import patch, MagicMock
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
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
    toggle_report_sharing,
    get_report_sharing_status,
    list_dashboard_datetime_columns,
)
from ddpui.schemas.report_schema import SnapshotCreate, SnapshotUpdate, DateColumnSchema
from ddpui.schemas.dashboard_schema import ShareToggle
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
def other_authuser():
    """A second django User for permission tests"""
    user = User.objects.create(
        username="otherreportuser", email="otherreportuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def other_orguser(other_authuser, org):
    """A second OrgUser in the same org (not the snapshot creator)"""
    orguser = OrgUser.objects.create(
        user=other_authuser,
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


@pytest.fixture
def empty_dashboard(orguser, org):
    """A dashboard with no chart components"""
    dashboard = Dashboard.objects.create(
        title="Empty Dashboard",
        description="No charts",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[],
        components={},
        created_by=orguser,
        org=org,
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
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
        assert response["success"] is True
        assert len(response["data"]) == 0

    def test_list_with_data(self, orguser, sample_snapshot, seed_db):
        """Test listing returns snapshots"""
        request = mock_request(orguser)
        response = list_snapshots(request)
        data = response["data"]
        assert len(data) == 1
        assert data[0].title == "January 2025 Report"
        assert data[0].date_column is not None
        assert data[0].date_column["column_name"] == "created_at"

    def test_list_with_search(self, orguser, sample_snapshot, seed_db):
        """Test listing with search filter"""
        request = mock_request(orguser)

        response = list_snapshots(request, search="January")
        assert len(response["data"]) == 1

        response = list_snapshots(request, search="nonexistent")
        assert len(response["data"]) == 0

    def test_list_filter_by_dashboard_title(self, orguser, sample_snapshot, seed_db):
        """Test listing with dashboard_title filter"""
        request = mock_request(orguser)

        response = list_snapshots(request, dashboard_title="Test Dashboard")
        assert len(response["data"]) == 1

        response = list_snapshots(request, dashboard_title="nonexistent")
        assert len(response["data"]) == 0

    def test_list_filter_by_created_by(self, orguser, sample_snapshot, seed_db):
        """Test listing with created_by filter"""
        request = mock_request(orguser)

        response = list_snapshots(request, created_by="reportapiuser@test.com")
        assert len(response["data"]) == 1

        response = list_snapshots(request, created_by="nobody")
        assert len(response["data"]) == 0


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
        assert response["success"] is True
        data = response["data"]
        assert data["title"] == "Q1 Report"
        assert data["period_start"] == date(2025, 1, 1)
        assert data["period_end"] == date(2025, 3, 31)
        assert data["dashboard_title"] == "Test Dashboard"
        assert data["date_column"]["column_name"] == "created_at"
        # Cleanup
        ReportSnapshot.objects.filter(id=data["id"]).delete()

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
        assert response["success"] is True
        data = response["data"]
        assert data["title"] == "Ongoing Report"
        assert data["period_start"] is None
        assert data["period_end"] == date(2025, 3, 31)
        # Cleanup
        ReportSnapshot.objects.filter(id=data["id"]).delete()

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
        assert response["success"] is True

        snapshot = ReportSnapshot.objects.get(id=response["data"]["id"])
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
        assert response["success"] is True
        data = response["data"]

        assert data["report_metadata"]["title"] == "January 2025 Report"
        assert data["report_metadata"]["period_start"] == "2025-01-01"
        assert data["report_metadata"]["period_end"] == "2025-01-31"
        assert data["report_metadata"]["date_column"]["column_name"] == "created_at"
        assert data["dashboard_data"]["title"] == "Test Dashboard"
        assert data["dashboard_data"]["dashboard_type"] == "native"

    def test_view_marks_as_viewed(self, orguser, sample_snapshot, seed_db):
        """Test that viewing a snapshot marks it as viewed"""
        assert sample_snapshot.status == SnapshotStatus.GENERATED.value
        request = mock_request(orguser)
        get_snapshot_view(request, sample_snapshot.id)

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.status == SnapshotStatus.VIEWED.value

    def test_view_already_viewed_stays_viewed(self, orguser, sample_snapshot, seed_db):
        """Test that viewing an already-viewed snapshot does not change status"""
        sample_snapshot.status = SnapshotStatus.VIEWED.value
        sample_snapshot.save(update_fields=["status"])

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

    def test_view_injects_period_into_filters(self, orguser, sample_snapshot, seed_db):
        """Test that the view response injects period dates into the matching filter"""
        request = mock_request(orguser)
        response = get_snapshot_view(request, sample_snapshot.id)
        data = response["data"]

        # Find the datetime filter in dashboard_data
        filters = data["dashboard_data"].get("filters", [])
        datetime_filter = None
        for f in filters:
            if f.get("filter_type") == "datetime" and f.get("column_name") == "created_at":
                datetime_filter = f
                break

        assert datetime_filter is not None
        settings = datetime_filter.get("settings", {})
        assert settings.get("locked") is True
        assert settings.get("default_start_date") == "2025-01-01"
        assert settings.get("default_end_date") == "2025-01-31"


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
        assert response["data"]["summary"] == "Key findings: revenue up 15%"

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

    def test_delete_by_non_creator_forbidden(
        self, other_orguser, sample_snapshot, seed_db
    ):
        """Test that a user who did not create the snapshot cannot delete it"""
        request = mock_request(other_orguser)
        with pytest.raises(HttpError) as exc_info:
            delete_snapshot(request, sample_snapshot.id)
        assert exc_info.value.status_code == 403


# ================================================================================
# Test toggle_report_sharing endpoint
# ================================================================================


class TestToggleReportSharing:
    """Tests for toggle_report_sharing endpoint"""

    def test_enable_sharing(self, orguser, sample_snapshot, seed_db):
        """Test enabling public sharing generates token and URL"""
        request = mock_request(orguser)
        payload = ShareToggle(is_public=True)
        response = toggle_report_sharing(request, sample_snapshot.id, payload)

        data = response["data"]
        assert data["is_public"] is True
        assert data["public_url"] is not None
        assert "/share/report/" in data["public_url"]
        assert data["public_share_token"] is not None
        assert data["message"] == "Report made public"

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.is_public is True
        assert sample_snapshot.public_share_token is not None
        assert sample_snapshot.public_shared_at is not None
        assert sample_snapshot.public_disabled_at is None

    def test_disable_sharing(self, orguser, sample_snapshot, seed_db):
        """Test disabling public sharing"""
        # First enable
        request = mock_request(orguser)
        toggle_report_sharing(request, sample_snapshot.id, ShareToggle(is_public=True))

        # Then disable
        response = toggle_report_sharing(
            request, sample_snapshot.id, ShareToggle(is_public=False)
        )

        data = response["data"]
        assert data["is_public"] is False
        assert data["message"] == "Report made private"

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.is_public is False
        assert sample_snapshot.public_disabled_at is not None

    def test_enable_sharing_preserves_existing_token(self, orguser, sample_snapshot, seed_db):
        """Test re-enabling sharing reuses the existing token"""
        request = mock_request(orguser)
        # Enable
        resp1 = toggle_report_sharing(request, sample_snapshot.id, ShareToggle(is_public=True))
        token1 = resp1["data"]["public_share_token"]

        # Disable
        toggle_report_sharing(request, sample_snapshot.id, ShareToggle(is_public=False))

        # Re-enable
        resp2 = toggle_report_sharing(request, sample_snapshot.id, ShareToggle(is_public=True))
        token2 = resp2["data"]["public_share_token"]

        assert token1 == token2

    def test_sharing_not_found(self, orguser, seed_db):
        """Test toggling sharing on nonexistent snapshot"""
        request = mock_request(orguser)
        payload = ShareToggle(is_public=True)
        with pytest.raises(HttpError) as exc_info:
            toggle_report_sharing(request, 99999, payload)
        assert exc_info.value.status_code == 404

    def test_sharing_non_creator_forbidden(self, other_orguser, sample_snapshot, seed_db):
        """Test that non-creator cannot toggle sharing"""
        request = mock_request(other_orguser)
        payload = ShareToggle(is_public=True)
        with pytest.raises(HttpError) as exc_info:
            toggle_report_sharing(request, sample_snapshot.id, payload)
        assert exc_info.value.status_code == 403


# ================================================================================
# Test get_report_sharing_status endpoint
# ================================================================================


class TestGetReportSharingStatus:
    """Tests for get_report_sharing_status endpoint"""

    def test_status_private_report(self, orguser, sample_snapshot, seed_db):
        """Test sharing status for a private report"""
        request = mock_request(orguser)
        response = get_report_sharing_status(request, sample_snapshot.id)

        data = response["data"]
        assert data["is_public"] is False
        assert data["public_access_count"] == 0

    def test_status_public_report(self, orguser, sample_snapshot, seed_db):
        """Test sharing status for a public report includes URL"""
        request = mock_request(orguser)
        # Enable sharing first
        toggle_report_sharing(request, sample_snapshot.id, ShareToggle(is_public=True))

        response = get_report_sharing_status(request, sample_snapshot.id)

        data = response["data"]
        assert data["is_public"] is True
        assert data["public_url"] is not None
        assert "/share/report/" in data["public_url"]
        assert data["public_shared_at"] is not None

    def test_status_not_found(self, orguser, seed_db):
        """Test sharing status for nonexistent snapshot"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            get_report_sharing_status(request, 99999)
        assert exc_info.value.status_code == 404

    def test_status_non_creator_forbidden(self, other_orguser, sample_snapshot, seed_db):
        """Test that non-creator cannot view sharing status"""
        request = mock_request(other_orguser)
        with pytest.raises(HttpError) as exc_info:
            get_report_sharing_status(request, sample_snapshot.id)
        assert exc_info.value.status_code == 403


# ================================================================================
# Test list_dashboard_datetime_columns endpoint
# ================================================================================


class TestListDashboardDatetimeColumns:
    """Tests for list_dashboard_datetime_columns endpoint"""

    def test_dashboard_not_found(self, orguser, seed_db):
        """Test with nonexistent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc_info:
            list_dashboard_datetime_columns(request, 99999)
        assert exc_info.value.status_code == 404

    def test_no_charts_returns_existing_filters(self, orguser, empty_dashboard, seed_db):
        """Test dashboard with no charts returns empty list (no filters either)"""
        request = mock_request(orguser)
        response = list_dashboard_datetime_columns(request, empty_dashboard.id)
        assert response["success"] is True
        assert len(response["data"]) == 0

    def test_includes_existing_datetime_filters(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test that existing dashboard datetime filters are included in results"""
        # Mock warehouse so it returns no columns (only dashboard filters matter)
        mock_warehouse = MagicMock()
        mock_org_warehouse = MagicMock()

        with patch(
            "ddpui.core.reports.report_service.OrgWarehouse.objects"
        ) as mock_ow_objects, patch(
            "ddpui.core.charts.charts_service.get_warehouse_client"
        ) as mock_get_wc, patch(
            "ddpui.api.filter_api.get_table_columns"
        ) as mock_get_cols:
            mock_ow_objects.filter.return_value.first.return_value = mock_org_warehouse
            mock_get_wc.return_value = mock_warehouse
            mock_get_cols.return_value = []  # No warehouse columns

            request = mock_request(orguser)
            response = list_dashboard_datetime_columns(request, sample_dashboard.id)

        data = response["data"]
        # The existing datetime filter should be included and flagged
        assert len(data) >= 1
        column_names = [r.column_name for r in data]
        assert "created_at" in column_names
        created_at_col = next(r for r in data if r.column_name == "created_at")
        assert created_at_col.is_dashboard_filter is True

    def test_discovers_warehouse_datetime_columns(
        self, orguser, sample_dashboard, sample_chart, sample_filter, seed_db
    ):
        """Test that warehouse datetime columns are discovered"""
        mock_warehouse = MagicMock()
        mock_org_warehouse = MagicMock()

        with patch(
            "ddpui.core.reports.report_service.OrgWarehouse.objects"
        ) as mock_ow_objects, patch(
            "ddpui.core.charts.charts_service.get_warehouse_client"
        ) as mock_get_wc, patch(
            "ddpui.api.filter_api.get_table_columns"
        ) as mock_get_cols, patch(
            "ddpui.api.filter_api.determine_filter_type_from_column"
        ) as mock_determine:
            mock_ow_objects.filter.return_value.first.return_value = mock_org_warehouse
            mock_get_wc.return_value = mock_warehouse
            mock_get_cols.return_value = [
                {"column_name": "created_at", "data_type": "timestamp"},
                {"column_name": "updated_at", "data_type": "timestamp"},
                {"column_name": "name", "data_type": "varchar"},
            ]
            mock_determine.side_effect = lambda dt: (
                "datetime" if "timestamp" in dt.lower() else "value"
            )

            request = mock_request(orguser)
            response = list_dashboard_datetime_columns(request, sample_dashboard.id)

        data = response["data"]
        # Should find the timestamp columns + existing filter (deduplicated)
        column_names = [r.column_name for r in data]
        assert "created_at" in column_names
        assert "updated_at" in column_names
        assert "name" not in column_names

        # created_at matches a dashboard filter, updated_at does not
        created_at_col = next(r for r in data if r.column_name == "created_at")
        assert created_at_col.is_dashboard_filter is True
        updated_at_col = next(r for r in data if r.column_name == "updated_at")
        assert updated_at_col.is_dashboard_filter is False

    def test_no_warehouse_configured(self, orguser, sample_dashboard, sample_chart, seed_db):
        """Test error when warehouse is not configured"""
        with patch(
            "ddpui.core.reports.report_service.OrgWarehouse.objects"
        ) as mock_ow_objects:
            mock_ow_objects.filter.return_value.first.return_value = None

            request = mock_request(orguser)
            with pytest.raises(HttpError) as exc_info:
                list_dashboard_datetime_columns(request, sample_dashboard.id)
            assert exc_info.value.status_code == 502
