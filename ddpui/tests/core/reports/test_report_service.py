"""Service-layer tests for ReportService

Tests:
1. _inject_period_into_dashboard_config — matching filter, no match (display-only), no date_col, null filters
2. _inject_period_into_chart_configs — inject, no date_col, non-matching chart, no period dates
3. _freeze_dashboard — captures all expected keys
4. _freeze_chart_configs — captures chart data, skips non-chart components, handles empty components
5. update_snapshot — disallowed field, allowed field
6. delete_snapshot — non-creator forbidden, success
7. get_snapshot — not found
8. get_snapshot_view_data — marks as viewed, already viewed stays viewed, warehouse-discovered column
"""

import os
import copy
import django
from datetime import date
from unittest.mock import patch, MagicMock, PropertyMock
import pytest

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
from ddpui.core.reports.report_service import ReportService
from ddpui.core.reports.exceptions import (
    SnapshotNotFoundError,
    SnapshotValidationError,
    SnapshotPermissionError,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="svcreportuser", email="svcreportuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Service Test Org", slug="svc-test-org", airbyte_workspace_id="workspace-id"
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
def other_authuser():
    user = User.objects.create(
        username="svcotherrptuser", email="svcotherrptuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def other_orguser(other_authuser, org):
    orguser = OrgUser.objects.create(
        user=other_authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_dashboard(orguser, org):
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test Description",
        dashboard_type="native",
        grid_columns=12,
        target_screen_size="desktop",
        layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": 1, "chartType": "bar", "title": "Bar Chart"},
            },
            "text-1": {
                "id": "text-1",
                "type": "text",
                "config": {"content": "Hello"},
            },
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
        settings={"default_start_date": "2024-01-01"},
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
    snapshot = ReportService.create_snapshot(
        title="Jan 2025 Report",
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
# Test _inject_period_into_dashboard_config
# ================================================================================


class TestInjectPeriodIntoFilters:
    """Tests for ReportService._inject_period_into_dashboard_config"""

    def test_matching_filter_is_enriched(self, sample_snapshot):
        """When a datetime filter matches the snapshot's date_column, inject dates + locked"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)

        assert result is True
        dt_filter = None
        for f in frozen["filters"]:
            if f.get("filter_type") == "datetime" and f.get("column_name") == "created_at":
                dt_filter = f
                break
        assert dt_filter is not None
        assert dt_filter["settings"]["locked"] is True
        assert dt_filter["settings"]["default_start_date"] == "2025-01-01"
        assert dt_filter["settings"]["default_end_date"] == "2025-01-31"

    def test_no_matching_filter_injects_display_filter(self, sample_snapshot):
        """When no matching filter exists, a display-only filter is injected"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        # Change snapshot date_column to something that doesn't match
        sample_snapshot.date_column = {
            "schema_name": "analytics",
            "table_name": "events",
            "column_name": "event_time",
        }

        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)

        assert result is False
        # A display-only filter should have been inserted at position 0
        display_filter = frozen["filters"][0]
        assert display_filter["filter_type"] == "datetime"
        assert display_filter["column_name"] == "event_time"
        assert display_filter["id"] < 0  # Negative ID for display-only
        assert display_filter["settings"]["locked"] is True

    def test_no_date_col_returns_true(self, sample_snapshot):
        """When date_column is empty, nothing is injected and True is returned"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        sample_snapshot.date_column = {}

        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)
        assert result is True

    def test_null_filters_creates_list(self, sample_snapshot):
        """When frozen_dashboard has no filters key, a list is created"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        frozen.pop("filters", None)
        # Use a non-matching column to trigger display-only injection
        sample_snapshot.date_column = {
            "schema_name": "analytics",
            "table_name": "events",
            "column_name": "event_time",
        }

        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)

        assert result is False
        assert "filters" in frozen
        assert len(frozen["filters"]) == 1

    def test_no_period_start_sets_none(self, sample_snapshot):
        """When snapshot has no period_start, default_start_date is None"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        sample_snapshot.period_start = None

        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)

        assert result is True
        dt_filter = None
        for f in frozen["filters"]:
            if f.get("filter_type") == "datetime" and f.get("column_name") == "created_at":
                dt_filter = f
                break
        assert dt_filter is not None
        assert dt_filter["settings"]["default_start_date"] is None
        assert dt_filter["settings"]["default_end_date"] == "2025-01-31"


# ================================================================================
# Test _inject_period_into_chart_configs
# ================================================================================


class TestInjectPeriodIntoChartConfigs:
    """Tests for ReportService._inject_period_into_chart_configs"""

    def test_inject_filters_into_matching_chart(self, sample_snapshot):
        """Injects date filters into charts matching the date column's schema/table"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        # The chart has schema_name=public, table_name=orders
        # snapshot date_column matches
        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        filters = chart["extra_config"]["filters"]
        assert len(filters) == 2
        assert filters[0]["column"] == "created_at"
        assert filters[0]["operator"] == "greater_than_equal"
        assert filters[0]["value"] == "2025-01-01"
        assert filters[1]["operator"] == "less_than_equal"
        assert "2025-01-31" in filters[1]["value"]

    def test_no_date_col_does_nothing(self, sample_snapshot):
        """When date_column is empty, no filters are injected"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        sample_snapshot.date_column = {}

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        extra = chart.get("extra_config", {})
        assert "filters" not in extra or len(extra.get("filters", [])) == 0

    def test_non_matching_chart_not_modified(self, sample_snapshot):
        """Charts with different schema/table are not modified"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        # Change the chart's schema so it doesn't match
        frozen_charts["1"]["schema_name"] = "other_schema"

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        extra = chart.get("extra_config", {})
        assert "filters" not in extra or len(extra.get("filters", [])) == 0

    def test_no_period_start_only_end_filter(self, sample_snapshot):
        """When period_start is None, only the end-date filter is injected"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        sample_snapshot.period_start = None

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        filters = chart["extra_config"]["filters"]
        assert len(filters) == 1
        assert filters[0]["operator"] == "less_than_equal"

    def test_empty_frozen_configs_does_nothing(self, sample_snapshot):
        """When frozen_chart_configs is empty, no error is raised"""
        frozen_charts = {}
        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)
        assert frozen_charts == {}


# ================================================================================
# Test _freeze_dashboard
# ================================================================================


class TestFreezeDashboard:
    """Tests for ReportService._freeze_dashboard"""

    def test_captures_all_expected_keys(self, sample_dashboard, sample_filter):
        """Freeze captures title, description, grid_columns, etc."""
        frozen = ReportService._freeze_dashboard(sample_dashboard)

        assert frozen["title"] == "Test Dashboard"
        assert frozen["description"] == "Test Description"
        assert frozen["grid_columns"] == 12
        assert frozen["target_screen_size"] == "desktop"
        assert frozen["layout_config"] is not None
        assert "chart-1" in frozen["components"]
        assert len(frozen["filters"]) == 1
        assert frozen["filters"][0]["filter_type"] == "datetime"

    def test_empty_dashboard_no_filters(self, orguser, org):
        """Dashboard with no filters returns empty filters list"""
        dashboard = Dashboard.objects.create(
            title="Empty",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=org,
        )
        frozen = ReportService._freeze_dashboard(dashboard)
        assert frozen["filters"] == []
        dashboard.delete()


# ================================================================================
# Test _freeze_chart_configs
# ================================================================================


class TestFreezeChartConfigs:
    """Tests for ReportService._freeze_chart_configs"""

    def test_captures_chart_data(self, sample_dashboard, sample_chart):
        """Charts referenced in components are frozen with all fields"""
        frozen = ReportService._freeze_chart_configs(sample_dashboard)

        assert str(sample_chart.id) in frozen
        chart_data = frozen[str(sample_chart.id)]
        assert chart_data["id"] == sample_chart.id
        assert chart_data["title"] == "Bar Chart"
        assert chart_data["chart_type"] == "bar"
        assert chart_data["schema_name"] == "public"
        assert chart_data["table_name"] == "orders"
        assert "extra_config" in chart_data

    def test_skips_non_chart_components(self, sample_dashboard, sample_chart):
        """Non-chart components (text, etc.) are not included in frozen configs"""
        frozen = ReportService._freeze_chart_configs(sample_dashboard)
        # Only the chart component should be frozen, not the text component
        assert len(frozen) == 1

    def test_empty_components(self, orguser, org):
        """Dashboard with no components returns empty dict"""
        dashboard = Dashboard.objects.create(
            title="Empty",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=org,
        )
        frozen = ReportService._freeze_chart_configs(dashboard)
        assert frozen == {}
        dashboard.delete()

    def test_component_with_missing_chart_id(self, orguser, org):
        """Chart component without chartId in config is skipped"""
        dashboard = Dashboard.objects.create(
            title="Missing ID",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={
                "chart-1": {
                    "id": "chart-1",
                    "type": "chart",
                    "config": {},  # No chartId
                }
            },
            created_by=orguser,
            org=org,
        )
        frozen = ReportService._freeze_chart_configs(dashboard)
        assert frozen == {}
        dashboard.delete()


# ================================================================================
# Test update_snapshot
# ================================================================================


class TestUpdateSnapshot:
    """Tests for ReportService.update_snapshot"""

    def test_update_allowed_field(self, sample_snapshot, org):
        """Updating 'summary' succeeds"""
        updated = ReportService.update_snapshot(
            sample_snapshot.id, org, summary="New summary"
        )
        assert updated.summary == "New summary"

    def test_update_disallowed_field_raises(self, sample_snapshot, org):
        """Updating a non-editable field raises SnapshotValidationError"""
        with pytest.raises(SnapshotValidationError) as exc_info:
            ReportService.update_snapshot(sample_snapshot.id, org, title="New title")
        assert "not editable" in str(exc_info.value)

    def test_update_not_found_raises(self, org):
        """Updating nonexistent snapshot raises SnapshotNotFoundError"""
        with pytest.raises(SnapshotNotFoundError):
            ReportService.update_snapshot(99999, org, summary="test")

    def test_update_with_none_value_skips(self, sample_snapshot, org):
        """Updating with None value does not change the field"""
        sample_snapshot.summary = "Original"
        sample_snapshot.save(update_fields=["summary"])

        updated = ReportService.update_snapshot(sample_snapshot.id, org, summary=None)
        assert updated.summary == "Original"


# ================================================================================
# Test delete_snapshot
# ================================================================================


class TestDeleteSnapshot:
    """Tests for ReportService.delete_snapshot"""

    def test_delete_success(self, sample_snapshot, org, orguser):
        """Creator can delete their own snapshot"""
        snapshot_id = sample_snapshot.id
        result = ReportService.delete_snapshot(snapshot_id, org, orguser)
        assert result is True
        assert not ReportSnapshot.objects.filter(id=snapshot_id).exists()

    def test_delete_non_creator_forbidden(self, sample_snapshot, org, other_orguser):
        """Non-creator cannot delete a snapshot"""
        with pytest.raises(SnapshotPermissionError):
            ReportService.delete_snapshot(sample_snapshot.id, org, other_orguser)

    def test_delete_not_found(self, org, orguser):
        """Deleting nonexistent snapshot raises SnapshotNotFoundError"""
        with pytest.raises(SnapshotNotFoundError):
            ReportService.delete_snapshot(99999, org, orguser)


# ================================================================================
# Test get_snapshot
# ================================================================================


class TestGetSnapshot:
    """Tests for ReportService.get_snapshot"""

    def test_get_success(self, sample_snapshot, org):
        """Successfully retrieve an existing snapshot"""
        snapshot = ReportService.get_snapshot(sample_snapshot.id, org)
        assert snapshot.id == sample_snapshot.id
        assert snapshot.title == "Jan 2025 Report"

    def test_get_not_found(self, org):
        """Retrieving nonexistent snapshot raises SnapshotNotFoundError"""
        with pytest.raises(SnapshotNotFoundError):
            ReportService.get_snapshot(99999, org)


# ================================================================================
# Test get_snapshot_view_data
# ================================================================================


class TestGetSnapshotViewData:
    """Tests for ReportService.get_snapshot_view_data"""

    def test_marks_generated_as_viewed(self, sample_snapshot, org):
        """A generated snapshot is marked as viewed after get_snapshot_view_data"""
        assert sample_snapshot.status == SnapshotStatus.GENERATED.value

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.status == SnapshotStatus.VIEWED.value
        assert "dashboard_data" in view_data
        assert "report_metadata" in view_data
        assert "frozen_chart_configs" in view_data

    def test_already_viewed_stays_viewed(self, sample_snapshot, org):
        """A viewed snapshot stays viewed"""
        sample_snapshot.status = SnapshotStatus.VIEWED.value
        sample_snapshot.save(update_fields=["status"])

        ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        sample_snapshot.refresh_from_db()
        assert sample_snapshot.status == SnapshotStatus.VIEWED.value

    def test_view_data_structure(self, sample_snapshot, org):
        """View data has expected keys and values"""
        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        # dashboard_data keys
        dd = view_data["dashboard_data"]
        assert dd["title"] == "Test Dashboard"
        assert dd["dashboard_type"] == "native"
        assert dd["is_published"] is True
        assert "components" in dd
        assert "filters" in dd

        # report_metadata keys
        rm = view_data["report_metadata"]
        assert rm["snapshot_id"] == sample_snapshot.id
        assert rm["title"] == "Jan 2025 Report"
        assert rm["period_start"] == "2025-01-01"
        assert rm["period_end"] == "2025-01-31"
        assert rm["dashboard_title"] == "Test Dashboard"

    def test_warehouse_discovered_column_injects_chart_filters(self, sample_snapshot, org):
        """When date_column doesn't match a dashboard filter, chart-level filters are injected"""
        # Change date_column to something not matching any dashboard filter
        sample_snapshot.date_column = {
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "updated_at",  # Not in dashboard filters
        }
        sample_snapshot.save(update_fields=["date_column"])

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        # The chart's extra_config should have filters injected
        frozen_charts = view_data["frozen_chart_configs"]
        chart = frozen_charts.get("1")
        assert chart is not None
        filters = chart.get("extra_config", {}).get("filters", [])
        assert len(filters) == 2  # start and end date filters
        col_names = [f["column"] for f in filters]
        assert "updated_at" in col_names

    def test_not_found(self, org):
        """Viewing nonexistent snapshot raises SnapshotNotFoundError"""
        with pytest.raises(SnapshotNotFoundError):
            ReportService.get_snapshot_view_data(99999, org)


# ================================================================================
# Test create_snapshot
# ================================================================================


class TestCreateSnapshot:
    """Tests for ReportService.create_snapshot"""

    def test_invalid_date_range(self, orguser):
        """period_start > period_end raises SnapshotValidationError"""
        with pytest.raises(SnapshotValidationError) as exc_info:
            ReportService.create_snapshot(
                title="Bad",
                dashboard_id=1,
                date_column={"schema_name": "s", "table_name": "t", "column_name": "c"},
                period_start=date(2025, 12, 31),
                period_end=date(2025, 1, 1),
                orguser=orguser,
            )
        assert "before" in str(exc_info.value)

    def test_dashboard_not_found(self, orguser):
        """Nonexistent dashboard raises SnapshotValidationError"""
        with pytest.raises(SnapshotValidationError) as exc_info:
            ReportService.create_snapshot(
                title="Ghost",
                dashboard_id=99999,
                date_column={"schema_name": "s", "table_name": "t", "column_name": "c"},
                period_end=date(2025, 1, 31),
                orguser=orguser,
            )
        assert "not found" in str(exc_info.value)

    def test_success_with_matching_filter(
        self, orguser, org, sample_dashboard, sample_filter, sample_chart
    ):
        """Successful creation when date_column matches a dashboard filter"""
        snapshot = ReportService.create_snapshot(
            title="Q1 Report",
            dashboard_id=sample_dashboard.id,
            date_column={
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "created_at",
            },
            period_start=date(2025, 1, 1),
            period_end=date(2025, 3, 31),
            orguser=orguser,
        )
        assert snapshot.title == "Q1 Report"
        assert snapshot.period_start == date(2025, 1, 1)
        assert snapshot.period_end == date(2025, 3, 31)
        assert snapshot.frozen_dashboard["title"] == "Test Dashboard"
        assert str(sample_chart.id) in snapshot.frozen_chart_configs
        snapshot.delete()

    def test_success_without_start_date(
        self, orguser, org, sample_dashboard, sample_filter, sample_chart
    ):
        """period_start=None means no lower bound"""
        snapshot = ReportService.create_snapshot(
            title="No Start",
            dashboard_id=sample_dashboard.id,
            date_column={
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "created_at",
            },
            period_end=date(2025, 3, 31),
            orguser=orguser,
        )
        assert snapshot.period_start is None
        assert snapshot.period_end == date(2025, 3, 31)
        snapshot.delete()


# ================================================================================
# Test list_snapshots
# ================================================================================


class TestListSnapshots:
    """Tests for ReportService.list_snapshots"""

    def test_list_empty(self, org):
        """No snapshots returns empty list"""
        result = ReportService.list_snapshots(org)
        assert result == []

    def test_list_with_data(self, org, sample_snapshot):
        """Returns existing snapshots"""
        result = ReportService.list_snapshots(org)
        assert len(result) == 1
        assert result[0].id == sample_snapshot.id

    def test_list_with_search(self, org, sample_snapshot):
        """Search filter works on title"""
        result = ReportService.list_snapshots(org, search="Jan")
        assert len(result) == 1

        result = ReportService.list_snapshots(org, search="nonexistent")
        assert len(result) == 0

    def test_list_filter_by_dashboard_title(self, org, sample_snapshot):
        """Filter by dashboard_title matches frozen_dashboard.title"""
        result = ReportService.list_snapshots(org, dashboard_title="Test Dashboard")
        assert len(result) == 1

        result = ReportService.list_snapshots(org, dashboard_title="test dash")
        assert len(result) == 1  # icontains

        result = ReportService.list_snapshots(org, dashboard_title="nonexistent")
        assert len(result) == 0

    def test_list_filter_by_created_by_email(self, org, sample_snapshot):
        """Filter by created_by_email matches creator's email"""
        result = ReportService.list_snapshots(org, created_by_email="svcreportuser@test.com")
        assert len(result) == 1

        result = ReportService.list_snapshots(org, created_by_email="svcreport")
        assert len(result) == 1  # icontains

        result = ReportService.list_snapshots(org, created_by_email="nobody@test.com")
        assert len(result) == 0

    def test_list_combined_filters(self, org, sample_snapshot):
        """Multiple filters are combined with AND"""
        result = ReportService.list_snapshots(
            org, search="Jan", dashboard_title="Test", created_by_email="svcreport"
        )
        assert len(result) == 1

        # One filter mismatches -> no results
        result = ReportService.list_snapshots(
            org, search="Jan", dashboard_title="wrong"
        )
        assert len(result) == 0


# ================================================================================
# Test snapshot isolation — deleting charts/dashboards doesn't affect snapshots
# ================================================================================


class TestSnapshotIsolation:
    """Snapshots freeze config at creation time. Deleting the original
    dashboard or its charts must not affect snapshot view data."""

    def test_deleting_chart_does_not_affect_snapshot(
        self, sample_snapshot, org, sample_chart
    ):
        """After deleting the original chart, get_snapshot_view_data still returns
        the full frozen chart config."""
        snapshot_id = sample_snapshot.id
        chart_id = sample_chart.id  # Save before delete sets it to None

        # Verify the chart is in frozen configs before deletion
        assert str(chart_id) in sample_snapshot.frozen_chart_configs

        # Delete the original chart
        sample_chart.delete()
        assert not Chart.objects.filter(id=chart_id).exists()

        # Snapshot view data should still work — it reads from frozen JSON, not the Chart table
        view_data = ReportService.get_snapshot_view_data(snapshot_id, org)

        frozen_charts = view_data["frozen_chart_configs"]
        assert str(chart_id) in frozen_charts
        assert frozen_charts[str(chart_id)]["title"] == "Bar Chart"
        assert frozen_charts[str(chart_id)]["chart_type"] == "bar"
        assert frozen_charts[str(chart_id)]["schema_name"] == "public"
        assert frozen_charts[str(chart_id)]["table_name"] == "orders"

    def test_deleting_dashboard_does_not_affect_snapshot(
        self, sample_snapshot, org, sample_dashboard
    ):
        """After deleting the original dashboard, get_snapshot_view_data still
        returns the full frozen dashboard config."""
        snapshot_id = sample_snapshot.id
        original_title = sample_dashboard.title

        # Delete the original dashboard (cascades to filters)
        sample_dashboard.delete()
        assert not Dashboard.objects.filter(id=sample_dashboard.id).exists()

        # Snapshot view data should still work
        view_data = ReportService.get_snapshot_view_data(snapshot_id, org)

        dd = view_data["dashboard_data"]
        assert dd["title"] == original_title
        assert dd["dashboard_type"] == "native"
        assert "components" in dd
        assert "filters" in dd

        rm = view_data["report_metadata"]
        assert rm["dashboard_title"] == original_title

    def test_different_datetime_columns_produce_two_filters(
        self, sample_snapshot, org
    ):
        """When the report's date_column differs from the dashboard's existing
        datetime filter, two datetime filters appear in the view data:
        one is the original dashboard filter (untouched) and one is the
        injected display-only filter (locked with the snapshot period)."""
        # Change date_column to a different column than the dashboard filter
        # Dashboard filter: public.orders.created_at
        # Snapshot date_column: public.orders.updated_at (different column)
        sample_snapshot.date_column = {
            "schema_name": "public",
            "table_name": "orders",
            "column_name": "updated_at",
        }
        sample_snapshot.save(update_fields=["date_column"])

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        filters = view_data["dashboard_data"]["filters"]
        datetime_filters = [f for f in filters if f.get("filter_type") == "datetime"]
        assert len(datetime_filters) == 2

        # The display-only filter (injected) should be first and locked
        display_filter = datetime_filters[0]
        assert display_filter["column_name"] == "updated_at"
        assert display_filter["settings"]["locked"] is True
        assert display_filter["settings"]["default_start_date"] == "2025-01-01"
        assert display_filter["settings"]["default_end_date"] == "2025-01-31"
        assert display_filter["id"] < 0  # Negative ID for display-only

        # The original dashboard filter should remain untouched (not locked)
        original_filter = datetime_filters[1]
        assert original_filter["column_name"] == "created_at"
        assert original_filter["settings"].get("locked") is not True
