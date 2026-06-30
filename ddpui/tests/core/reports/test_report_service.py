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
from ddpui.models.report import ReportSnapshot
from ddpui.auth import ACCOUNT_MANAGER_ROLE, ANALYST_ROLE
from ddpui.core.reports.report_service import ReportService
from ddpui.schemas.report_schema import SnapshotUpdate
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
        new_role=Role.objects.filter(slug=ANALYST_ROLE).first(),
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
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
                "components": {
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

    def test_matching_filter_moved_to_front(self, sample_snapshot):
        """When a matching datetime filter is not first, it should be moved to position 0"""
        frozen = copy.deepcopy(sample_snapshot.frozen_dashboard)
        # Insert a value filter before the datetime filter so it's not at index 0
        frozen["filters"].insert(
            0,
            {
                "id": 999,
                "filter_type": "value",
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "status",
                "settings": {},
                "order": 0,
            },
        )
        # The datetime filter is now at index 1
        assert frozen["filters"][1]["column_name"] == "created_at"

        result = ReportService._inject_period_into_dashboard_config(frozen, sample_snapshot)

        assert result is True
        # The locked datetime filter should now be first
        assert frozen["filters"][0]["column_name"] == "created_at"
        assert frozen["filters"][0]["settings"]["locked"] is True
        # The value filter should be second
        assert frozen["filters"][1]["column_name"] == "status"

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


@patch("ddpui.core.reports.report_service.WarehouseFactory")
@patch("ddpui.core.reports.report_service.OrgWarehouse")
class TestInjectPeriodIntoChartConfigs:
    """Tests for ReportService._inject_period_into_chart_configs"""

    def test_inject_filters_into_matching_chart(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
        """Injects date filters into charts matching the date column's schema/table"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        filters = chart["extra_config"]["filters"]
        assert len(filters) == 2
        assert filters[0]["column"] == "created_at"
        assert filters[0]["operator"] == "greater_than_equal"
        assert filters[0]["value"] == "2025-01-01"
        assert filters[1]["operator"] == "less_than_equal"
        assert "2025-01-31" in filters[1]["value"]

    def test_no_date_col_does_nothing(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
        """When date_column is empty, no filters are injected"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        sample_snapshot.date_column = {}

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        extra = chart.get("extra_config", {})
        assert "filters" not in extra or len(extra.get("filters", [])) == 0

    def test_non_matching_chart_not_modified(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
        """Charts whose table lacks the column are not modified"""
        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        frozen_charts["1"]["schema_name"] = "other_schema"

        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_wh_client = MagicMock()
        mock_wh_client.column_exists.return_value = False
        mock_factory.get_warehouse_client.return_value = mock_wh_client

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        extra = chart.get("extra_config", {})
        assert "filters" not in extra or len(extra.get("filters", [])) == 0

    def test_no_period_start_only_end_filter(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
        """When period_start is None, only the end-date filter is injected"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        frozen_charts = copy.deepcopy(sample_snapshot.frozen_chart_configs)
        sample_snapshot.period_start = None

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        chart = frozen_charts["1"]
        filters = chart["extra_config"]["filters"]
        assert len(filters) == 1
        assert filters[0]["operator"] == "less_than_equal"

    def test_empty_frozen_configs_does_nothing(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
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
        assert len(frozen["tabs"]) == 1
        assert "chart-1" in frozen["tabs"][0]["components"]
        assert len(frozen["filters"]) == 1
        assert frozen["filters"][0]["filter_type"] == "datetime"

    def test_captures_dashboard_id(self, sample_dashboard, sample_filter):
        """Freeze includes the source dashboard_id for linking back"""
        frozen = ReportService._freeze_dashboard(sample_dashboard)

        assert frozen["dashboard_id"] == sample_dashboard.id

    def test_empty_dashboard_no_filters(self, orguser, org):
        """Dashboard with no filters returns empty filters list"""
        dashboard = Dashboard.objects.create(
            title="Empty",
            dashboard_type="native",
            grid_columns=12,
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
            tabs=[
                {
                    "id": "tab-1",
                    "title": "Tab 1",
                    "layout_config": [],
                    "components": {"chart-1": {"id": "chart-1", "type": "chart", "config": {}}},
                }
            ],
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

    def test_update_summary(self, sample_snapshot, org, orguser):
        """Updating 'summary' succeeds"""
        data = SnapshotUpdate(summary="New summary")
        updated = ReportService.update_snapshot(sample_snapshot.id, data, orguser)
        assert updated.summary == "New summary"

    def test_update_not_found_raises(self, org, orguser):
        """Updating nonexistent snapshot raises SnapshotNotFoundError"""
        data = SnapshotUpdate(summary="test")
        with pytest.raises(SnapshotNotFoundError):
            ReportService.update_snapshot(99999, data, orguser)

    def test_update_with_none_value_skips(self, sample_snapshot, org, orguser):
        """Updating with None value does not change the field"""
        sample_snapshot.summary = "Original"
        sample_snapshot.save(update_fields=["summary"])

        data = SnapshotUpdate(summary=None)
        updated = ReportService.update_snapshot(sample_snapshot.id, data, orguser)
        assert updated.summary == "Original"

    def test_update_sets_last_modified_by(self, sample_snapshot, org, orguser):
        """Updating summary sets last_modified_by to the editing user"""
        assert sample_snapshot.last_modified_by is None

        data = SnapshotUpdate(summary="Edited summary")
        updated = ReportService.update_snapshot(sample_snapshot.id, data, orguser)

        assert updated.last_modified_by == orguser
        assert updated.last_modified_by.user.email == orguser.user.email

    def test_update_tracks_different_modifier(self, sample_snapshot, org, other_orguser):
        """last_modified_by reflects the user who made the latest edit"""
        data = SnapshotUpdate(summary="Edited by other user")
        updated = ReportService.update_snapshot(sample_snapshot.id, data, other_orguser)

        assert updated.last_modified_by == other_orguser
        assert updated.last_modified_by.user.email == other_orguser.user.email

    def test_update_none_summary_does_not_set_last_modified_by(self, sample_snapshot, org, orguser):
        """When summary is None (no change), last_modified_by is not updated"""
        assert sample_snapshot.last_modified_by is None

        data = SnapshotUpdate(summary=None)
        updated = ReportService.update_snapshot(sample_snapshot.id, data, orguser)

        assert updated.last_modified_by is None

    def test_creator_writes_then_other_user_edits(
        self, sample_snapshot, org, orguser, other_orguser
    ):
        """Creator writes the initial summary, then another user edits it"""
        # Creator writes the first summary
        data = SnapshotUpdate(summary="Initial summary by creator")
        updated = ReportService.update_snapshot(sample_snapshot.id, data, orguser)
        assert updated.summary == "Initial summary by creator"
        assert updated.last_modified_by == orguser

        # A different user edits the summary
        data = SnapshotUpdate(summary="Revised by another user")
        updated = ReportService.update_snapshot(sample_snapshot.id, data, other_orguser)
        assert updated.summary == "Revised by another user"
        assert updated.last_modified_by == other_orguser
        assert updated.created_by == orguser  # creator unchanged


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


@patch("ddpui.core.reports.report_service.WarehouseFactory")
@patch("ddpui.core.reports.report_service.OrgWarehouse")
class TestGetSnapshotViewData:
    """Tests for ReportService.get_snapshot_view_data"""

    def test_view_data_returns_expected_keys(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """get_snapshot_view_data returns dashboard_data, report_metadata, frozen_chart_configs"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        assert "dashboard_data" in view_data
        assert "report_metadata" in view_data
        assert "frozen_chart_configs" in view_data

    def test_view_data_structure(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """View data has expected keys and values"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        # dashboard_data keys
        dd = view_data["dashboard_data"]
        assert dd["title"] == "Test Dashboard"
        assert dd["dashboard_type"] == "native"
        assert dd["is_published"] is True
        assert "tabs" in dd
        assert "filters" in dd

        # report_metadata keys
        rm = view_data["report_metadata"]
        assert rm["snapshot_id"] == sample_snapshot.id
        assert rm["title"] == "Jan 2025 Report"
        assert rm["period_start"] == date(2025, 1, 1)
        assert rm["period_end"] == date(2025, 1, 31)
        assert rm["dashboard_title"] == "Test Dashboard"
        assert rm["dashboard_id"] == sample_snapshot.frozen_dashboard["dashboard_id"]

    def test_warehouse_discovered_column_injects_chart_filters(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """When date_column doesn't match a dashboard filter, chart-level filters are injected"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

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

    def test_view_data_includes_last_modified_by_null(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """report_metadata includes last_modified_by as None for unedited snapshots"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)
        rm = view_data["report_metadata"]
        assert "last_modified_by" in rm
        assert rm["last_modified_by"] is None

    def test_view_data_includes_last_modified_by_email(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org, orguser
    ):
        """report_metadata includes last_modified_by email after an update"""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        data = SnapshotUpdate(summary="Updated summary")
        ReportService.update_snapshot(sample_snapshot.id, data, orguser)

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)
        rm = view_data["report_metadata"]
        assert rm["last_modified_by"] == orguser.user.email

    def test_not_found(self, mock_org_warehouse_model, mock_factory, org):
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
        assert snapshot.frozen_dashboard["dashboard_id"] == sample_dashboard.id
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
        result = ReportService.list_snapshots(org, search="Jan", dashboard_title="wrong")
        assert len(result) == 0


# ================================================================================
# Test snapshot isolation — deleting charts/dashboards doesn't affect snapshots
# ================================================================================


@patch("ddpui.core.reports.report_service.WarehouseFactory")
@patch("ddpui.core.reports.report_service.OrgWarehouse")
class TestSnapshotIsolation:
    """Snapshots freeze config at creation time. Deleting the original
    dashboard or its charts must not affect snapshot view data."""

    def test_deleting_chart_does_not_affect_snapshot(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org, sample_chart
    ):
        """After deleting the original chart, get_snapshot_view_data still returns
        the full frozen chart config."""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        snapshot_id = sample_snapshot.id
        chart_id = sample_chart.id

        assert str(chart_id) in sample_snapshot.frozen_chart_configs

        sample_chart.delete()
        assert not Chart.objects.filter(id=chart_id).exists()

        view_data = ReportService.get_snapshot_view_data(snapshot_id, org)

        frozen_charts = view_data["frozen_chart_configs"]
        assert str(chart_id) in frozen_charts
        assert frozen_charts[str(chart_id)]["title"] == "Bar Chart"
        assert frozen_charts[str(chart_id)]["chart_type"] == "bar"
        assert frozen_charts[str(chart_id)]["schema_name"] == "public"
        assert frozen_charts[str(chart_id)]["table_name"] == "orders"

    def test_deleting_dashboard_does_not_affect_snapshot(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org, sample_dashboard
    ):
        """After deleting the original dashboard, get_snapshot_view_data still
        returns the full frozen dashboard config."""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        snapshot_id = sample_snapshot.id
        original_title = sample_dashboard.title

        sample_dashboard.delete()
        assert not Dashboard.objects.filter(id=sample_dashboard.id).exists()

        view_data = ReportService.get_snapshot_view_data(snapshot_id, org)

        dd = view_data["dashboard_data"]
        assert dd["title"] == original_title
        assert dd["dashboard_type"] == "native"
        assert "tabs" in dd
        assert "filters" in dd

        rm = view_data["report_metadata"]
        assert rm["dashboard_title"] == original_title

    def test_different_datetime_columns_produce_two_filters(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """When the report's date_column differs from the dashboard's existing
        datetime filter, two datetime filters appear in the view data:
        one is the original dashboard filter (untouched) and one is the
        injected display-only filter (locked with the snapshot period)."""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

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

        display_filter = datetime_filters[0]
        assert display_filter["column_name"] == "updated_at"
        assert display_filter["settings"]["locked"] is True
        assert display_filter["settings"]["default_start_date"] == "2025-01-01"
        assert display_filter["settings"]["default_end_date"] == "2025-01-31"
        assert display_filter["id"] < 0

        original_filter = datetime_filters[1]
        assert original_filter["column_name"] == "created_at"
        assert original_filter["settings"].get("locked") is not True


# ================================================================================
# Test cross-table filter injection (new behavior)
# ================================================================================


@patch("ddpui.core.reports.report_service.WarehouseFactory")
@patch("ddpui.core.reports.report_service.OrgWarehouse")
class TestCrossTableFilterInjection:
    """Tests for applying date filters across ALL charts whose table has the
    date column, not just the anchor table.

    New behavior: any chart whose table has a column with the same
    name (e.g. created_at) should get filtered.
    """

    def test_chart_on_different_table_with_same_column_gets_filtered(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot
    ):
        """A chart on public.customers (different table) should also get date
        filters if its table has a 'created_at' column.

        sample_snapshot.date_column = {schema: public, table: orders, column: created_at}
        """
        frozen_charts = {
            "1": {
                "id": 1,
                "title": "Orders Chart",
                "chart_type": "bar",
                "schema_name": "public",
                "table_name": "orders",
                "extra_config": {},
            },
            "2": {
                "id": 2,
                "title": "Customers Chart",
                "chart_type": "line",
                "schema_name": "public",
                "table_name": "customers",
                "extra_config": {},
            },
        }

        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_wh_client = MagicMock()
        mock_wh_client.column_exists.return_value = True
        mock_factory.get_warehouse_client.return_value = mock_wh_client

        ReportService._inject_period_into_chart_configs(frozen_charts, sample_snapshot)

        # Anchor table chart should be filtered
        orders_filters = frozen_charts["1"]["extra_config"].get("filters", [])
        assert len(orders_filters) == 2

        # Different table chart should ALSO be filtered
        customers_filters = frozen_charts["2"]["extra_config"].get("filters", [])
        assert len(customers_filters) == 2
        assert customers_filters[0]["column"] == "created_at"
        assert customers_filters[0]["operator"] == "greater_than_equal"
        assert customers_filters[1]["operator"] == "less_than_equal"

    def test_view_data_injects_chart_filters_even_when_dashboard_filter_matches(
        self, mock_org_warehouse_model, mock_factory, sample_snapshot, org
    ):
        """Even when a dashboard datetime filter matches (filter_matched=True),
        chart-level filters should still be injected."""
        mock_org_warehouse_model.objects.filter.return_value.first.return_value = MagicMock()
        mock_factory.get_warehouse_client.return_value = MagicMock()

        view_data = ReportService.get_snapshot_view_data(sample_snapshot.id, org)

        frozen_charts = view_data["frozen_chart_configs"]
        chart = frozen_charts.get("1")
        assert chart is not None

        filters = chart.get("extra_config", {}).get("filters", [])
        assert len(filters) == 2  # start + end date filters
        assert filters[0]["column"] == "created_at"


# ================================================================================
# Tab-based dashboard fixtures
# ================================================================================


@pytest.fixture
def tab_chart(orguser, org):
    chart = Chart.objects.create(
        title="Tab Line Chart",
        description="A chart inside a tab",
        chart_type="line",
        schema_name="public",
        table_name="sales",
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
def tab_dashboard(orguser, org, tab_chart):
    dashboard = Dashboard.objects.create(
        title="Tab Dashboard",
        dashboard_type="native",
        grid_columns=12,
        tabs=[
            {
                "id": "tab-1",
                "title": "Overview",
                "layout_config": [{"i": "chart-2", "x": 0, "y": 0, "w": 6, "h": 4}],
                "components": {
                    "chart-2": {
                        "id": "chart-2",
                        "type": "chart",
                        "config": {"chartId": tab_chart.id, "chartType": "line"},
                    },
                    "text-1": {
                        "id": "text-1",
                        "type": "text",
                        "config": {"content": "Tab text"},
                    },
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


# ================================================================================
# Test _extract_chart_ids
# ================================================================================


class TestExtractChartIds:
    """Tests for ReportService._extract_chart_ids"""

    def test_extracts_ids_from_tabs(self, tab_dashboard, tab_chart):
        """Charts inside tabs are discovered"""
        chart_ids = ReportService._extract_chart_ids(tab_dashboard)
        assert tab_chart.id in chart_ids

    def test_extracts_ids_from_tabs_sample(self, sample_dashboard, sample_chart):
        """Charts inside tabs are discovered (sample_dashboard fixture)"""
        chart_ids = ReportService._extract_chart_ids(sample_dashboard)
        assert sample_chart.id in chart_ids

    def test_deduplicates_when_chart_in_multiple_tabs(self, orguser, org):
        """Same chartId in multiple tabs appears only once"""
        dashboard = Dashboard.objects.create(
            title="Overlap",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T1",
                    "layout_config": [],
                    "components": {"chart-1": {"type": "chart", "config": {"chartId": 99}}},
                },
                {
                    "id": "tab-2",
                    "title": "T2",
                    "layout_config": [],
                    "components": {"chart-1": {"type": "chart", "config": {"chartId": 99}}},
                },
            ],
            created_by=orguser,
            org=org,
        )
        chart_ids = ReportService._extract_chart_ids(dashboard)
        assert chart_ids.count(99) == 1
        dashboard.delete()


# ================================================================================
# Test _freeze_dashboard with tabs
# ================================================================================


class TestFreezeDashboardTabs:
    """Tests for ReportService._freeze_dashboard on tab-based dashboards"""

    def test_tabs_captured_in_frozen_output(self, tab_dashboard):
        """tabs field is included and contains all tab data"""
        frozen = ReportService._freeze_dashboard(tab_dashboard)

        assert "tabs" in frozen
        assert len(frozen["tabs"]) == 1
        assert frozen["tabs"][0]["id"] == "tab-1"
        assert frozen["tabs"][0]["title"] == "Overview"


# ================================================================================
# Test _freeze_chart_configs with tabs
# ================================================================================


class TestFreezeChartConfigsTabs:
    """Tests for ReportService._freeze_chart_configs on tab-based dashboards"""

    def test_freezes_chart_from_tab(self, tab_dashboard, tab_chart):
        """Charts referenced inside tabs are frozen"""
        frozen = ReportService._freeze_chart_configs(tab_dashboard)

        assert str(tab_chart.id) in frozen
        chart_data = frozen[str(tab_chart.id)]
        assert chart_data["title"] == "Tab Line Chart"
        assert chart_data["chart_type"] == "line"
        assert chart_data["table_name"] == "sales"

    def test_non_chart_components_in_tabs_are_skipped(self, tab_dashboard, tab_chart):
        """Text components inside tabs are not included in frozen configs"""
        frozen = ReportService._freeze_chart_configs(tab_dashboard)
        assert len(frozen) == 1


# ================================================================================
# KPI Freezing Fixtures
# ================================================================================


@pytest.fixture
def sample_metric(orguser, org):
    from ddpui.models.metric import Metric

    metric = Metric.objects.create(
        name="Total Revenue",
        schema_name="public",
        table_name="orders",
        column="amount",
        aggregation="sum",
        org=org,
        created_by=orguser,
    )
    yield metric
    try:
        metric.refresh_from_db()
        metric.delete()
    except Metric.DoesNotExist:
        pass


@pytest.fixture
def sample_kpi(orguser, org, sample_metric):
    from ddpui.models.metric import KPI

    kpi = KPI.objects.create(
        metric=sample_metric,
        name="Revenue KPI",
        target_value=10000.0,
        direction="increase",
        green_threshold_pct=100.0,
        amber_threshold_pct=80.0,
        time_grain="monthly",
        time_dimension_column="created_at",
        metric_type_tag="outcome",
        program_tags=["education", "health"],
        org=org,
        created_by=orguser,
    )
    yield kpi
    try:
        kpi.refresh_from_db()
        kpi.delete()
    except KPI.DoesNotExist:
        pass


@pytest.fixture
def kpi_dashboard(orguser, org, sample_chart, sample_kpi):
    """Dashboard with both a chart and a KPI in tabs"""
    dashboard = Dashboard.objects.create(
        title="KPI Dashboard",
        dashboard_type="native",
        grid_columns=12,
        tabs=[
            {
                "id": "tab-1",
                "title": "Overview",
                "layout_config": [
                    {"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4},
                    {"i": "kpi-1", "x": 6, "y": 0, "w": 6, "h": 4},
                ],
                "components": {
                    "chart-1": {
                        "id": "chart-1",
                        "type": "chart",
                        "config": {"chartId": sample_chart.id},
                    },
                    "kpi-1": {
                        "id": "kpi-1",
                        "type": "kpi",
                        "config": {"kpiId": sample_kpi.id},
                    },
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


# ================================================================================
# Test _freeze_chart_configs with KPI components
# ================================================================================


@patch("ddpui.core.reports.report_service.OrgWarehouse")
@patch("ddpui.core.reports.report_service.KPIService")
class TestFreezeKpiConfigs:
    """Tests for KPI freezing in ReportService._freeze_chart_configs"""

    def test_freezes_kpi_from_tab(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi, sample_chart
    ):
        """KPI components inside tabs are frozen with all fields"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.return_value = [
            {"label": "Jan 2025", "value": 8000},
            {"label": "Feb 2025", "value": 9500},
        ]

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)

        # Both chart and KPI should be frozen
        assert str(sample_chart.id) in frozen
        assert str(sample_kpi.id) in frozen

        kpi_data = frozen[str(sample_kpi.id)]
        assert kpi_data["component_type"] == "kpi"
        assert kpi_data["title"] == "Revenue KPI"
        assert kpi_data["target_value"] == 10000.0
        assert kpi_data["direction"] == "increase"
        assert kpi_data["time_grain"] == "monthly"
        assert kpi_data["metric"]["name"] == "Total Revenue"
        assert kpi_data["metric"]["schema_name"] == "public"
        assert kpi_data["metric"]["table_name"] == "orders"
        assert kpi_data["metric"]["column"] == "amount"
        assert kpi_data["metric"]["aggregation"] == "sum"

    def test_kpi_frozen_with_trend_data(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """Frozen KPI captures computed trend periods and current value"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.return_value = [
            {"label": "Jan 2025", "value": 8000},
            {"label": "Feb 2025", "value": 9500},
        ]

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert len(kpi_data["periods"]) == 2
        assert kpi_data["periods"][-1]["value"] == 9500

    def test_kpi_frozen_with_rag_status(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """Frozen KPI includes computed RAG status"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.return_value = [
            {"label": "Feb 2025", "value": 10000},
        ]

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        # target=10000, current=10000, direction=increase, green_pct=100 → green
        assert kpi_data["rag_status"] == "green"

    def test_kpi_frozen_amber_status(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """KPI at 85% of target with green=100%, amber=80% should be amber"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.return_value = [
            {"label": "Feb 2025", "value": 8500},
        ]

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["rag_status"] == "amber"

    def test_kpi_frozen_red_status(self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi):
        """KPI at 70% of target with amber=80% should be red"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.return_value = [
            {"label": "Feb 2025", "value": 7000},
        ]

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["rag_status"] == "red"

    def test_kpi_frozen_no_warehouse(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """When no warehouse is available, KPI is frozen with empty periods"""
        mock_org_wh.objects.filter.return_value.first.return_value = None

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["periods"] == []
        assert kpi_data["rag_status"] is None

    def test_kpi_frozen_trend_error_graceful(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """If trend computation fails, KPI is frozen with empty periods"""
        mock_org_wh.objects.filter.return_value.first.return_value = MagicMock()
        mock_kpi_service.kpi_to_response.return_value = MagicMock()
        mock_kpi_service._compute_trend.side_effect = Exception("Warehouse error")

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["periods"] == []

    def test_kpi_extra_config_frozen_with_customizations(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """Customizations on the live KPI are copied into the frozen blob."""
        mock_org_wh.objects.filter.return_value.first.return_value = None
        sample_kpi.extra_config = {
            "customizations": {
                "numberFormat": "indian",
                "decimalPlaces": 0,
                "numberPrefix": "₹",
                "numberSuffix": "",
            }
        }
        sample_kpi.save(update_fields=["extra_config"])

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["extra_config"] == {
            "customizations": {
                "numberFormat": "indian",
                "decimalPlaces": 0,
                "numberPrefix": "₹",
                "numberSuffix": "",
            }
        }

    def test_kpi_extra_config_frozen_empty_for_pre_v11_kpi(
        self, mock_kpi_service, mock_org_wh, kpi_dashboard, sample_kpi
    ):
        """A KPI with no customizations freezes extra_config as {} —
        snapshots taken before v1.1 render with no formatting."""
        mock_org_wh.objects.filter.return_value.first.return_value = None
        # sample_kpi fixture creates the row without customizations → JSONField
        # default = {}, simulating a pre-v1.1 KPI.
        assert sample_kpi.extra_config == {}

        frozen = ReportService._freeze_chart_configs(kpi_dashboard)
        kpi_data = frozen[str(sample_kpi.id)]

        assert kpi_data["extra_config"] == {}

    def test_kpi_only_dashboard(self, mock_kpi_service, mock_org_wh, orguser, org, sample_kpi):
        """Dashboard with only KPI components (no charts)"""
        mock_org_wh.objects.filter.return_value.first.return_value = None
        dashboard = Dashboard.objects.create(
            title="KPI Only",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "KPIs",
                    "components": {
                        "kpi-1": {
                            "type": "kpi",
                            "config": {"kpiId": sample_kpi.id},
                        },
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        frozen = ReportService._freeze_chart_configs(dashboard)

        assert str(sample_kpi.id) in frozen
        assert frozen[str(sample_kpi.id)]["component_type"] == "kpi"
        dashboard.delete()

    def test_kpi_component_without_kpi_id(self, mock_kpi_service, mock_org_wh, orguser, org):
        """KPI component without kpiId in config is skipped"""
        dashboard = Dashboard.objects.create(
            title="Missing KPI ID",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "kpi-1": {
                            "type": "kpi",
                            "config": {},  # No kpiId
                        },
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        frozen = ReportService._freeze_chart_configs(dashboard)
        assert frozen == {}
        dashboard.delete()

    def test_kpi_expression_metric_frozen(self, mock_kpi_service, mock_org_wh, orguser, org):
        """KPI with an expression metric is frozen with column_expression"""
        mock_org_wh.objects.filter.return_value.first.return_value = None
        from ddpui.models.metric import Metric, KPI as KPIModel

        metric = Metric.objects.create(
            name="Profit Ratio",
            schema_name="public",
            table_name="financials",
            column_expression="SUM(revenue - cost) / COUNT(DISTINCT id)",
            org=org,
            created_by=orguser,
        )
        kpi = KPIModel.objects.create(
            metric=metric,
            name="Profit KPI",
            direction="increase",
            time_grain="monthly",
            org=org,
            created_by=orguser,
        )
        dashboard = Dashboard.objects.create(
            title="Expr Dashboard",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "kpi-1": {"type": "kpi", "config": {"kpiId": kpi.id}},
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )

        frozen = ReportService._freeze_chart_configs(dashboard)
        kpi_data = frozen[str(kpi.id)]

        assert kpi_data["metric"]["column_expression"] == "SUM(revenue - cost) / COUNT(DISTINCT id)"
        assert kpi_data["metric"]["column"] is None
        assert kpi_data["metric"]["aggregation"] is None

        dashboard.delete()
        kpi.delete()
        metric.delete()


# ================================================================================
# Test get_report_kpi_data
# ================================================================================


class TestGetReportKpiData:
    """Tests for ReportService.get_report_kpi_data"""

    @patch("ddpui.core.reports.report_service.KPIService")
    def test_kpi_not_found_in_snapshot(self, mock_kpi_service, sample_snapshot, org):
        """Requesting a KPI ID not in the snapshot raises SnapshotValidationError"""
        with pytest.raises(SnapshotValidationError, match="not found"):
            ReportService.get_report_kpi_data(sample_snapshot.id, 99999, org)

    @patch("ddpui.core.reports.report_service.KPIService")
    def test_non_kpi_entry_raises(self, mock_kpi_service, sample_snapshot, org, sample_chart):
        """Requesting a chart ID via get_report_kpi_data raises SnapshotValidationError"""
        # sample_snapshot has chart with id=sample_chart.id frozen in it
        with pytest.raises(SnapshotValidationError, match="not a KPI"):
            ReportService.get_report_kpi_data(sample_snapshot.id, sample_chart.id, org)

    @patch("ddpui.core.kpi.kpi_service.KPIService.compute_kpi_data")
    def test_returns_kpi_data_from_frozen_config(
        self, mock_compute, orguser, org, sample_chart, sample_kpi
    ):
        """get_report_kpi_data builds KPIResponse from frozen config and delegates to compute"""
        # Create a dashboard with a KPI and freeze it
        dashboard = Dashboard.objects.create(
            title="Report Dashboard",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "kpi-1": {"type": "kpi", "config": {"kpiId": sample_kpi.id}},
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        f = DashboardFilter.objects.create(
            dashboard=dashboard,
            name="Date",
            filter_type="datetime",
            schema_name="public",
            table_name="orders",
            column_name="created_at",
            settings={},
            order=0,
        )
        snapshot = ReportService.create_snapshot(
            title="KPI Report",
            dashboard_id=dashboard.id,
            date_column={
                "schema_name": "public",
                "table_name": "orders",
                "column_name": "created_at",
            },
            period_start=date(2025, 1, 1),
            period_end=date(2025, 3, 31),
            orguser=orguser,
        )

        # Verify KPI was frozen
        assert str(sample_kpi.id) in snapshot.frozen_chart_configs
        frozen_kpi = snapshot.frozen_chart_configs[str(sample_kpi.id)]
        assert frozen_kpi["component_type"] == "kpi"

        # Mock the compute call
        mock_compute.return_value = {"data": {"current_value": 9500}, "echarts_config": {}}

        result = ReportService.get_report_kpi_data(snapshot.id, sample_kpi.id, org)

        assert result == {"data": {"current_value": 9500}, "echarts_config": {}}
        mock_compute.assert_called_once()

        # Verify the KPIResponse was constructed correctly
        call_args = mock_compute.call_args
        kpi_response = call_args[0][0]
        assert kpi_response.name == "Revenue KPI"
        assert kpi_response.metric.name == "Total Revenue"
        assert kpi_response.target_value == 10000.0

        # Verify date filter was passed (same schema/table as metric)
        date_filter = (
            call_args[1].get("date_filter") or call_args[0][2]
            if len(call_args[0]) > 2
            else call_args[1].get("date_filter")
        )
        assert date_filter is not None
        assert date_filter["start"] == "2025-01-01"
        assert date_filter["end"] == "2025-03-31"

        snapshot.delete()
        f.delete()
        dashboard.delete()

    @patch("ddpui.core.kpi.kpi_service.KPIService.compute_kpi_data")
    def test_snapshot_uses_frozen_customizations_not_live(
        self, mock_compute, orguser, org, sample_kpi
    ):
        """Editing the KPI's customizations AFTER snapshot must not affect the
        snapshot — the snapshot reads from frozen extra_config."""
        sample_kpi.extra_config = {
            "customizations": {
                "numberFormat": "indian",
                "decimalPlaces": 0,
                "numberPrefix": "₹",
                "numberSuffix": "",
            }
        }
        sample_kpi.save(update_fields=["extra_config"])

        dashboard = Dashboard.objects.create(
            title="Frozen Cust Test",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "kpi-1": {"type": "kpi", "config": {"kpiId": sample_kpi.id}},
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        snapshot = ReportService.create_snapshot(
            title="Frozen Cust Report",
            dashboard_id=dashboard.id,
            date_column={},
            period_end=date(2025, 3, 31),
            orguser=orguser,
        )

        # Now mutate the live KPI's customizations — snapshot should be unaffected.
        sample_kpi.extra_config = {
            "customizations": {"numberFormat": "european", "decimalPlaces": 2}
        }
        sample_kpi.save(update_fields=["extra_config"])

        mock_compute.return_value = {"data": {}, "echarts_config": {}}
        ReportService.get_report_kpi_data(snapshot.id, sample_kpi.id, org)

        kpi_response = mock_compute.call_args[0][0]
        # KPIResponse built from the FROZEN config — original 'indian' format
        assert kpi_response.extra_config.customizations is not None
        assert kpi_response.extra_config.customizations.numberFormat == "indian"
        assert kpi_response.extra_config.customizations.numberPrefix == "₹"

        snapshot.delete()
        dashboard.delete()

    @patch("ddpui.core.kpi.kpi_service.KPIService.compute_kpi_data")
    def test_kpi_survives_deletion(self, mock_compute, orguser, org, sample_chart, sample_kpi):
        """Frozen KPI data is available even after the original KPI is deleted"""
        dashboard = Dashboard.objects.create(
            title="Deletion Test",
            dashboard_type="native",
            grid_columns=12,
            tabs=[
                {
                    "id": "tab-1",
                    "title": "T",
                    "components": {
                        "kpi-1": {"type": "kpi", "config": {"kpiId": sample_kpi.id}},
                    },
                }
            ],
            created_by=orguser,
            org=org,
        )
        snapshot = ReportService.create_snapshot(
            title="Deletion Report",
            dashboard_id=dashboard.id,
            date_column={},
            period_end=date(2025, 3, 31),
            orguser=orguser,
        )

        kpi_id = sample_kpi.id
        assert str(kpi_id) in snapshot.frozen_chart_configs

        # Delete the original KPI and metric
        sample_kpi.delete()

        # get_report_kpi_data should still work from frozen config
        mock_compute.return_value = {"data": {}, "echarts_config": {}}
        result = ReportService.get_report_kpi_data(snapshot.id, kpi_id, org)
        assert result is not None

        snapshot.delete()
        dashboard.delete()
