"""Service Tests for DashboardService

Tests business logic NOT covered by API tests:
1. update_dashboard lock checking (locked by other user)
2. delete_dashboard permission checks (creator-only, org default, landing page, locked)
3. create_filter invalid type validation
4. Exception classes
5. Data classes (DashboardData, FilterData)
"""

import os
import django
import pytest
from datetime import timedelta
from unittest.mock import patch

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardLock
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.services.dashboard_service import (
    DashboardService,
    DashboardData,
    FilterData,
    DashboardNotFoundError,
    DashboardLockedError,
    DashboardPermissionError,
    DashboardServiceError,
    FilterNotFoundError,
    FilterValidationError,
)
from ddpui.schemas.dashboard_schema import DashboardUpdate
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="dashserviceuser", email="dashserviceuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def authuser2():
    """A second django User object for permission testing"""
    user = User.objects.create(
        username="dashserviceuser2", email="dashserviceuser2@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Dashboard Service Test Org",
        slug="dash-svc-test-org",  # max_length=20
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
def orguser2(authuser2, org):
    """A second OrgUser for permission testing"""
    orguser = OrgUser.objects.create(
        user=authuser2,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def sample_dashboard(orguser, org):
    """A sample dashboard for testing"""
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test Description",
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


@pytest.fixture
def sample_chart(orguser, org):
    """A sample chart for dashboard export tests."""
    chart = Chart.objects.create(
        title="Program Reach",
        description="Program reach by month",
        chart_type="line",
        schema_name="analytics",
        table_name="program_reach",
        extra_config={"metric": "beneficiaries"},
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield chart
    try:
        chart.refresh_from_db()
        chart.delete()
    except Chart.DoesNotExist:
        pass


# ================================================================================
# Test update_dashboard lock checking (NOT in API tests)
# ================================================================================


class TestUpdateDashboardLockChecking:
    """Tests for DashboardService.update_dashboard() lock behavior"""

    def test_update_dashboard_locked_by_other_user(
        self, orguser, orguser2, sample_dashboard, seed_db
    ):
        """Test that updating locked dashboard raises error"""
        # Create a lock by another user
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        with pytest.raises(DashboardLockedError) as excinfo:
            DashboardService.update_dashboard(
                sample_dashboard.id,
                orguser.org,
                orguser,
                DashboardUpdate(title="New Title"),
            )

        assert orguser2.user.email in excinfo.value.locked_by_email

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()


class TestExportDashboardContext:
    """Tests for DashboardService.export_dashboard_context()."""

    def test_export_dashboard_context_includes_all_referenced_charts(
        self, org, sample_dashboard, sample_chart, orguser, seed_db
    ):
        """Test export includes chart metadata for every chart component reference."""
        second_chart = Chart.objects.create(
            title="Attendance Trend",
            description="Attendance by month",
            chart_type="bar",
            schema_name="analytics",
            table_name="attendance",
            extra_config={"metric": "attendance_count"},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )
        sample_dashboard.components = {
            "chart-1": {"id": "chart-1", "type": "chart", "config": {"chartId": sample_chart.id}},
            "chart-2": {"id": "chart-2", "type": "chart", "config": {"chartId": second_chart.id}},
            "text-1": {"id": "text-1", "type": "text", "config": {"content": "Intro"}},
        }
        sample_dashboard.save(update_fields=["components"])

        export_data = DashboardService.export_dashboard_context(sample_dashboard.id, org)

        assert export_data["dashboard"]["id"] == sample_dashboard.id
        assert {chart["id"] for chart in export_data["charts"]} == {
            sample_chart.id,
            second_chart.id,
        }

        second_chart.delete()

    def test_export_dashboard_context_skips_missing_chart_references(
        self, org, sample_dashboard, sample_chart, seed_db
    ):
        """Test export skips broken chart references without failing the full export."""
        sample_dashboard.components = {
            "chart-1": {"id": "chart-1", "type": "chart", "config": {"chartId": sample_chart.id}},
            "chart-missing": {"id": "chart-missing", "type": "chart", "config": {"chartId": 99999}},
        }
        sample_dashboard.save(update_fields=["components"])

        export_data = DashboardService.export_dashboard_context(sample_dashboard.id, org)

        assert len(export_data["charts"]) == 1
        assert export_data["charts"][0]["id"] == sample_chart.id

    def test_update_dashboard_locked_by_same_user_succeeds(
        self, orguser, sample_dashboard, seed_db
    ):
        """Test that updating own locked dashboard succeeds"""
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        dashboard = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(title="Updated by Lock Owner"),
        )

        assert dashboard.title == "Updated by Lock Owner"

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()

    def test_update_dashboard_expired_lock_succeeds(
        self, orguser, orguser2, sample_dashboard, seed_db
    ):
        """Test that updating with expired lock succeeds"""
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() - timedelta(minutes=1),  # Expired
        )

        dashboard = DashboardService.update_dashboard(
            sample_dashboard.id,
            orguser.org,
            orguser,
            DashboardUpdate(title="Updated After Expiry"),
        )

        assert dashboard.title == "Updated After Expiry"

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()


# ================================================================================
# Test delete_dashboard permission checks (NOT in API tests)
# ================================================================================


class TestDeleteDashboardPermissions:
    """Tests for DashboardService.delete_dashboard() permission logic"""

    def test_delete_dashboard_permission_denied_not_creator(self, orguser, orguser2, org, seed_db):
        """Test that only creator can delete dashboard"""
        dashboard = Dashboard.objects.create(
            title="Protected Dashboard",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=org,
        )

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser2)

        assert "only delete dashboards you created" in excinfo.value.message

        # Cleanup
        dashboard.delete()

    def test_delete_dashboard_org_default_fails(self, orguser, org, seed_db):
        """Test that org default dashboard cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Org Default Dashboard",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            is_org_default=True,
            created_by=orguser,
            org=org,
        )

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        assert "default dashboard" in excinfo.value.message.lower()

        # Cleanup
        dashboard.delete()

    def test_delete_dashboard_with_landing_page_fails(self, orguser, org, seed_db):
        """Test that dashboard set as landing page cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Landing Page Dashboard",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=org,
        )
        orguser.landing_dashboard = dashboard
        orguser.save()

        with pytest.raises(DashboardPermissionError) as excinfo:
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        assert "landing page" in excinfo.value.message.lower()

        # Cleanup
        orguser.landing_dashboard = None
        orguser.save()
        dashboard.delete()

    def test_delete_dashboard_locked_fails(self, orguser, orguser2, org, seed_db):
        """Test that locked dashboard cannot be deleted"""
        dashboard = Dashboard.objects.create(
            title="Locked Dashboard",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=org,
        )
        DashboardLock.objects.create(
            dashboard=dashboard,
            locked_by=orguser2,
            lock_token="test-token",
            expires_at=timezone.now() + timedelta(minutes=2),
        )

        with pytest.raises(DashboardLockedError):
            DashboardService.delete_dashboard(dashboard.id, org, orguser)

        # Cleanup
        DashboardLock.objects.filter(dashboard=dashboard).delete()
        dashboard.delete()


# ================================================================================
# Test create_filter validation (NOT in API tests)
# ================================================================================


class TestCreateFilterValidation:
    """Tests for DashboardService.create_filter() validation"""

    def test_create_filter_invalid_type(self, sample_dashboard, org, seed_db):
        """Test creating filter with invalid type raises error"""
        filter_data = FilterData(
            filter_type="invalid_type",
            schema_name="public",
            table_name="users",
            column_name="status",
        )

        with pytest.raises(FilterValidationError) as excinfo:
            DashboardService.create_filter(sample_dashboard.id, org, filter_data)

        assert "invalid_type" in excinfo.value.message.lower()


# ================================================================================
# Test Exception Classes
# ================================================================================


class TestExceptionClasses:
    """Tests for custom exception classes"""

    def test_dashboard_not_found_error(self):
        """Test DashboardNotFoundError attributes"""
        error = DashboardNotFoundError(123)

        assert error.dashboard_id == 123
        assert error.error_code == "DASHBOARD_NOT_FOUND"
        assert "123" in error.message

    def test_dashboard_locked_error(self):
        """Test DashboardLockedError attributes"""
        error = DashboardLockedError("user@example.com")

        assert error.locked_by_email == "user@example.com"
        assert error.error_code == "DASHBOARD_LOCKED"
        assert "user@example.com" in error.message

    def test_dashboard_permission_error(self):
        """Test DashboardPermissionError attributes"""
        error = DashboardPermissionError("You cannot do this")

        assert error.error_code == "PERMISSION_DENIED"
        assert error.message == "You cannot do this"

    def test_dashboard_permission_error_default_message(self):
        """Test DashboardPermissionError default message"""
        error = DashboardPermissionError()

        assert error.message == "Permission denied"

    def test_filter_not_found_error(self):
        """Test FilterNotFoundError attributes"""
        error = FilterNotFoundError(456)

        assert error.filter_id == 456
        assert error.error_code == "FILTER_NOT_FOUND"
        assert "456" in error.message

    def test_filter_validation_error(self):
        """Test FilterValidationError attributes"""
        error = FilterValidationError("Invalid filter type")

        assert error.error_code == "FILTER_VALIDATION_ERROR"
        assert error.message == "Invalid filter type"


# ================================================================================
# Test Data Classes
# ================================================================================


class TestDataClasses:
    """Tests for data classes"""

    def test_dashboard_data_all_fields(self):
        """Test DashboardData with all fields"""
        data = DashboardData(
            title="Test Dashboard",
            description="Description",
            grid_columns=24,
        )

        assert data.title == "Test Dashboard"
        assert data.description == "Description"
        assert data.grid_columns == 24

    def test_dashboard_data_optional_fields(self):
        """Test DashboardData without optional fields"""
        data = DashboardData(title="Minimal Dashboard")

        assert data.title == "Minimal Dashboard"
        assert data.description is None
        assert data.grid_columns == 12  # Default

    def test_filter_data_all_fields(self):
        """Test FilterData with all fields"""
        data = FilterData(
            filter_type="value",
            schema_name="public",
            table_name="users",
            column_name="status",
            name="Status Filter",
            settings={"key": "value"},
            order=1,
        )

        assert data.filter_type == "value"
        assert data.name == "Status Filter"
        assert data.order == 1

    def test_filter_data_optional_fields(self):
        """Test FilterData without optional fields"""
        data = FilterData(
            filter_type="value",
            schema_name="public",
            table_name="users",
            column_name="status",
        )

        assert data.name is None
        assert data.settings is None
        assert data.order == 0  # Default
