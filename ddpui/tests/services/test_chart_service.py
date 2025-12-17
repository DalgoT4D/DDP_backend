"""Service Tests for ChartService

Tests business logic NOT covered by API tests:
1. delete_chart permission check (creator-only) 
2. bulk_delete_charts - all edge cases
3. get_chart_dashboards - multiple dashboards
4. Exception classes
5. ChartData dataclass
"""

import os
import django
import pytest
from unittest.mock import patch

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.models.dashboard import Dashboard, DashboardComponentType
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.services.chart_service import (
    ChartService,
    ChartData,
    ChartNotFoundError,
    ChartValidationError,
    ChartPermissionError,
)
from ddpui.services.warehouse_service import WarehouseService
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="chartserviceuser", email="chartserviceuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def authuser2():
    """A second django User object for permission testing"""
    user = User.objects.create(
        username="chartserviceuser2", email="chartserviceuser2@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Chart Service Test Org",
        slug="chart-svc-test-org",  # max_length=20
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def other_org():
    """Another Org for cross-org testing"""
    org = Org.objects.create(
        name="Other Org",
        slug="other-org-svc",
        airbyte_workspace_id="workspace-id-2",
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
def sample_chart(orguser, org):
    """A sample chart for testing"""
    chart = Chart.objects.create(
        title="Test Chart",
        description="Test Description",
        chart_type="bar",
        schema_name="public",
        table_name="users",
        extra_config={},
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
# Test delete_chart permission check (NOT in API tests - API tests wrong org)
# ================================================================================


class TestDeleteChartPermissions:
    """Tests for ChartService.delete_chart() permission logic"""

    def test_delete_chart_permission_denied_not_creator(self, orguser, orguser2, org, seed_db):
        """Test that only creator can delete chart"""
        # Create chart by orguser
        chart = Chart.objects.create(
            title="Protected Chart",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        # orguser2 tries to delete - should fail
        with pytest.raises(ChartPermissionError) as excinfo:
            ChartService.delete_chart(chart.id, org, orguser2)

        assert excinfo.value.error_code == "PERMISSION_DENIED"
        assert "only delete charts you created" in excinfo.value.message

        # Cleanup
        chart.delete()

    def test_delete_chart_creator_can_delete(self, orguser, org, seed_db):
        """Test that creator can delete their own chart"""
        chart = Chart.objects.create(
            title="My Chart",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )
        chart_id = chart.id

        result = ChartService.delete_chart(chart_id, org, orguser)

        assert result is True
        assert not Chart.objects.filter(id=chart_id).exists()


# ================================================================================
# Test bulk_delete_charts (NOT fully covered by API tests)
# ================================================================================


class TestBulkDeleteCharts:
    """Tests for ChartService.bulk_delete_charts()"""

    def test_bulk_delete_all_missing(self, orguser, seed_db):
        """Test bulk delete when all charts are missing"""
        result = ChartService.bulk_delete_charts([99997, 99998, 99999], orguser.org, orguser)

        assert result["deleted_count"] == 0
        assert result["requested_count"] == 3
        assert len(result["missing_ids"]) == 3

    def test_bulk_delete_wrong_org(self, orguser, org, other_org, seed_db):
        """Test bulk delete doesn't delete charts from other org"""
        # Create chart in org
        chart = Chart.objects.create(
            title="Org Chart",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
            created_by=orguser,
            last_modified_by=orguser,
            org=org,
        )

        # Try to delete using other_org
        result = ChartService.bulk_delete_charts([chart.id], other_org, orguser)

        assert result["deleted_count"] == 0
        assert chart.id in result["missing_ids"]

        # Chart should still exist
        assert Chart.objects.filter(id=chart.id).exists()

        # Cleanup
        chart.delete()

    def test_bulk_delete_empty_list(self, orguser, seed_db):
        """Test bulk delete with empty list"""
        result = ChartService.bulk_delete_charts([], orguser.org, orguser)

        assert result["deleted_count"] == 0
        assert result["requested_count"] == 0
        assert result["missing_ids"] == []


# ================================================================================
# Test get_chart_dashboards (NOT fully covered by API tests)
# ================================================================================


class TestGetChartDashboards:
    """Tests for ChartService.get_chart_dashboards()"""

    def test_get_chart_dashboards_multiple(self, orguser, sample_chart, org, seed_db):
        """Test when chart is used in multiple dashboards"""
        dashboards_created = []
        for i in range(3):
            dashboard = Dashboard.objects.create(
                title=f"Dashboard {i}",
                dashboard_type="native",
                grid_columns=12,
                layout_config=[],
                components={
                    "1": {
                        "type": DashboardComponentType.CHART.value,
                        "config": {"chartId": sample_chart.id},
                    }
                },
                created_by=orguser,
                org=org,
            )
            dashboards_created.append(dashboard)

        result = ChartService.get_chart_dashboards(sample_chart.id, org)

        assert len(result) == 3

        # Cleanup
        for d in dashboards_created:
            d.delete()


# ================================================================================
# Test get_org_warehouse
# ================================================================================


class TestGetOrgWarehouse:
    """Tests for WarehouseService.get_org_warehouse()"""

    def test_get_org_warehouse_success(self, org, seed_db):
        """Test getting warehouse for org"""
        warehouse = OrgWarehouse.objects.create(
            org=org,
            wtype="postgres",
            credentials={},
        )

        result = WarehouseService.get_org_warehouse(org)

        assert result is not None
        assert result.id == warehouse.id
        assert result.wtype == "postgres"

        # Cleanup
        warehouse.delete()

    def test_get_org_warehouse_not_found(self, org, seed_db):
        """Test getting warehouse when none exists"""
        result = WarehouseService.get_org_warehouse(org)

        assert result is None


# ================================================================================
# Test Exception Classes
# ================================================================================


class TestExceptionClasses:
    """Tests for custom exception classes"""

    def test_chart_not_found_error(self):
        """Test ChartNotFoundError attributes"""
        error = ChartNotFoundError(123)

        assert error.chart_id == 123
        assert error.error_code == "CHART_NOT_FOUND"
        assert "123" in error.message

    def test_chart_validation_error(self):
        """Test ChartValidationError attributes"""
        error = ChartValidationError("Invalid chart type")

        assert error.error_code == "VALIDATION_ERROR"
        assert error.message == "Invalid chart type"

    def test_chart_permission_error(self):
        """Test ChartPermissionError attributes"""
        error = ChartPermissionError("You cannot do this")

        assert error.error_code == "PERMISSION_DENIED"
        assert error.message == "You cannot do this"

    def test_chart_permission_error_default_message(self):
        """Test ChartPermissionError default message"""
        error = ChartPermissionError()

        assert error.message == "Permission denied"


# ================================================================================
# Test ChartData Dataclass
# ================================================================================


class TestChartDataDataclass:
    """Tests for ChartData dataclass"""

    def test_chart_data_all_fields(self):
        """Test ChartData with all fields"""
        data = ChartData(
            title="Test",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={"key": "value"},
            description="Description",
        )

        assert data.title == "Test"
        assert data.chart_type == "bar"
        assert data.description == "Description"

    def test_chart_data_optional_description(self):
        """Test ChartData without description"""
        data = ChartData(
            title="Test",
            chart_type="bar",
            schema_name="public",
            table_name="users",
            extra_config={},
        )

        assert data.description is None
