"""
Shared test fixtures for the ddpui test suite.

Provides the most commonly-used fixtures (authuser, org, orguser, etc.)
so that individual test files don't need to duplicate them.
"""

import pytest

from django.core.management import call_command
from django.contrib.auth.models import User

from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE


# =============================================================================
# Database seeding
# =============================================================================


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    """Load roles, permissions, and role_permissions JSON fixtures."""
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


# =============================================================================
# Users
# =============================================================================


@pytest.fixture
def authuser():
    """A django User object."""
    user = User.objects.create(
        username="testuser", email="testuser@example.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def authuser2():
    """A second django User object for permission testing."""
    user = User.objects.create(
        username="testuser2", email="testuser2@example.com", password="testpassword"
    )
    yield user
    user.delete()


# =============================================================================
# Orgs
# =============================================================================


@pytest.fixture
def org():
    """An Org with an airbyte workspace id."""
    org = Org.objects.create(
        name="Test Org",
        slug="test-org-slug",
        airbyte_workspace_id="fake-workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def org_without_workspace():
    """An Org without an airbyte workspace."""
    org = Org.objects.create(
        airbyte_workspace_id=None, slug="test-org-WO-slug", name="test-org-WO-name"
    )
    yield org
    org.delete()


@pytest.fixture
def org_with_workspace():
    """An Org with an airbyte workspace."""
    org = Org.objects.create(
        name="org-name",
        airbyte_workspace_id="FAKE-WORKSPACE-ID",
        slug="test-org-W-slug",
    )
    yield org
    org.delete()


# =============================================================================
# OrgUsers
# =============================================================================


@pytest.fixture
def orguser(authuser, org):
    """An OrgUser with the account-manager role."""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguser2(authuser2, org):
    """A second OrgUser for permission testing."""
    orguser = OrgUser.objects.create(
        user=authuser2,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# =============================================================================
# Warehouse
# =============================================================================


@pytest.fixture
def org_warehouse(org):
    """An OrgWarehouse (postgres) attached to the org."""
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="Test Warehouse",
        airbyte_destination_id="dest-id",
    )
    yield warehouse
    warehouse.delete()


# =============================================================================
# Dashboard / Chart / Filter
# =============================================================================


@pytest.fixture
def sample_dashboard(orguser, org):
    """A sample dashboard for testing."""
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
    """A sample chart for testing."""
    chart = Chart.objects.create(
        title="Test Chart",
        description="Test Description",
        chart_type="bar",
        schema_name="public",
        table_name="users",
        extra_config={
            "dimension_column": "category",
            "metrics": [{"column": "revenue", "aggregation": "sum"}],
        },
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


@pytest.fixture
def sample_filter(sample_dashboard):
    """A sample DashboardFilter for testing."""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="Status Filter",
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
        settings={},
        order=0,
    )
    yield filter_obj
    try:
        filter_obj.refresh_from_db()
        filter_obj.delete()
    except DashboardFilter.DoesNotExist:
        pass
