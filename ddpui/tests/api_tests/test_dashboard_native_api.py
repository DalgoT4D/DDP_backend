"""Test cases for native dashboard API endpoints"""

import os
import json
import django
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError
from datetime import datetime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardLock
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.dashboard_native_api import (
    list_dashboards,
    get_dashboard,
    create_dashboard,
    update_dashboard,
    delete_dashboard,
    create_filter,
    update_filter,
    delete_filter,
    DashboardCreate,
    DashboardUpdate,
    FilterCreate,
    FilterUpdate,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db

# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="dashuser", email="dashuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Test Org",
        slug="test-org",
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
    dashboard.delete()


@pytest.fixture
def sample_chart(orguser, org):
    """A sample chart for dashboard testing"""
    chart = Chart.objects.create(
        title="Test Chart",
        chart_type="bar",
        computation_type="raw",
        schema_name="public",
        table_name="users",
        extra_config={},
        created_by=orguser,
        org=org,
    )
    yield chart
    chart.delete()


# ================================================================================
# Test list_dashboards endpoint
# ================================================================================


def test_list_dashboards_success(orguser, sample_dashboard, seed_db):
    """Test successfully listing dashboards"""
    request = mock_request(orguser)

    response = list_dashboards(request)

    assert len(response) == 1
    assert response[0].title == "Test Dashboard"
    assert response[0].dashboard_type == "native"


def test_list_dashboards_with_search(orguser, sample_dashboard, seed_db):
    """Test listing dashboards with search filter"""
    request = mock_request(orguser)

    response = list_dashboards(request, search="Test")

    assert len(response) == 1
    assert response[0].title == "Test Dashboard"


def test_list_dashboards_with_search_no_results(orguser, sample_dashboard, seed_db):
    """Test listing dashboards with search that returns no results"""
    request = mock_request(orguser)

    response = list_dashboards(request, search="Nonexistent")

    assert len(response) == 0


def test_list_dashboards_filter_by_dashboard_type(orguser, sample_dashboard, seed_db):
    """Test listing dashboards filtered by type"""
    request = mock_request(orguser)

    response = list_dashboards(request, dashboard_type="native")

    assert len(response) == 1
    assert response[0].dashboard_type == "native"


def test_list_dashboards_filter_by_published_status(orguser, org, seed_db):
    """Test listing dashboards filtered by published status"""
    # Create published dashboard
    Dashboard.objects.create(
        title="Published Dashboard",
        dashboard_type="native",
        is_published=True,
        created_by=orguser,
        org=org,
    )

    # Create unpublished dashboard
    Dashboard.objects.create(
        title="Unpublished Dashboard",
        dashboard_type="native",
        is_published=False,
        created_by=orguser,
        org=org,
    )

    request = mock_request(orguser)

    # Test published only
    published = list_dashboards(request, is_published=True)
    assert len(published) == 1
    assert published[0].is_published is True

    # Test unpublished only
    unpublished = list_dashboards(request, is_published=False)
    assert len(unpublished) == 1
    assert unpublished[0].is_published is False


def test_list_dashboards_empty_org(orguser, seed_db):
    """Test listing dashboards when org has no dashboards"""
    request = mock_request(orguser)

    response = list_dashboards(request)

    assert len(response) == 0


# ================================================================================
# Test get_dashboard endpoint
# ================================================================================


def test_get_dashboard_success(orguser, sample_dashboard, seed_db):
    """Test successfully getting a single dashboard"""
    request = mock_request(orguser)

    response = get_dashboard(request, dashboard_id=sample_dashboard.id)

    assert response.id == sample_dashboard.id
    assert response.title == "Test Dashboard"
    assert response.dashboard_type == "native"


def test_get_dashboard_not_found(orguser, seed_db):
    """Test getting a non-existent dashboard"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dashboard(request, dashboard_id=99999)

    assert excinfo.value.status_code == 404


def test_get_dashboard_wrong_org(orguser, seed_db):
    """Test getting a dashboard from another org"""
    # Create another org and dashboard
    other_org = Org.objects.create(name="Other Org", slug="other-org")
    other_user = User.objects.create(username="otheruser", email="other@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    other_dashboard = Dashboard.objects.create(
        title="Other Dashboard",
        dashboard_type="native",
        created_by=other_orguser,
        org=other_org,
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_dashboard(request, dashboard_id=other_dashboard.id)

    assert excinfo.value.status_code == 404

    # Cleanup
    other_dashboard.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


# ================================================================================
# Test create_dashboard endpoint
# ================================================================================


def test_create_dashboard_success(orguser, seed_db):
    """Test successfully creating a dashboard"""
    request = mock_request(orguser)

    payload = DashboardCreate(
        title="New Dashboard",
        description="New Description",
        grid_columns=12,
    )

    response = create_dashboard(request, payload)

    assert response.title == "New Dashboard"
    assert response.description == "New Description"
    assert response.grid_columns == 12
    assert response.dashboard_type == "native"

    # Cleanup
    Dashboard.objects.filter(id=response.id).delete()


def test_create_dashboard_minimal(orguser, seed_db):
    """Test creating a dashboard with minimal info"""
    request = mock_request(orguser)

    payload = DashboardCreate(
        title="Minimal Dashboard",
    )

    response = create_dashboard(request, payload)

    assert response.title == "Minimal Dashboard"
    assert response.description is None
    assert response.grid_columns == 12  # default value

    # Cleanup
    Dashboard.objects.filter(id=response.id).delete()


# ================================================================================
# Test update_dashboard endpoint
# ================================================================================


def test_update_dashboard_success(orguser, sample_dashboard, seed_db):
    """Test successfully updating a dashboard"""
    request = mock_request(orguser)

    payload = DashboardUpdate(
        title="Updated Dashboard",
        description="Updated Description",
    )

    response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

    assert response.id == sample_dashboard.id
    assert response.title == "Updated Dashboard"
    assert response.description == "Updated Description"


def test_update_dashboard_partial(orguser, sample_dashboard, seed_db):
    """Test partial update of a dashboard"""
    request = mock_request(orguser)

    original_title = sample_dashboard.title

    payload = DashboardUpdate(
        description="Only description updated",
    )

    response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

    assert response.title == original_title
    assert response.description == "Only description updated"


def test_update_dashboard_not_found(orguser, seed_db):
    """Test updating a non-existent dashboard"""
    request = mock_request(orguser)

    payload = DashboardUpdate(title="Updated")

    with pytest.raises(HttpError) as excinfo:
        update_dashboard(request, dashboard_id=99999, payload=payload)

    assert excinfo.value.status_code == 404


def test_update_dashboard_layout_and_components(orguser, sample_dashboard, seed_db):
    """Test updating dashboard layout and components"""
    request = mock_request(orguser)

    new_layout = [
        {"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4},
        {"i": "chart-2", "x": 6, "y": 0, "w": 6, "h": 4},
    ]

    new_components = {
        "chart-1": {"type": "chart", "chart_id": 1},
        "chart-2": {"type": "chart", "chart_id": 2},
    }

    payload = DashboardUpdate(
        layout_config=new_layout,
        components=new_components,
    )

    response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

    assert len(response.layout_config) == 2
    assert len(response.components) == 2


def test_update_dashboard_publish(orguser, sample_dashboard, seed_db):
    """Test publishing a dashboard"""
    request = mock_request(orguser)

    payload = DashboardUpdate(is_published=True)

    response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

    assert response.is_published is True
    assert response.published_at is not None


# ================================================================================
# Test delete_dashboard endpoint
# ================================================================================


def test_delete_dashboard_success(orguser, sample_dashboard, seed_db):
    """Test successfully deleting a dashboard"""
    request = mock_request(orguser)
    dashboard_id = sample_dashboard.id

    response = delete_dashboard(request, dashboard_id=dashboard_id)

    assert response.get("success") is True
    assert not Dashboard.objects.filter(id=dashboard_id).exists()


def test_delete_dashboard_not_found(orguser, seed_db):
    """Test deleting a non-existent dashboard"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_dashboard(request, dashboard_id=99999)

    assert excinfo.value.status_code == 404


def test_delete_dashboard_wrong_org(orguser, seed_db):
    """Test deleting a dashboard from another org"""
    # Create another org and dashboard
    other_org = Org.objects.create(name="Other Org", slug="other-org-del")
    other_user = User.objects.create(username="otheruser2", email="other2@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    other_dashboard = Dashboard.objects.create(
        title="Other Dashboard",
        dashboard_type="native",
        created_by=other_orguser,
        org=other_org,
    )

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_dashboard(request, dashboard_id=other_dashboard.id)

    assert excinfo.value.status_code == 404

    # Cleanup
    other_dashboard.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


# ================================================================================
# Test dashboard filter endpoints
# ================================================================================


def test_create_filter_success(orguser, sample_dashboard, seed_db):
    """Test successfully creating a dashboard filter"""
    request = mock_request(orguser)

    payload = FilterCreate(
        name="Status Filter",
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
        settings={"options": ["active", "completed"]},
        order=0,
    )

    response = create_filter(request, dashboard_id=sample_dashboard.id, payload=payload)

    assert response.name == "Status Filter"
    assert response.filter_type == "value"
    assert response.column_name == "status"

    # Cleanup
    DashboardFilter.objects.filter(id=response.id).delete()


def test_create_filter_dashboard_not_found(orguser, seed_db):
    """Test creating a filter for non-existent dashboard"""
    request = mock_request(orguser)

    payload = FilterCreate(
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
    )

    with pytest.raises(HttpError) as excinfo:
        create_filter(request, dashboard_id=99999, payload=payload)

    assert excinfo.value.status_code == 404


def test_update_filter_success(orguser, sample_dashboard, seed_db):
    """Test successfully updating a dashboard filter"""
    # Create a filter first
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
        settings={},
        order=0,
    )

    request = mock_request(orguser)

    payload = FilterUpdate(
        name="Updated Filter Name",
        settings={"options": ["new", "pending"]},
    )

    response = update_filter(
        request,
        dashboard_id=sample_dashboard.id,
        filter_id=filter_obj.id,
        payload=payload,
    )

    assert response.name == "Updated Filter Name"
    assert response.settings["options"] == ["new", "pending"]

    # Cleanup
    filter_obj.delete()


def test_update_filter_not_found(orguser, sample_dashboard, seed_db):
    """Test updating a non-existent filter"""
    request = mock_request(orguser)

    payload = FilterUpdate(name="Updated")

    with pytest.raises(HttpError) as excinfo:
        update_filter(
            request,
            dashboard_id=sample_dashboard.id,
            filter_id=99999,
            payload=payload,
        )

    assert excinfo.value.status_code == 404


def test_delete_filter_success(orguser, sample_dashboard, seed_db):
    """Test successfully deleting a dashboard filter"""
    # Create a filter first
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
        settings={},
        order=0,
    )

    request = mock_request(orguser)
    filter_id = filter_obj.id

    response = delete_filter(request, dashboard_id=sample_dashboard.id, filter_id=filter_id)

    assert response.get("success") is True
    assert not DashboardFilter.objects.filter(id=filter_id).exists()


def test_delete_filter_not_found(orguser, sample_dashboard, seed_db):
    """Test deleting a non-existent filter"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_filter(request, dashboard_id=sample_dashboard.id, filter_id=99999)

    assert excinfo.value.status_code == 404


# ================================================================================
# Test seed data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
