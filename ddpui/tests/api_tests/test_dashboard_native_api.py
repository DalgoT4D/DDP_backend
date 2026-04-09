"""API Tests for Native Dashboard endpoints

Tests the full request -> response cycle for all dashboard_native_api endpoints.
"""

import os
import django
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
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardLock
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.dashboard_native_api import (
    list_dashboards,
    get_dashboard,
    create_dashboard,
    update_dashboard,
    delete_dashboard,
    duplicate_dashboard,
    lock_dashboard,
    refresh_dashboard_lock,
    unlock_dashboard,
    create_filter,
    get_filter,
    update_filter,
    delete_filter,
    get_filter_options,
    toggle_dashboard_sharing,
    get_dashboard_sharing_status,
    set_personal_landing_dashboard,
    remove_personal_landing_dashboard,
    set_org_default_dashboard,
    remove_org_default_dashboard,
    resolve_user_landing_page,
)
from ddpui.schemas.dashboard_schema import (
    DashboardCreate,
    DashboardUpdate,
    DashboardShareToggle,
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
        username="dashapiuser", email="dashapiuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Dashboard API Test Org",
        slug="dash-api-test-org",  # max_length=20
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
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


@pytest.fixture
def sample_filter(sample_dashboard):
    """A sample filter for testing"""
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


# ================================================================================
# Test list_dashboards endpoint
# ================================================================================


class TestListDashboards:
    """Tests for list_dashboards endpoint"""

    def test_list_dashboards_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully listing dashboards"""
        request = mock_request(orguser)

        response = list_dashboards(request)

        assert len(response) == 1
        assert response[0].title == "Test Dashboard"
        assert response[0].dashboard_type == "native"

    def test_list_dashboards_with_search(self, orguser, sample_dashboard, seed_db):
        """Test listing with search filter"""
        request = mock_request(orguser)

        response = list_dashboards(request, search="Test")

        assert len(response) == 1
        assert response[0].title == "Test Dashboard"

    def test_list_dashboards_search_no_results(self, orguser, sample_dashboard, seed_db):
        """Test search with no matching results"""
        request = mock_request(orguser)

        response = list_dashboards(request, search="Nonexistent")

        assert len(response) == 0

    def test_list_dashboards_filter_by_type(self, orguser, sample_dashboard, seed_db):
        """Test filtering by dashboard type"""
        request = mock_request(orguser)

        response = list_dashboards(request, dashboard_type="native")

        assert len(response) == 1
        assert response[0].dashboard_type == "native"

    def test_list_dashboards_filter_by_published(self, orguser, org, seed_db):
        """Test filtering by published status"""
        # Create published and unpublished dashboards
        Dashboard.objects.create(
            title="Published Dashboard",
            dashboard_type="native",
            is_published=True,
            created_by=orguser,
            org=org,
        )
        Dashboard.objects.create(
            title="Unpublished Dashboard",
            dashboard_type="native",
            is_published=False,
            created_by=orguser,
            org=org,
        )

        request = mock_request(orguser)

        published = list_dashboards(request, is_published=True)
        assert len(published) == 1
        assert published[0].is_published is True

        unpublished = list_dashboards(request, is_published=False)
        assert len(unpublished) == 1
        assert unpublished[0].is_published is False

    def test_list_dashboards_empty_org(self, orguser, seed_db):
        """Test listing when org has no dashboards"""
        request = mock_request(orguser)

        response = list_dashboards(request)

        assert len(response) == 0


# ================================================================================
# Test get_dashboard endpoint
# ================================================================================


class TestGetDashboard:
    """Tests for get_dashboard endpoint"""

    def test_get_dashboard_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully getting a dashboard"""
        request = mock_request(orguser)

        response = get_dashboard(request, dashboard_id=sample_dashboard.id)

        assert response.id == sample_dashboard.id
        assert response.title == "Test Dashboard"
        assert response.dashboard_type == "native"

    def test_get_dashboard_not_found(self, orguser, seed_db):
        """Test getting non-existent dashboard returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            get_dashboard(request, dashboard_id=99999)

        assert excinfo.value.status_code == 404

    def test_get_dashboard_wrong_org(self, orguser, seed_db):
        """Test getting dashboard from another org returns 404"""
        # Create another org and dashboard
        other_org = Org.objects.create(name="Other Org", slug="other-org-dash")
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


class TestCreateDashboard:
    """Tests for create_dashboard endpoint"""

    def test_create_dashboard_success(self, orguser, seed_db):
        """Test successfully creating a dashboard"""
        request = mock_request(orguser)

        payload = DashboardCreate(
            title="New Dashboard",
            description="New Description",
            grid_columns=24,
        )

        response = create_dashboard(request, payload)

        assert response.title == "New Dashboard"
        assert response.description == "New Description"
        assert response.grid_columns == 24
        assert response.dashboard_type == "native"

        # Cleanup
        Dashboard.objects.filter(id=response.id).delete()

    def test_create_dashboard_minimal(self, orguser, seed_db):
        """Test creating dashboard with minimal config"""
        request = mock_request(orguser)

        payload = DashboardCreate(title="Minimal Dashboard")

        response = create_dashboard(request, payload)

        assert response.title == "Minimal Dashboard"
        assert response.description is None
        assert response.grid_columns == 12  # Default

        # Cleanup
        Dashboard.objects.filter(id=response.id).delete()


# ================================================================================
# Test update_dashboard endpoint
# ================================================================================


class TestUpdateDashboard:
    """Tests for update_dashboard endpoint"""

    def test_update_dashboard_success(self, orguser, sample_dashboard, seed_db):
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

    def test_update_dashboard_partial(self, orguser, sample_dashboard, seed_db):
        """Test partial update"""
        request = mock_request(orguser)
        original_title = sample_dashboard.title

        payload = DashboardUpdate(description="Only description updated")

        response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

        assert response.title == original_title
        assert response.description == "Only description updated"

    def test_update_dashboard_not_found(self, orguser, seed_db):
        """Test updating non-existent dashboard returns 404"""
        request = mock_request(orguser)

        payload = DashboardUpdate(title="Updated")

        with pytest.raises(HttpError) as excinfo:
            update_dashboard(request, dashboard_id=99999, payload=payload)

        assert excinfo.value.status_code == 404

    def test_update_dashboard_layout_and_components(self, orguser, sample_dashboard, seed_db):
        """Test updating layout and components"""
        request = mock_request(orguser)

        new_layout = [
            {"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4},
            {"i": "chart-2", "x": 6, "y": 0, "w": 6, "h": 4},
        ]
        new_components = {
            "chart-1": {"type": "chart", "config": {"chartId": 1}},
            "chart-2": {"type": "chart", "config": {"chartId": 2}},
        }

        payload = DashboardUpdate(
            layout_config=new_layout,
            components=new_components,
        )

        response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

        assert len(response.layout_config) == 2
        assert len(response.components) == 2

    def test_update_dashboard_publish(self, orguser, sample_dashboard, seed_db):
        """Test publishing a dashboard"""
        request = mock_request(orguser)

        payload = DashboardUpdate(is_published=True)

        response = update_dashboard(request, dashboard_id=sample_dashboard.id, payload=payload)

        assert response.is_published is True
        assert response.published_at is not None


# ================================================================================
# Test delete_dashboard endpoint
# ================================================================================


class TestDeleteDashboard:
    """Tests for delete_dashboard endpoint"""

    def test_delete_dashboard_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully deleting a dashboard"""
        # Create another dashboard so we're not deleting the last one
        Dashboard.objects.create(
            title="Another Dashboard",
            dashboard_type="native",
            grid_columns=12,
            layout_config=[],
            components={},
            created_by=orguser,
            org=orguser.org,
        )

        dashboard_id = sample_dashboard.id
        request = mock_request(orguser)

        response = delete_dashboard(request, dashboard_id=dashboard_id)

        assert response.get("success") is True
        assert not Dashboard.objects.filter(id=dashboard_id).exists()

    def test_delete_dashboard_not_found(self, orguser, seed_db):
        """Test deleting non-existent dashboard returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            delete_dashboard(request, dashboard_id=99999)

        assert excinfo.value.status_code == 404

    def test_delete_dashboard_wrong_org(self, orguser, seed_db):
        """Test deleting dashboard from another org returns 404"""
        # Create another org and dashboard
        other_org = Org.objects.create(name="Other Org", slug="other-org-del-dash")
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
# Test create_filter endpoint
# ================================================================================


class TestCreateFilter:
    """Tests for create_filter endpoint"""

    def test_create_filter_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully creating a filter"""
        request = mock_request(orguser)

        payload = FilterCreate(
            name="Category Filter",
            filter_type="value",
            schema_name="public",
            table_name="products",
            column_name="category",
            settings={"multiSelect": True},
            order=1,
        )

        response = create_filter(request, dashboard_id=sample_dashboard.id, payload=payload)

        assert response.name == "Category Filter"
        assert response.filter_type == "value"
        assert response.column_name == "category"

        # Cleanup
        DashboardFilter.objects.filter(id=response.id).delete()

    def test_create_filter_dashboard_not_found(self, orguser, seed_db):
        """Test creating filter for non-existent dashboard returns 404"""
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


# ================================================================================
# Test update_filter endpoint
# ================================================================================


class TestUpdateFilter:
    """Tests for update_filter endpoint"""

    def test_update_filter_success(self, orguser, sample_dashboard, sample_filter, seed_db):
        """Test successfully updating a filter"""
        request = mock_request(orguser)

        payload = FilterUpdate(
            name="Updated Filter Name",
            settings={"newSetting": True},
        )

        response = update_filter(
            request,
            dashboard_id=sample_dashboard.id,
            filter_id=sample_filter.id,
            payload=payload,
        )

        assert response.name == "Updated Filter Name"
        assert response.settings["newSetting"] is True

    def test_update_filter_not_found(self, orguser, sample_dashboard, seed_db):
        """Test updating non-existent filter returns 404"""
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


# ================================================================================
# Test delete_filter endpoint
# ================================================================================


class TestDeleteFilter:
    """Tests for delete_filter endpoint"""

    def test_delete_filter_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully deleting a filter"""
        # Create filter to delete
        filter_obj = DashboardFilter.objects.create(
            dashboard=sample_dashboard,
            name="Filter to Delete",
            filter_type="value",
            schema_name="public",
            table_name="orders",
            column_name="status",
        )
        filter_id = filter_obj.id

        request = mock_request(orguser)

        response = delete_filter(request, dashboard_id=sample_dashboard.id, filter_id=filter_id)

        assert response.get("success") is True
        assert not DashboardFilter.objects.filter(id=filter_id).exists()

    def test_delete_filter_not_found(self, orguser, sample_dashboard, seed_db):
        """Test deleting non-existent filter returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            delete_filter(request, dashboard_id=sample_dashboard.id, filter_id=99999)

        assert excinfo.value.status_code == 404


# ================================================================================
# Test duplicate_dashboard endpoint
# ================================================================================


class TestDuplicateDashboard:
    """Tests for duplicate_dashboard endpoint"""

    def test_duplicate_dashboard_success(self, orguser, sample_dashboard, sample_filter, seed_db):
        """Test successfully duplicating a dashboard with filters"""
        request = mock_request(orguser)

        response = duplicate_dashboard(request, dashboard_id=sample_dashboard.id)

        assert response.title == f"Copy of {sample_dashboard.title}"
        assert response.id != sample_dashboard.id
        assert response.dashboard_type == sample_dashboard.dashboard_type

        # Verify filter was duplicated
        new_dashboard = Dashboard.objects.get(id=response.id)
        assert new_dashboard.filters.count() == 1
        new_filter = new_dashboard.filters.first()
        assert new_filter.column_name == sample_filter.column_name
        assert new_filter.id != sample_filter.id

        # Cleanup
        new_dashboard.delete()

    def test_duplicate_dashboard_not_found(self, orguser, seed_db):
        """Test duplicating non-existent dashboard returns 404"""
        request = mock_request(orguser)

        with pytest.raises(HttpError) as excinfo:
            duplicate_dashboard(request, dashboard_id=99999)

        assert excinfo.value.status_code == 404

    def test_duplicate_dashboard_with_filter_layout_references(self, orguser, org, seed_db):
        """Test that filter references in layout/components are updated"""
        # Create dashboard with filter-referencing layout
        dashboard = Dashboard.objects.create(
            title="Dashboard with filters",
            dashboard_type="native",
            layout_config=[
                {"i": "filter-100", "x": 0, "y": 0, "w": 3, "h": 2},
                {"i": "chart-1", "x": 3, "y": 0, "w": 9, "h": 4},
            ],
            components={
                "filter-100": {"type": "filter", "config": {"filterId": 100}},
                "chart-1": {"type": "chart", "config": {"chartId": 1}},
            },
            created_by=orguser,
            org=org,
        )
        filter_obj = DashboardFilter.objects.create(
            id=100,
            dashboard=dashboard,
            name="Test filter",
            filter_type="value",
            schema_name="public",
            table_name="orders",
            column_name="status",
        )

        request = mock_request(orguser)
        response = duplicate_dashboard(request, dashboard_id=dashboard.id)

        new_dashboard = Dashboard.objects.get(id=response.id)
        new_filter = new_dashboard.filters.first()

        # Check that filter references were updated
        filter_layout_items = [
            item for item in new_dashboard.layout_config if item["i"].startswith("filter-")
        ]
        assert len(filter_layout_items) == 1
        assert filter_layout_items[0]["i"] == f"filter-{new_filter.id}"

        # Check component references
        filter_component_key = f"filter-{new_filter.id}"
        assert filter_component_key in new_dashboard.components

        # Cleanup
        new_dashboard.delete()
        filter_obj.delete()
        dashboard.delete()


# ================================================================================
# Test lock/unlock dashboard endpoints
# ================================================================================


class TestDashboardLocking:
    """Tests for lock, refresh_lock, and unlock endpoints"""

    def test_lock_dashboard_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully locking a dashboard"""
        request = mock_request(orguser)
        response = lock_dashboard(request, dashboard_id=sample_dashboard.id)

        assert response.lock_token is not None
        assert response.expires_at is not None
        assert response.locked_by == orguser.user.email

        # Cleanup
        DashboardLock.objects.filter(dashboard=sample_dashboard).delete()

    def test_lock_dashboard_not_found(self, orguser, seed_db):
        """Test locking non-existent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            lock_dashboard(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404

    def test_lock_already_locked(self, orguser, org, seed_db):
        """Test locking already-locked dashboard raises 423"""
        dashboard = Dashboard.objects.create(
            title="Lockable Dashboard",
            dashboard_type="native",
            created_by=orguser,
            org=org,
        )
        request = mock_request(orguser)
        # First lock
        lock_dashboard(request, dashboard_id=dashboard.id)

        # Create another user to attempt lock
        other_user = User.objects.create(username="locker2", email="locker2@test.com")
        other_orguser = OrgUser.objects.create(
            user=other_user,
            org=org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        request2 = mock_request(other_orguser)
        with pytest.raises(HttpError) as excinfo:
            lock_dashboard(request2, dashboard_id=dashboard.id)
        assert excinfo.value.status_code == 423

        # Cleanup
        DashboardLock.objects.filter(dashboard=dashboard).delete()
        other_orguser.delete()
        other_user.delete()
        dashboard.delete()

    def test_unlock_dashboard_success(self, orguser, sample_dashboard, seed_db):
        """Test successfully unlocking a dashboard"""
        request = mock_request(orguser)
        lock_dashboard(request, dashboard_id=sample_dashboard.id)

        response = unlock_dashboard(request, dashboard_id=sample_dashboard.id)
        assert response["success"] is True

    def test_unlock_dashboard_not_found(self, orguser, seed_db):
        """Test unlocking non-existent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            unlock_dashboard(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404

    def test_refresh_lock_not_found(self, orguser, seed_db):
        """Test refreshing lock on non-existent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            refresh_dashboard_lock(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404


# ================================================================================
# Test get_filter endpoint
# ================================================================================


class TestGetFilter:
    """Tests for get_filter endpoint"""

    def test_get_filter_success(self, orguser, sample_dashboard, sample_filter, seed_db):
        """Test successfully getting a filter"""
        request = mock_request(orguser)
        response = get_filter(
            request,
            dashboard_id=sample_dashboard.id,
            filter_id=sample_filter.id,
        )
        assert response.id == sample_filter.id
        assert response.name == "Status Filter"
        assert response.filter_type == "value"

    def test_get_filter_dashboard_not_found(self, orguser, seed_db):
        """Test getting filter from non-existent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_filter(request, dashboard_id=99999, filter_id=1)
        assert excinfo.value.status_code == 404

    def test_get_filter_not_found(self, orguser, sample_dashboard, seed_db):
        """Test getting non-existent filter"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_filter(request, dashboard_id=sample_dashboard.id, filter_id=99999)
        assert excinfo.value.status_code == 404


# ================================================================================
# Test get_filter_options endpoint
# ================================================================================


class TestGetFilterOptions:
    """Tests for get_filter_options endpoint"""

    def test_no_warehouse(self, orguser, seed_db):
        """Test filter options with no warehouse"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_filter_options(
                request,
                schema_name="public",
                table_name="orders",
                column_name="status",
            )
        assert excinfo.value.status_code == 400

    @patch("ddpui.services.dashboard_service.DashboardService.generate_filter_options")
    def test_success(self, mock_gen_options, orguser, seed_db):
        """Test successful filter options retrieval"""
        # Create a real OrgWarehouse for this test
        wh = OrgWarehouse.objects.create(
            wtype="postgres", credentials="test-creds", org=orguser.org
        )

        mock_gen_options.return_value = [
            {"label": "Active", "value": "active", "count": 10},
        ]

        request = mock_request(orguser)
        response = get_filter_options(
            request,
            schema_name="public",
            table_name="orders",
            column_name="status",
        )
        assert response.total_count == 1

        wh.delete()


# ================================================================================
# Test sharing endpoints
# ================================================================================


class TestDashboardSharing:
    """Tests for toggle_dashboard_sharing and get_dashboard_sharing_status"""

    def test_toggle_sharing_not_found(self, orguser, seed_db):
        """Test toggling sharing on non-existent dashboard"""
        request = mock_request(orguser)
        payload = DashboardShareToggle(is_public=True)
        with pytest.raises(HttpError) as excinfo:
            toggle_dashboard_sharing(request, dashboard_id=99999, payload=payload)
        assert excinfo.value.status_code == 404

    def test_toggle_sharing_not_creator(self, orguser, org, seed_db):
        """Test that non-creator cannot toggle sharing"""
        other_user = User.objects.create(username="sharer2", email="sharer2@test.com")
        other_orguser = OrgUser.objects.create(
            user=other_user,
            org=org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        dashboard = Dashboard.objects.create(
            title="Share Dashboard",
            dashboard_type="native",
            created_by=other_orguser,
            org=org,
        )

        request = mock_request(orguser)
        payload = DashboardShareToggle(is_public=True)
        with pytest.raises(HttpError) as excinfo:
            toggle_dashboard_sharing(request, dashboard_id=dashboard.id, payload=payload)
        assert excinfo.value.status_code == 403

        dashboard.delete()
        other_orguser.delete()
        other_user.delete()

    def test_toggle_sharing_enable(self, orguser, sample_dashboard, seed_db):
        """Test enabling public sharing"""
        request = mock_request(orguser)
        payload = DashboardShareToggle(is_public=True)
        response = toggle_dashboard_sharing(
            request, dashboard_id=sample_dashboard.id, payload=payload
        )
        assert response.is_public is True
        assert response.public_share_token is not None

    def test_toggle_sharing_disable(self, orguser, sample_dashboard, seed_db):
        """Test disabling public sharing"""
        # First enable
        request = mock_request(orguser)
        payload_on = DashboardShareToggle(is_public=True)
        toggle_dashboard_sharing(request, dashboard_id=sample_dashboard.id, payload=payload_on)

        # Then disable
        payload_off = DashboardShareToggle(is_public=False)
        response = toggle_dashboard_sharing(
            request, dashboard_id=sample_dashboard.id, payload=payload_off
        )
        assert response.is_public is False

    def test_get_sharing_status_not_found(self, orguser, seed_db):
        """Test getting sharing status on non-existent dashboard"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_dashboard_sharing_status(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404

    def test_get_sharing_status_not_creator(self, orguser, org, seed_db):
        """Test non-creator cannot get sharing status"""
        other_user = User.objects.create(username="viewer3", email="viewer3@test.com")
        other_orguser = OrgUser.objects.create(
            user=other_user,
            org=org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        dashboard = Dashboard.objects.create(
            title="StatusDash",
            dashboard_type="native",
            created_by=other_orguser,
            org=org,
        )

        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            get_dashboard_sharing_status(request, dashboard_id=dashboard.id)
        assert excinfo.value.status_code == 403

        dashboard.delete()
        other_orguser.delete()
        other_user.delete()

    def test_get_sharing_status_success(self, orguser, sample_dashboard, seed_db):
        """Test getting sharing status for own dashboard"""
        request = mock_request(orguser)
        response = get_dashboard_sharing_status(request, dashboard_id=sample_dashboard.id)
        assert response.is_public is False
        assert response.public_access_count == 0


# ================================================================================
# Test landing page endpoints
# ================================================================================


class TestLandingPage:
    """Tests for landing page management endpoints"""

    def test_set_personal_landing_dashboard(self, orguser, sample_dashboard, seed_db):
        """Test setting personal landing dashboard"""
        request = mock_request(orguser)
        response = set_personal_landing_dashboard(request, dashboard_id=sample_dashboard.id)
        assert response.success is True
        orguser.refresh_from_db()
        assert orguser.landing_dashboard_id == sample_dashboard.id

    def test_set_personal_landing_dashboard_not_found(self, orguser, seed_db):
        """Test setting non-existent dashboard as landing"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            set_personal_landing_dashboard(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404

    def test_remove_personal_landing_dashboard(self, orguser, sample_dashboard, seed_db):
        """Test removing personal landing dashboard"""
        orguser.landing_dashboard = sample_dashboard
        orguser.save()

        request = mock_request(orguser)
        response = remove_personal_landing_dashboard(request)
        assert response.success is True
        orguser.refresh_from_db()
        assert orguser.landing_dashboard is None

    def test_remove_personal_landing_when_none(self, orguser, seed_db):
        """Test removing when no landing dashboard is set"""
        request = mock_request(orguser)
        response = remove_personal_landing_dashboard(request)
        assert response.success is True
        assert "No personal landing page was set" in response.message

    def test_set_org_default_dashboard(self, orguser, sample_dashboard, seed_db):
        """Test setting org default dashboard"""
        request = mock_request(orguser)
        response = set_org_default_dashboard(request, dashboard_id=sample_dashboard.id)
        assert response.success is True
        sample_dashboard.refresh_from_db()
        assert sample_dashboard.is_org_default is True

    def test_set_org_default_dashboard_not_found(self, orguser, seed_db):
        """Test setting non-existent dashboard as org default"""
        request = mock_request(orguser)
        with pytest.raises(HttpError) as excinfo:
            set_org_default_dashboard(request, dashboard_id=99999)
        assert excinfo.value.status_code == 404

    def test_remove_org_default_dashboard(self, orguser, org, sample_dashboard, seed_db):
        """Test removing org default dashboard"""
        sample_dashboard.is_org_default = True
        sample_dashboard.save()

        request = mock_request(orguser)
        response = remove_org_default_dashboard(request)
        assert response.success is True
        sample_dashboard.refresh_from_db()
        assert sample_dashboard.is_org_default is False

    def test_remove_org_default_when_none(self, orguser, seed_db):
        """Test removing when no org default is set"""
        request = mock_request(orguser)
        response = remove_org_default_dashboard(request)
        assert response.success is True
        assert "No organization default" in response.message

    def test_resolve_landing_page_personal(self, orguser, sample_dashboard, seed_db):
        """Test resolving landing page returns personal preference"""
        orguser.landing_dashboard = sample_dashboard
        orguser.save()

        request = mock_request(orguser)
        response = resolve_user_landing_page(request)
        assert response["dashboard_id"] == sample_dashboard.id
        assert response["source"] == "personal"

    def test_resolve_landing_page_org_default(self, orguser, org, seed_db):
        """Test resolving landing page returns org default"""
        dashboard = Dashboard.objects.create(
            title="Org Default",
            dashboard_type="native",
            created_by=orguser,
            org=org,
            is_org_default=True,
        )

        request = mock_request(orguser)
        response = resolve_user_landing_page(request)
        assert response["dashboard_id"] == dashboard.id
        assert response["source"] == "org_default"

        dashboard.delete()

    def test_resolve_landing_page_none(self, orguser, seed_db):
        """Test resolving landing page when nothing is set"""
        request = mock_request(orguser)
        response = resolve_user_landing_page(request)
        assert response["dashboard_id"] is None
        assert response["source"] == "none"


# ================================================================================
# Test seed data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
