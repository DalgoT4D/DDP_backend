"""Test cases for Dashboard models"""

import os
import django
import pytest
import uuid
from datetime import datetime, timedelta

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.db import IntegrityError
from django.utils import timezone
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import (
    Dashboard,
    DashboardFilter,
    DashboardLock,
    DashboardType,
    DashboardComponentType,
    DashboardFilterType,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    """A django User object"""
    user = User.objects.create(
        username="dashmodeluser", email="dashmodeluser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    """An Org object"""
    org = Org.objects.create(
        name="Test Org",
        slug="test-org-dash",
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
    # Try to delete if it still exists
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


# ================================================================================
# Test Dashboard Enums
# ================================================================================


def test_dashboard_type_enum():
    """Test DashboardType enum values"""
    assert DashboardType.NATIVE.value == "native"
    assert DashboardType.SUPERSET.value == "superset"

    choices = DashboardType.choices()
    assert ("native", "NATIVE") in choices
    assert ("superset", "SUPERSET") in choices


def test_dashboard_component_type_enum():
    """Test DashboardComponentType enum values"""
    assert DashboardComponentType.CHART.value == "chart"
    assert DashboardComponentType.TEXT.value == "text"
    assert DashboardComponentType.HEADING.value == "heading"

    choices = DashboardComponentType.choices()
    assert ("chart", "CHART") in choices
    assert ("text", "TEXT") in choices


def test_dashboard_filter_type_enum():
    """Test DashboardFilterType enum values"""
    assert DashboardFilterType.VALUE.value == "value"
    assert DashboardFilterType.NUMERICAL.value == "numerical"
    assert DashboardFilterType.DATETIME.value == "datetime"

    choices = DashboardFilterType.choices()
    assert ("value", "VALUE") in choices
    assert ("numerical", "NUMERICAL") in choices


# ================================================================================
# Test Dashboard Model Creation
# ================================================================================


def test_dashboard_create_success(orguser, org, seed_db):
    """Test creating a dashboard with valid data"""
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test Description",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={"chart-1": {"type": "chart", "chart_id": 1}},
        created_by=orguser,
        org=org,
    )

    assert dashboard.id is not None
    assert dashboard.title == "Test Dashboard"
    assert dashboard.dashboard_type == "native"
    assert dashboard.grid_columns == 12
    assert dashboard.is_published is False
    assert dashboard.is_public is False
    assert dashboard.created_at is not None

    dashboard.delete()


def test_dashboard_create_with_defaults(orguser, org, seed_db):
    """Test creating a dashboard with default values"""
    dashboard = Dashboard.objects.create(
        title="Default Dashboard",
        created_by=orguser,
        org=org,
    )

    assert dashboard.dashboard_type == "native"
    assert dashboard.grid_columns == 12
    assert dashboard.target_screen_size == "desktop"
    assert dashboard.filter_layout == "vertical"
    assert dashboard.is_published is False
    assert dashboard.is_public is False
    assert dashboard.public_access_count == 0
    assert dashboard.layout_config == []
    assert dashboard.components == {}

    dashboard.delete()


def test_dashboard_create_all_dashboard_types(orguser, org, seed_db):
    """Test creating dashboards with all valid types"""
    for dash_type, _ in DashboardType.choices():
        dashboard = Dashboard.objects.create(
            title=f"{dash_type} Dashboard",
            dashboard_type=dash_type,
            created_by=orguser,
            org=org,
        )

        assert dashboard.dashboard_type == dash_type
        dashboard.delete()


def test_dashboard_create_all_screen_sizes(orguser, org, seed_db):
    """Test creating dashboards with all target screen sizes"""
    screen_sizes = ["desktop", "tablet", "mobile", "a4"]

    for size in screen_sizes:
        dashboard = Dashboard.objects.create(
            title=f"{size} Dashboard",
            target_screen_size=size,
            created_by=orguser,
            org=org,
        )

        assert dashboard.target_screen_size == size
        dashboard.delete()


def test_dashboard_create_all_filter_layouts(orguser, org, seed_db):
    """Test creating dashboards with all filter layouts"""
    layouts = ["vertical", "horizontal"]

    for layout in layouts:
        dashboard = Dashboard.objects.create(
            title=f"{layout} Filter Dashboard",
            filter_layout=layout,
            created_by=orguser,
            org=org,
        )

        assert dashboard.filter_layout == layout
        dashboard.delete()


# ================================================================================
# Test Dashboard String Representation
# ================================================================================


def test_dashboard_str_representation(orguser, org, seed_db):
    """Test the __str__ method of Dashboard model"""
    dashboard = Dashboard.objects.create(
        title="My Dashboard",
        dashboard_type="native",
        created_by=orguser,
        org=org,
    )

    assert str(dashboard) == "My Dashboard (native)"
    dashboard.delete()


# ================================================================================
# Test Dashboard to_json Method
# ================================================================================


def test_dashboard_to_json(orguser, org, seed_db):
    """Test the to_json method of Dashboard model"""
    dashboard = Dashboard.objects.create(
        title="JSON Dashboard",
        description="JSON Description",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "1", "x": 0, "y": 0}],
        components={"1": {"type": "chart"}},
        created_by=orguser,
        org=org,
    )

    json_data = dashboard.to_json()

    assert json_data["id"] == dashboard.id
    assert json_data["title"] == "JSON Dashboard"
    assert json_data["description"] == "JSON Description"
    assert json_data["dashboard_type"] == "native"
    assert json_data["grid_columns"] == 12
    assert json_data["layout_config"] == [{"i": "1", "x": 0, "y": 0}]
    assert json_data["components"] == {"1": {"type": "chart"}}
    assert json_data["is_published"] is False
    assert json_data["created_by"] == "dashmodeluser@test.com"
    assert json_data["org_id"] == org.id

    dashboard.delete()


def test_dashboard_to_json_with_published(orguser, org, seed_db):
    """Test to_json with published dashboard"""
    published_at = timezone.now()
    dashboard = Dashboard.objects.create(
        title="Published Dashboard",
        is_published=True,
        published_at=published_at,
        created_by=orguser,
        org=org,
    )

    json_data = dashboard.to_json()

    assert json_data["is_published"] is True
    assert json_data["published_at"] == published_at.isoformat()

    dashboard.delete()


# ================================================================================
# Test Dashboard Relationships
# ================================================================================


def test_dashboard_org_relationship(orguser, org, seed_db):
    """Test the relationship between Dashboard and Org"""
    dashboard = Dashboard.objects.create(
        title="Org Related Dashboard",
        created_by=orguser,
        org=org,
    )

    assert dashboard.org == org
    assert dashboard.org.name == "Test Org"

    # Test reverse relationship
    org_dashboards = Dashboard.objects.filter(org=org)
    assert dashboard in org_dashboards

    dashboard.delete()


def test_dashboard_created_by_relationship(orguser, org, seed_db):
    """Test the relationship between Dashboard and OrgUser (created_by)"""
    dashboard = Dashboard.objects.create(
        title="User Related Dashboard",
        created_by=orguser,
        org=org,
    )

    assert dashboard.created_by == orguser
    assert dashboard.created_by.user.email == "dashmodeluser@test.com"

    dashboard.delete()


# ================================================================================
# Test Dashboard Publishing
# ================================================================================


def test_dashboard_publish(orguser, org, seed_db):
    """Test publishing a dashboard"""
    dashboard = Dashboard.objects.create(
        title="Unpublished Dashboard",
        created_by=orguser,
        org=org,
    )

    assert dashboard.is_published is False
    assert dashboard.published_at is None

    dashboard.is_published = True
    dashboard.published_at = timezone.now()
    dashboard.save()

    dashboard.refresh_from_db()
    assert dashboard.is_published is True
    assert dashboard.published_at is not None

    dashboard.delete()


# ================================================================================
# Test Dashboard Public Sharing
# ================================================================================


def test_dashboard_public_sharing(orguser, org, seed_db):
    """Test public sharing configuration"""
    share_token = str(uuid.uuid4())

    dashboard = Dashboard.objects.create(
        title="Public Dashboard",
        is_public=True,
        public_share_token=share_token,
        public_shared_at=timezone.now(),
        created_by=orguser,
        org=org,
    )

    assert dashboard.is_public is True
    assert dashboard.public_share_token == share_token
    assert dashboard.public_shared_at is not None

    dashboard.delete()


def test_dashboard_public_access_count(orguser, org, seed_db):
    """Test incrementing public access count"""
    dashboard = Dashboard.objects.create(
        title="Access Count Dashboard",
        is_public=True,
        public_access_count=0,
        created_by=orguser,
        org=org,
    )

    # Simulate access
    dashboard.public_access_count += 1
    dashboard.last_public_accessed = timezone.now()
    dashboard.save()

    dashboard.refresh_from_db()
    assert dashboard.public_access_count == 1
    assert dashboard.last_public_accessed is not None

    dashboard.delete()


# ================================================================================
# Test Dashboard Org Default Constraint
# ================================================================================


@pytest.mark.django_db(transaction=True)
def test_dashboard_org_default_unique_constraint(orguser, org, seed_db):
    """Test that only one dashboard can be org default per org"""
    dashboard1 = Dashboard.objects.create(
        title="Default Dashboard",
        is_org_default=True,
        created_by=orguser,
        org=org,
    )

    # Try to create another default dashboard for same org
    with pytest.raises(IntegrityError):
        Dashboard.objects.create(
            title="Another Default Dashboard",
            is_org_default=True,
            created_by=orguser,
            org=org,
        )

    dashboard1.delete()


def test_dashboard_org_default_different_orgs(authuser, seed_db):
    """Test that different orgs can each have a default dashboard"""
    org1 = Org.objects.create(name="Org 1", slug="org-1-def")
    org2 = Org.objects.create(name="Org 2", slug="org-2-def")

    orguser1 = OrgUser.objects.create(
        user=authuser,
        org=org1,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    orguser2 = OrgUser.objects.create(
        user=authuser,
        org=org2,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )

    dashboard1 = Dashboard.objects.create(
        title="Org 1 Default",
        is_org_default=True,
        created_by=orguser1,
        org=org1,
    )

    dashboard2 = Dashboard.objects.create(
        title="Org 2 Default",
        is_org_default=True,
        created_by=orguser2,
        org=org2,
    )

    assert dashboard1.is_org_default is True
    assert dashboard2.is_org_default is True

    # Cleanup
    dashboard1.delete()
    dashboard2.delete()
    orguser1.delete()
    orguser2.delete()
    org1.delete()
    org2.delete()


# ================================================================================
# Test DashboardFilter Model
# ================================================================================


def test_dashboard_filter_create(sample_dashboard, seed_db):
    """Test creating a dashboard filter"""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="Status Filter",
        filter_type="value",
        schema_name="public",
        table_name="orders",
        column_name="status",
        settings={"options": ["active", "completed"]},
        order=0,
    )

    assert filter_obj.id is not None
    assert filter_obj.name == "Status Filter"
    assert filter_obj.filter_type == "value"
    assert filter_obj.dashboard == sample_dashboard

    filter_obj.delete()


def test_dashboard_filter_all_types(sample_dashboard, seed_db):
    """Test creating filters with all filter types"""
    for filter_type, _ in DashboardFilterType.choices():
        filter_obj = DashboardFilter.objects.create(
            dashboard=sample_dashboard,
            filter_type=filter_type,
            schema_name="public",
            table_name="test",
            column_name="test_col",
            settings={},
            order=0,
        )

        assert filter_obj.filter_type == filter_type
        filter_obj.delete()


def test_dashboard_filter_str_representation(sample_dashboard, seed_db):
    """Test the __str__ method of DashboardFilter"""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="My Filter",
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="col",
        settings={},
        order=0,
    )

    expected = f"{sample_dashboard.title} - My Filter (value)"
    assert str(filter_obj) == expected

    filter_obj.delete()


def test_dashboard_filter_str_without_name(sample_dashboard, seed_db):
    """Test __str__ when filter has no name (uses column_name)"""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="my_column",
        settings={},
        order=0,
    )

    expected = f"{sample_dashboard.title} - my_column (value)"
    assert str(filter_obj) == expected

    filter_obj.delete()


def test_dashboard_filter_to_json(sample_dashboard, seed_db):
    """Test the to_json method of DashboardFilter"""
    filter_obj = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        name="JSON Filter",
        filter_type="numerical",
        schema_name="public",
        table_name="metrics",
        column_name="amount",
        settings={"min": 0, "max": 100},
        order=1,
    )

    json_data = filter_obj.to_json()

    assert json_data["id"] == filter_obj.id
    assert json_data["dashboard_id"] == sample_dashboard.id
    assert json_data["name"] == "JSON Filter"
    assert json_data["filter_type"] == "numerical"
    assert json_data["settings"] == {"min": 0, "max": 100}
    assert json_data["order"] == 1

    filter_obj.delete()


def test_dashboard_filter_cascade_delete(orguser, org, seed_db):
    """Test that filters are deleted when dashboard is deleted"""
    dashboard = Dashboard.objects.create(
        title="Cascade Test Dashboard",
        created_by=orguser,
        org=org,
    )

    filter_obj = DashboardFilter.objects.create(
        dashboard=dashboard,
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="col",
        settings={},
        order=0,
    )

    filter_id = filter_obj.id
    dashboard.delete()

    assert not DashboardFilter.objects.filter(id=filter_id).exists()


def test_dashboard_filter_ordering(sample_dashboard, seed_db):
    """Test that filters are ordered by 'order' field"""
    filter3 = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="col3",
        order=3,
    )
    filter1 = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="col1",
        order=1,
    )
    filter2 = DashboardFilter.objects.create(
        dashboard=sample_dashboard,
        filter_type="value",
        schema_name="public",
        table_name="test",
        column_name="col2",
        order=2,
    )

    filters = list(DashboardFilter.objects.filter(dashboard=sample_dashboard))

    assert filters[0].order == 1
    assert filters[1].order == 2
    assert filters[2].order == 3

    filter1.delete()
    filter2.delete()
    filter3.delete()


# ================================================================================
# Test DashboardLock Model
# ================================================================================


def test_dashboard_lock_create(sample_dashboard, orguser, seed_db):
    """Test creating a dashboard lock"""
    expires_at = timezone.now() + timedelta(minutes=30)
    lock_token = str(uuid.uuid4())

    lock = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=lock_token,
        expires_at=expires_at,
    )

    assert lock.dashboard == sample_dashboard
    assert lock.locked_by == orguser
    assert lock.lock_token == lock_token
    assert lock.locked_at is not None

    lock.delete()


def test_dashboard_lock_str_representation(sample_dashboard, orguser, seed_db):
    """Test the __str__ method of DashboardLock"""
    lock = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    expected = f"Lock for {sample_dashboard.title} by {orguser.user.email}"
    assert str(lock) == expected

    lock.delete()


def test_dashboard_lock_is_expired_false(sample_dashboard, orguser, seed_db):
    """Test is_expired returns False for valid lock"""
    lock = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    assert lock.is_expired() is False

    lock.delete()


def test_dashboard_lock_is_expired_true(sample_dashboard, orguser, seed_db):
    """Test is_expired returns True for expired lock"""
    lock = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() - timedelta(minutes=1),  # Already expired
    )

    assert lock.is_expired() is True

    lock.delete()


def test_dashboard_lock_to_json(sample_dashboard, orguser, seed_db):
    """Test the to_json method of DashboardLock"""
    expires_at = timezone.now() + timedelta(minutes=30)
    lock_token = str(uuid.uuid4())

    lock = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=lock_token,
        expires_at=expires_at,
    )

    json_data = lock.to_json()

    assert json_data["dashboard_id"] == sample_dashboard.id
    assert json_data["locked_by"] == orguser.user.email
    assert json_data["lock_token"] == lock_token
    assert json_data["is_expired"] is False

    lock.delete()


@pytest.mark.django_db(transaction=True)
def test_dashboard_lock_one_to_one_constraint(sample_dashboard, orguser, seed_db):
    """Test that only one lock can exist per dashboard"""
    lock1 = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    # Try to create another lock for same dashboard
    with pytest.raises(IntegrityError):
        DashboardLock.objects.create(
            dashboard=sample_dashboard,
            locked_by=orguser,
            lock_token=str(uuid.uuid4()),
            expires_at=timezone.now() + timedelta(minutes=30),
        )

    lock1.delete()


@pytest.mark.django_db(transaction=True)
def test_dashboard_lock_unique_token(sample_dashboard, orguser, org, seed_db):
    """Test that lock tokens must be unique"""
    lock_token = str(uuid.uuid4())

    lock1 = DashboardLock.objects.create(
        dashboard=sample_dashboard,
        locked_by=orguser,
        lock_token=lock_token,
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    # Create another dashboard
    dashboard2 = Dashboard.objects.create(
        title="Another Dashboard",
        created_by=orguser,
        org=org,
    )

    # Try to create lock with same token
    with pytest.raises(IntegrityError):
        DashboardLock.objects.create(
            dashboard=dashboard2,
            locked_by=orguser,
            lock_token=lock_token,  # Same token
            expires_at=timezone.now() + timedelta(minutes=30),
        )

    lock1.delete()
    dashboard2.delete()


def test_dashboard_lock_cascade_delete(orguser, org, seed_db):
    """Test that lock is deleted when dashboard is deleted"""
    dashboard = Dashboard.objects.create(
        title="Lock Cascade Dashboard",
        created_by=orguser,
        org=org,
    )

    lock = DashboardLock.objects.create(
        dashboard=dashboard,
        locked_by=orguser,
        lock_token=str(uuid.uuid4()),
        expires_at=timezone.now() + timedelta(minutes=30),
    )

    lock_id = lock.id
    dashboard.delete()

    assert not DashboardLock.objects.filter(id=lock_id).exists()


# ================================================================================
# Test Seed Data
# ================================================================================


def test_seed_data(seed_db):
    """Test that seed data is loaded correctly"""
    assert Role.objects.count() == 5
