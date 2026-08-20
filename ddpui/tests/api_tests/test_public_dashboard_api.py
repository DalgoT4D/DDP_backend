"""Tests for the public dashboard API endpoint (no authentication)

Tests:
1. get_public_dashboard — valid token returns org identity (org_slug + org_name),
   increments the access count, and rejects invalid / non-public tokens
"""

import os
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.test import RequestFactory
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.public_api import get_public_dashboard
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

rf = RequestFactory()


def _make_public_request():
    """A public endpoint request — no auth, but the handler reads IP / user agent"""
    request = rf.get("/api/v1/public/dashboards/")
    request.META["REMOTE_ADDR"] = "127.0.0.1"
    request.META["HTTP_USER_AGENT"] = "TestAgent"
    return request


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="pubdashuser", email="pubdashuser@test.com", password="testpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Public Dashboard Test Org",
        slug="pub-dash-test-org",
        airbyte_workspace_id="workspace-id",
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


def _create_dashboard(orguser, org, **kwargs):
    dashboard = Dashboard.objects.create(
        title="Test Dashboard",
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        tabs=[
            {
                "id": "tab-1",
                "title": "Tab 1",
                "layout_config": [],
                "components": {},
            }
        ],
        created_by=orguser,
        org=org,
        **kwargs,
    )
    return dashboard


@pytest.fixture
def public_dashboard(orguser, org):
    dashboard = _create_dashboard(orguser, org, is_public=True, public_share_token="pub-dash-token")
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


@pytest.fixture
def private_dashboard(orguser, org):
    dashboard = _create_dashboard(orguser, org, is_public=False, public_share_token="priv-token")
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


class TestGetPublicDashboard:
    """Tests for the get_public_dashboard endpoint"""

    def test_valid_token_returns_org_identity(self, public_dashboard, seed_db):
        """org_slug rides along with org_name — anonymous public views have no
        person or group, so the slug is the only org attribution available."""
        request = _make_public_request()
        response = get_public_dashboard(request, public_dashboard.public_share_token)

        assert response.is_valid is True
        assert response.org_slug == "pub-dash-test-org"
        assert response.org_name == "Public Dashboard Test Org"
        assert response.id == public_dashboard.id

    def test_valid_token_increments_access_count(self, public_dashboard, seed_db):
        request = _make_public_request()
        get_public_dashboard(request, public_dashboard.public_share_token)
        get_public_dashboard(request, public_dashboard.public_share_token)

        public_dashboard.refresh_from_db()
        assert public_dashboard.public_access_count == 2
        assert public_dashboard.last_public_accessed is not None

    def test_invalid_token(self, seed_db):
        request = _make_public_request()
        status, response = get_public_dashboard(request, "nonexistent-token")

        assert status == 404
        assert response.is_valid is False
        assert "not found" in response.error.lower()

    def test_private_dashboard_not_accessible(self, private_dashboard, seed_db):
        """A dashboard with a token but is_public=False stays inaccessible"""
        request = _make_public_request()
        status, response = get_public_dashboard(request, private_dashboard.public_share_token)

        assert status == 404
        assert response.is_valid is False
