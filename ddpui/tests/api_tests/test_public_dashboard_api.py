"""Tests for public dashboard sharing endpoints."""

import os
import django
import pytest

from django.contrib.auth.models import User
from django.test import RequestFactory

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.public_api import get_public_dashboard
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.models.dashboard import Dashboard
from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

rf = RequestFactory()


def _make_public_request():
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


@pytest.fixture
def public_dashboard(orguser, org):
    dashboard = Dashboard.objects.create(
        title="Public Dashboard",
        description="Shared dashboard",
        dashboard_type="native",
        grid_columns=12,
        layout_config=[{"i": "chart-1", "x": 0, "y": 0, "w": 6, "h": 4}],
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": 1, "chartType": "bar", "title": "Monthly Metrics"},
            }
        },
        is_public=True,
        public_share_token="public-dashboard-token",
        created_by=orguser,
        org=org,
        last_modified_by=orguser,
    )
    yield dashboard
    dashboard.delete()


def test_get_public_dashboard_includes_branding(seed_db, public_dashboard, org):
    OrgPreferences.objects.create(
        org=org,
        dashboard_logo_url="https://example.com/logo.png",
        dashboard_logo_width=124,
        chart_palette_name="Custom",
        chart_palette_colors=["#112233", "#445566", "#778899"],
    )

    request = _make_public_request()
    response = get_public_dashboard(request, public_dashboard.public_share_token)
    payload = response.dict() if hasattr(response, "dict") else response

    assert payload["is_valid"] is True
    assert payload["org_name"] == org.name
    assert payload["dashboard_logo_url"] == "https://example.com/logo.png"
    assert payload["dashboard_logo_width"] == 124
    assert payload["chart_palette_name"] == "Custom"
    assert payload["chart_palette_colors"] == ["#112233", "#445566", "#778899"]

    public_dashboard.refresh_from_db()
    assert public_dashboard.public_access_count == 1


def test_get_public_dashboard_defaults_branding_when_org_preferences_missing(
    seed_db,
    public_dashboard,
    org,
):
    request = _make_public_request()
    response = get_public_dashboard(request, public_dashboard.public_share_token)
    payload = response.dict() if hasattr(response, "dict") else response

    assert payload["org_name"] == org.name
    assert payload["dashboard_logo_url"] is None
    assert payload["dashboard_logo_width"] == 80
    assert payload["chart_palette_name"] is None
    assert payload["chart_palette_colors"] is None
