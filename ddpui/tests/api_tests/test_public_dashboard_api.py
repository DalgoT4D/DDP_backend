"""Task 11 Part A: org-level public-sharing kill switch on public dashboard
render endpoints.

`OrgPreferences.allow_public_sharing` is read fresh on every public-render
request. While off, an existing `is_public=True` + `public_share_token` row
is treated as if the token didn't exist (404, matching each endpoint's
existing "token not found" convention) -- no data is destroyed, so flipping
the switch back on immediately revives every existing link.

Tests:
1. get_public_dashboard -- full revival (switch off -> 404, back on -> 200, same data)
2. get_public_chart_metadata -- switch off -> 404
3. validate_public_dashboard -- switch off -> is_valid False, back on -> True
4. get_public_geojson_data -- switch off -> 404
5. Org with no OrgPreferences row -- renders normally (default True, no 500)
6. Cross-org -- org A's switch off doesn't kill org B's public dashboard
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.test import RequestFactory

from django.contrib.auth.models import User
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.public_api import (
    get_public_dashboard,
    get_public_chart_metadata,
    validate_public_dashboard,
    get_public_geojson_data,
)
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
        description="Test",
        dashboard_type="native",
        grid_columns=12,
        created_by=orguser,
        org=org,
        is_public=True,
        public_share_token="pub-dash-token-abc",
    )
    yield dashboard
    try:
        dashboard.refresh_from_db()
        dashboard.delete()
    except Dashboard.DoesNotExist:
        pass


# ================================================================================
# get_public_dashboard -- full revival
# ================================================================================


class TestGetPublicDashboardKillSwitch:
    def test_renders_when_no_preferences_row(self, public_dashboard, seed_db):
        assert not OrgPreferences.objects.filter(org=public_dashboard.org).exists()
        request = _make_public_request()
        response = get_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(response, tuple)
        assert response.is_valid is True

    def test_dead_when_switch_off(self, public_dashboard, seed_db):
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)
        request = _make_public_request()
        status, body = get_public_dashboard(request, public_dashboard.public_share_token)
        assert status == 404
        assert body.is_valid is False

    def test_revives_when_switch_flipped_back_on(self, public_dashboard, seed_db):
        prefs = OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)
        request = _make_public_request()
        status, _ = get_public_dashboard(request, public_dashboard.public_share_token)
        assert status == 404

        prefs.allow_public_sharing = True
        prefs.save()

        response = get_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(response, tuple)
        assert response.is_valid is True

    def test_cross_org_isolation(self, public_dashboard, org, seed_db):
        """Org A's switch off must not kill org B's public dashboard."""
        other_org = Org.objects.create(
            name="Other Public Org", slug="other-pub-org", airbyte_workspace_id="ws-2"
        )
        other_user = User.objects.create(username="otherpubuser", email="otherpub@test.com")
        other_orguser = OrgUser.objects.create(
            user=other_user,
            org=other_org,
            new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
        )
        other_dashboard = Dashboard.objects.create(
            title="Other Org Dashboard",
            dashboard_type="native",
            grid_columns=12,
            created_by=other_orguser,
            org=other_org,
            is_public=True,
            public_share_token="other-org-token-xyz",
        )

        # Org A (public_dashboard.org) turns its switch OFF.
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)

        request = _make_public_request()
        status, _ = get_public_dashboard(request, public_dashboard.public_share_token)
        assert status == 404

        # Org B's link must still render.
        response = get_public_dashboard(request, other_dashboard.public_share_token)
        assert not isinstance(response, tuple)
        assert response.is_valid is True

        other_dashboard.delete()
        other_orguser.delete()
        other_user.delete()
        other_org.delete()


class TestGetPublicChartMetadataKillSwitch:
    def test_dead_when_switch_off(self, public_dashboard, seed_db):
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)
        request = _make_public_request()
        status, body = get_public_chart_metadata(
            request, public_dashboard.public_share_token, chart_id=1
        )
        assert status == 404
        assert body.is_valid is False


class TestValidatePublicDashboardKillSwitch:
    def test_invalid_when_switch_off(self, public_dashboard, seed_db):
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)
        request = _make_public_request()
        status, body = validate_public_dashboard(request, public_dashboard.public_share_token)
        assert status == 404
        assert body.is_valid is False

    def test_valid_when_switch_on(self, public_dashboard, seed_db):
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=True)
        request = _make_public_request()
        response = validate_public_dashboard(request, public_dashboard.public_share_token)
        assert not isinstance(response, tuple)
        assert response.is_valid is True


class TestGetPublicGeojsonDataKillSwitch:
    def test_dead_when_switch_off(self, public_dashboard, seed_db):
        OrgPreferences.objects.create(org=public_dashboard.org, allow_public_sharing=False)
        request = _make_public_request()
        status, body = get_public_geojson_data(
            request, public_dashboard.public_share_token, geojson_id=1
        )
        assert status == 404
        assert body.is_valid is False
