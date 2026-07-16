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

from unittest.mock import MagicMock, patch

import pytest
from django.test import RequestFactory
from ninja.errors import HttpError

from django.contrib.auth.models import User
from ddpui.models.org import Org, OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.schemas.chart_schemas import ChartDataPayload
from ddpui.api.public_api import (
    get_public_dashboard,
    get_public_chart_metadata,
    get_public_chart_data,
    download_public_chart_data_csv,
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


# ================================================================================
# Milestone 0: public chart tile-membership leak fix.
#
# Before the fix, `get_public_chart_metadata`/`get_public_chart_data` only
# checked that the requested chart_id belonged to the public dashboard's org
# -- not that it was actually placed as a tile on that dashboard. Anyone
# holding any org's public dashboard link could fetch metadata/data for
# EVERY chart in the org by guessing/iterating chart_id.
# ================================================================================


def _make_chart(org_obj, creator, title="Public Leak Test Chart"):
    return Chart.objects.create(
        title=title,
        chart_type="bar",
        schema_name="public",
        table_name="beneficiaries",
        extra_config={
            "dimension_column": "category",
            "metrics": [{"column": "amount", "aggregation": "sum"}],
        },
        created_by=creator,
        org=org_obj,
    )


@pytest.fixture
def chart_on_dashboard(orguser, org, public_dashboard):
    """A chart actually placed as a tile on `public_dashboard` -- the
    legitimate case that must keep working."""
    chart = _make_chart(org, orguser, title="Tile Chart")
    public_dashboard.tabs = [
        {
            "id": "tab-1",
            "title": "Tab 1",
            "layout_config": [],
            "components": {"1": {"type": "chart", "config": {"chartId": chart.id}}},
        }
    ]
    public_dashboard.save()
    yield chart
    chart.delete()


@pytest.fixture
def chart_not_on_dashboard(orguser, org):
    """Same org as `public_dashboard`, but never placed on it -- the leak
    target: org-scoping alone would have let this through."""
    chart = _make_chart(org, orguser, title="Non-Tile Chart")
    yield chart
    chart.delete()


@pytest.fixture
def cross_org_chart():
    """A chart belonging to a different org entirely."""
    other_org = Org.objects.create(
        name="Other Chart Leak Org", slug="other-chart-leak-org", airbyte_workspace_id="ws-leak"
    )
    other_user = User.objects.create(username="otherleakuser", email="otherleak@test.com")
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    chart = _make_chart(other_org, other_orguser, title="Cross-Org Chart")
    yield chart
    chart.delete()
    other_orguser.delete()
    other_user.delete()
    other_org.delete()


class TestGetPublicChartMetadataTileMembership:
    def test_non_tile_chart_404(self, public_dashboard, chart_not_on_dashboard, seed_db):
        """Same-org chart that isn't a tile on the public dashboard -> 404."""
        request = _make_public_request()
        status, body = get_public_chart_metadata(
            request, public_dashboard.public_share_token, chart_id=chart_not_on_dashboard.id
        )
        assert status == 404
        assert body.is_valid is False

    def test_cross_org_chart_404(self, public_dashboard, cross_org_chart, seed_db):
        """Chart belonging to a different org entirely -> 404."""
        request = _make_public_request()
        status, body = get_public_chart_metadata(
            request, public_dashboard.public_share_token, chart_id=cross_org_chart.id
        )
        assert status == 404
        assert body.is_valid is False

    def test_non_public_dashboard_404(self, orguser, org, chart_not_on_dashboard, seed_db):
        """Token resolves to a dashboard that is not public -> 404."""
        private_dashboard = Dashboard.objects.create(
            title="Private Dashboard",
            dashboard_type="native",
            grid_columns=12,
            created_by=orguser,
            org=org,
            is_public=False,
            public_share_token="priv-dash-token-leak",
        )
        request = _make_public_request()
        status, body = get_public_chart_metadata(
            request, private_dashboard.public_share_token, chart_id=chart_not_on_dashboard.id
        )
        assert status == 404
        assert body.is_valid is False
        private_dashboard.delete()

    def test_legitimate_tile_chart_200(self, public_dashboard, chart_on_dashboard, seed_db):
        """A chart actually placed on the public dashboard still renders."""
        request = _make_public_request()
        response = get_public_chart_metadata(
            request, public_dashboard.public_share_token, chart_id=chart_on_dashboard.id
        )
        assert not isinstance(response, tuple)
        assert response["is_valid"] is True
        assert response["id"] == chart_on_dashboard.id


class TestGetPublicChartDataTileMembership:
    def test_non_tile_chart_404(self, public_dashboard, chart_not_on_dashboard, seed_db):
        request = _make_public_request()
        status, body = get_public_chart_data(
            request, public_dashboard.public_share_token, chart_id=chart_not_on_dashboard.id
        )
        assert status == 404
        assert body.is_valid is False

    def test_cross_org_chart_404(self, public_dashboard, cross_org_chart, seed_db):
        request = _make_public_request()
        status, body = get_public_chart_data(
            request, public_dashboard.public_share_token, chart_id=cross_org_chart.id
        )
        assert status == 404
        assert body.is_valid is False

    def test_legitimate_tile_chart_200(self, org, public_dashboard, chart_on_dashboard, seed_db):
        """A chart actually placed on the public dashboard still returns data."""
        OrgWarehouse.objects.create(
            org=org, wtype="postgres", name="Leak Test Warehouse", airbyte_destination_id="dest-1"
        )
        mock_chart_result = {
            "data": {"categories": ["A"], "values": [1]},
            "echarts_config": {"type": "bar"},
        }
        with patch(
            "ddpui.api.charts_api.generate_chart_data_and_config",
            return_value=mock_chart_result,
        ):
            request = _make_public_request()
            response = get_public_chart_data(
                request, public_dashboard.public_share_token, chart_id=chart_on_dashboard.id
            )

        assert not isinstance(response, tuple)
        assert response.is_valid is True


class TestDownloadPublicChartDataCsvTileMembership:
    """The CSV-export endpoint diverges from the others -- it raises
    ``HttpError`` (not the tuple-return convention) -- so it gets its own
    coverage rather than sharing ``_get_public_dashboard_chart``."""

    def _payload(self):
        return ChartDataPayload(chart_type="bar", schema_name="public", table_name="beneficiaries")

    def test_non_tile_chart_rejected(self, public_dashboard, chart_not_on_dashboard, seed_db):
        request = _make_public_request()
        with pytest.raises(HttpError):
            download_public_chart_data_csv(
                request,
                public_dashboard.public_share_token,
                chart_not_on_dashboard.id,
                self._payload(),
            )

    def test_cross_org_chart_rejected(self, public_dashboard, cross_org_chart, seed_db):
        request = _make_public_request()
        with pytest.raises(HttpError):
            download_public_chart_data_csv(
                request, public_dashboard.public_share_token, cross_org_chart.id, self._payload()
            )

    def test_legitimate_tile_chart_streams(
        self, org, public_dashboard, chart_on_dashboard, seed_db
    ):
        """A chart actually placed on the public dashboard still streams a CSV."""
        OrgWarehouse.objects.create(
            org=org, wtype="postgres", name="CSV Warehouse", airbyte_destination_id="dest-csv"
        )
        request = _make_public_request()
        with patch(
            "ddpui.api.charts_api.stream_chart_data_csv",
            return_value=iter([b"a,b\n1,2\n"]),
        ):
            response = download_public_chart_data_csv(
                request,
                public_dashboard.public_share_token,
                chart_on_dashboard.id,
                self._payload(),
            )
        assert response.status_code == 200
