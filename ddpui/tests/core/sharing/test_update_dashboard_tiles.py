"""v1.1 Milestone 2 — `update_dashboard` tile validation (the blind-JSON
overwrite hole).

`PUT /api/dashboards/{id}/` writes the raw `tabs` JSON; before M2 ANY chart
id could be embedded blind. Now every chart id NEWLY present in the payload:

- must be org-owned (400; cross-org ids indistinguishable from nonexistent),
- must resolve to >= view for the CALLER (403),
- if it under-covers the dashboard's audience: 409 with the coverage
  verdicts unless the request carries the embed confirmation
  (`extend_chart_ids`/`proceed`) — a CONFIRMED embed is never blocked
  (spec §3: inline rendering is the rule, the warning is exposure honesty).

Charts already on the dashboard are never re-validated — saves that only
move/resize/remove tiles stay untouched (auto-save regression).
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.api.dashboard_native_api import update_dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org
from ddpui.models.resource_share import ResourceShare
from ddpui.schemas.access_schema import EmbedCoverageConfirmation
from ddpui.schemas.dashboard_schema import DashboardTabSchema, DashboardUpdate
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_coverage import _narrow_chart
from ddpui.tests.core.sharing.test_chart_render_gate import (
    _chart,
    _dashboard_with_charts,
)
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db


def _tabs_with(*chart_ids):
    return [
        DashboardTabSchema(
            id="tab-1",
            title="Tab 1",
            layout_config=[],
            components={
                str(i): {"type": "chart", "config": {"chartId": cid}}
                for i, cid in enumerate(chart_ids, start=1)
            },
        )
    ]


def _put(caller, dashboard, *chart_ids, **confirm):
    payload = DashboardUpdate(tabs=_tabs_with(*chart_ids), **confirm)
    return update_dashboard(mock_request(caller), dashboard.id, payload)


@pytest.fixture
def analyst2(org, seed_db):
    from ddpui.auth import ANALYST_ROLE
    from ddpui.tests.core.sharing.test_chart_sharing_v11 import _make_orguser

    ou = _make_orguser(org, ANALYST_ROLE, "tiles-analyst2")
    yield ou
    ou.delete()


class TestUpdateDashboardTileValidation:
    def test_covered_chart_embeds_silently(self, org, analyst):
        chart = _chart(org, analyst)  # analyst_level=edit
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])

        response = _put(analyst, dashboard, chart.id)

        assert response.tabs[0].components["1"]["config"]["chartId"] == chart.id

    def test_cross_org_chart_id_400(self, org, analyst):
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])
        other_org = Org.objects.create(name="Tiles Other", slug="tiles-other-org")
        foreign_chart = _chart(other_org, None, title="Foreign")

        with pytest.raises(HttpError) as excinfo:
            _put(analyst, dashboard, foreign_chart.id)
        assert excinfo.value.status_code == 400
        dashboard.refresh_from_db()
        assert dashboard.tabs[0]["components"] == {}
        other_org.delete()

    def test_nonexistent_chart_id_400(self, org, analyst):
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])
        with pytest.raises(HttpError) as excinfo:
            _put(analyst, dashboard, 999999)
        assert excinfo.value.status_code == 400

    def test_chart_without_view_access_403(self, org, analyst, analyst2):
        """analyst2 can edit the dashboard but cannot even VIEW the Private
        chart — embedding it is a plain 403, not a warning."""
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(org, analyst2, AccessLevel.EDIT, AccessLevel.NONE, [])

        with pytest.raises(HttpError) as excinfo:
            _put(analyst2, dashboard, private.id)
        assert excinfo.value.status_code == 403

    def test_under_covering_embed_409_with_verdicts_nothing_saved(self, org, analyst):
        private = _narrow_chart(org, analyst)  # owner embeds their own Private chart
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])

        status, confirmation = _put(analyst, dashboard, private.id)

        assert status == 409
        assert isinstance(confirmation, EmbedCoverageConfirmation)
        assert confirmation.requires_confirmation is True
        assert [c.chart_id for c in confirmation.under_covering_charts] == [private.id]
        assert confirmation.under_covering_charts[0].role_gaps == ["analyst"]
        dashboard.refresh_from_db()
        assert dashboard.tabs[0]["components"] == {}

    def test_proceed_saves_without_touching_chart(self, org, analyst):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])

        response = _put(analyst, dashboard, private.id, proceed=True)

        assert response.tabs[0].components["1"]["config"]["chartId"] == private.id
        private.refresh_from_db()
        assert private.analyst_level == AccessLevel.NONE

    def test_extend_saves_and_extends(self, org, analyst):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])

        response = _put(analyst, dashboard, private.id, extend_chart_ids=[private.id])

        assert response.tabs[0].components["1"]["config"]["chartId"] == private.id
        private.refresh_from_db()
        assert private.analyst_level == AccessLevel.VIEW

    def test_extend_subset_violation_400(self, org, analyst):
        private = _narrow_chart(org, analyst)
        stray = _chart(org, analyst, title="Stray")
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])

        with pytest.raises(HttpError) as excinfo:
            _put(analyst, dashboard, private.id, extend_chart_ids=[stray.id])
        assert excinfo.value.status_code == 400

    def test_confirmed_embed_without_chart_edit_saves_but_extend_403s(self, org, analyst, analyst2):
        """An embedder with View but not Edit on the chart can PROCEED (the
        embed is never blocked once confirmed) but cannot EXTEND."""
        viewable = _narrow_chart(org, analyst)
        ResourceShare.objects.create(
            org=org,
            resource_type="chart",
            resource_id=str(viewable.pk),
            principal_type="user",
            principal_id=analyst2.id,
            permission="view",
            status="active",
        )
        dashboard = _dashboard_with_charts(org, analyst2, AccessLevel.VIEW, AccessLevel.NONE, [])

        # proceed works
        response = _put(analyst2, dashboard, viewable.id, proceed=True)
        assert response.tabs[0].components["1"]["config"]["chartId"] == viewable.id

        # extend on a fresh dashboard 403s (no Edit on the chart)
        dashboard2 = _dashboard_with_charts(org, analyst2, AccessLevel.VIEW, AccessLevel.NONE, [])
        with pytest.raises(HttpError) as excinfo:
            _put(analyst2, dashboard2, viewable.id, extend_chart_ids=[viewable.id])
        assert excinfo.value.status_code == 403

    def test_existing_tiles_are_not_revalidated(self, org, analyst):
        """Saves that only rearrange EXISTING tiles never re-trigger the
        embed warning — auto-save keeps working on dashboards whose old
        tiles under-cover."""
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [private]
        )

        response = _put(analyst, dashboard, private.id)  # same tile, new layout payload

        assert response.tabs[0].components["1"]["config"]["chartId"] == private.id

    def test_title_only_update_untouched(self, org, analyst):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [private]
        )
        payload = DashboardUpdate(title="Renamed")
        response = update_dashboard(mock_request(analyst), dashboard.id, payload)
        assert response.title == "Renamed"

    def test_member_visible_dashboard_embed_warns_informationally(self, org, analyst):
        """Embedding into a member-visible dashboard names the chart with a
        member gap (charts can't admit Members in v1.1) — proceed commits."""
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.EDIT, AccessLevel.VIEW, [])

        status, confirmation = _put(analyst, dashboard, chart.id)
        assert status == 409
        assert confirmation.under_covering_charts[0].role_gaps == ["member"]

        response = _put(analyst, dashboard, chart.id, proceed=True)
        assert response.tabs[0].components["1"]["config"]["chartId"] == chart.id
