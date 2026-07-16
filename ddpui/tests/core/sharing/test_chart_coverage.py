"""v1.1 Milestone 2 — the consolidated tile-walk + the coverage service.

Covers:

- ``chart_access.chart_ids_in_tabs`` / ``dashboard_chart_ids`` (the M2
  consolidation of the tabs->components->config.chartId walk) fails CLOSED
  on malformed tabs — the M0 review's requested regression test: a public
  chart endpoint must 404, never 500, when a dashboard's ``tabs`` JSON is
  junk.
- the coverage verdicts (``core.sharing.coverage``): covered / role-gap
  (analyst, member) / principal-gap (users and groups) / public-exposure.
- ``GET /api/dashboards/{id}/chart-coverage/`` — requires dashboard Edit;
  single-chart and whole-dashboard (bulk) modes.

Same conventions as test_chart_render_gate.py: route functions called
directly via ``mock_request(orguser)``.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.core.sharing.chart_access import chart_ids_in_tabs, dashboard_chart_ids
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_render_gate import (
    _chart,
    _dashboard_with_charts,
)
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db


def _tile_tab(*chart_ids):
    return {
        "id": "tab-1",
        "title": "Tab 1",
        "layout_config": [],
        "components": {
            str(i): {"type": "chart", "config": {"chartId": cid}}
            for i, cid in enumerate(chart_ids, start=1)
        },
    }


# ================================================================================
# The consolidated tile-walk — malformed tabs fail CLOSED (M0 follow-up)
# ================================================================================


class TestChartIdsInTabsFailsClosed:
    def test_none_tabs_yields_nothing(self):
        assert chart_ids_in_tabs(None) == set()

    def test_bare_string_tabs_yields_nothing(self):
        # iterating a string gives characters; the old walk raised
        # AttributeError (a 500 on the public endpoints) — now empty.
        assert chart_ids_in_tabs("garbage") == set()

    def test_non_dict_tab_entries_are_skipped(self):
        assert chart_ids_in_tabs(["tab", 42, None, _tile_tab(7)]) == {7}

    def test_non_dict_component_is_skipped(self):
        tabs = [
            {
                "id": "t",
                "components": {
                    "1": "not-a-dict",
                    "2": None,
                    "3": {"type": "chart", "config": {"chartId": 9}},
                },
            }
        ]
        assert chart_ids_in_tabs(tabs) == {9}

    def test_non_dict_components_and_config_are_skipped(self):
        tabs = [
            {"id": "t1", "components": ["not", "a", "dict"]},
            {"id": "t2", "components": {"1": {"type": "chart", "config": "junk"}}},
        ]
        assert chart_ids_in_tabs(tabs) == set()

    def test_non_integer_chart_ids_are_dropped(self):
        tabs = [
            {
                "id": "t",
                "components": {
                    "1": {"type": "chart", "config": {"chartId": "12"}},
                    "2": {"type": "chart", "config": {"chartId": True}},
                    "3": {"type": "chart", "config": {"chartId": None}},
                    "4": {"type": "chart", "config": {"chartId": 12}},
                },
            }
        ]
        assert chart_ids_in_tabs(tabs) == {12}

    def test_non_chart_components_are_ignored(self):
        tabs = [{"id": "t", "components": {"1": {"type": "text", "config": {"chartId": 5}}}}]
        assert chart_ids_in_tabs(tabs) == set()

    def test_dashboard_chart_ids_wraps_the_walk(self, org, analyst):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [chart]
        )
        assert dashboard_chart_ids(dashboard) == {chart.id}
        dashboard.tabs = "corrupted"
        assert dashboard_chart_ids(dashboard) == set()


# ================================================================================
# Coverage verdicts (core.sharing.coverage)
# ================================================================================


def _narrow_chart(org_obj, creator, owner=None, title="Salary Breakdown"):
    """A chart deliberately narrowed to Private (analyst none) — the spec's
    example case."""
    chart = _chart(org_obj, creator, owner=owner, title=title)
    chart.analyst_level = AccessLevel.NONE
    chart.save(update_fields=["analyst_level"])
    return chart


def _dashboard_grant(org_obj, dashboard, principal, permission="view", status="active"):
    if isinstance(principal, UserGroup):
        principal_type, principal_id = "group", principal.id
    else:
        principal_type, principal_id = "user", principal.id
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type="dashboard",
        resource_id=str(dashboard.pk),
        principal_type=principal_type,
        principal_id=principal_id,
        permission=permission,
        status=status,
    )


def _chart_grant(org_obj, chart, principal, permission="view"):
    if isinstance(principal, UserGroup):
        principal_type, principal_id = "group", principal.id
    else:
        principal_type, principal_id = "user", principal.id
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type="chart",
        resource_id=str(chart.pk),
        principal_type=principal_type,
        principal_id=principal_id,
        permission=permission,
        status="active",
    )


def _coverage(caller, dashboard, chart=None):
    from ddpui.api.dashboard_native_api import get_dashboard_chart_coverage

    return get_dashboard_chart_coverage(
        mock_request(caller), dashboard.id, chart_id=chart.id if chart else None
    )


@pytest.fixture
def analyst2(org, seed_db):
    from ddpui.auth import ANALYST_ROLE
    from ddpui.tests.core.sharing.test_chart_sharing_v11 import _make_orguser

    ou = _make_orguser(org, ANALYST_ROLE, "coverage-analyst2")
    yield ou
    ou.delete()


class TestCoverageVerdicts:
    def test_open_chart_on_analyst_dashboard_is_covered(self, org, analyst):
        chart = _chart(org, analyst)  # analyst_level defaults to edit
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [chart]
        )
        response = _coverage(analyst, dashboard, chart)
        assert response.covered is True
        verdict = response.charts[0]
        assert verdict.covered is True
        assert verdict.role_gaps == []
        assert verdict.principal_gaps == []
        assert verdict.public_exposure is False

    def test_private_chart_on_analyst_dashboard_has_analyst_role_gap(self, org, analyst):
        chart = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [chart]
        )
        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        assert verdict.role_gaps == ["analyst"]
        assert verdict.extendable is True
        assert verdict.title == "Salary Breakdown"

    def test_member_visible_dashboard_flags_member_gap_not_extendable_alone(self, org, analyst):
        """Charts can never admit Members in v1.1 (member_level pinned), so a
        member-visible dashboard always exposes past the chart's own levels —
        an informational class: named, but not extendable by itself."""
        chart = _chart(org, analyst)  # analyst edit — analysts covered
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.VIEW, [chart]
        )
        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        assert verdict.role_gaps == ["member"]
        assert verdict.extendable is False

    def test_user_principal_gap_and_grant_coverage(self, org, analyst, analyst2):
        chart = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _dashboard_grant(org, dashboard, analyst2)

        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        gaps = verdict.principal_gaps
        assert [(g.principal_type, g.principal_id) for g in gaps] == [("user", analyst2.id)]
        assert gaps[0].skipped_member is False
        assert verdict.extendable is True

        # a chart grant for the same principal closes the gap
        _chart_grant(org, chart, analyst2)
        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is True

    def test_member_principal_gap_is_flagged_skipped(self, org, analyst, member):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _dashboard_grant(org, dashboard, member)

        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        assert verdict.principal_gaps[0].skipped_member is True
        # a Member-only principal gap is not extendable (extend skips them)
        assert verdict.extendable is False

    def test_admin_principal_never_gaps(self, org, analyst, admin):
        chart = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _dashboard_grant(org, dashboard, admin, permission="edit")
        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.principal_gaps == []
        assert verdict.covered is True

    def test_group_principal_gap_and_group_grant_coverage(self, org, analyst, member):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        group = UserGroup.objects.create(org=org, name="Funders")
        UserGroupMember.objects.create(
            group=group, orguser=member, status=UserGroupMemberStatus.ACTIVE
        )
        _dashboard_grant(org, dashboard, group)

        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        assert [(g.principal_type, g.principal_id) for g in verdict.principal_gaps] == [
            ("group", group.id)
        ]
        assert verdict.principal_gaps[0].name == "Funders"

        _chart_grant(org, chart, group)
        assert _coverage(analyst, dashboard, chart).charts[0].covered is True

    def test_user_principal_covered_via_chart_group_grant(self, org, analyst, analyst2):
        """A dashboard USER principal is covered when the chart holds a
        GROUP grant for a group they belong to (resolver parity)."""
        chart = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _dashboard_grant(org, dashboard, analyst2)
        group = UserGroup.objects.create(org=org, name="Analysts Guild")
        UserGroupMember.objects.create(
            group=group, orguser=analyst2, status=UserGroupMemberStatus.ACTIVE
        )
        _chart_grant(org, chart, group)

        assert _coverage(analyst, dashboard, chart).charts[0].covered is True

    def test_pending_dashboard_grants_are_ignored(self, org, analyst, analyst2):
        chart = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        _dashboard_grant(org, dashboard, analyst2, status="pending")
        assert _coverage(analyst, dashboard, chart).charts[0].principal_gaps == []

    def test_public_dashboard_flags_public_exposure(self, org, analyst):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [chart]
        )
        dashboard.is_public = True
        dashboard.save(update_fields=["is_public"])

        verdict = _coverage(analyst, dashboard, chart).charts[0]
        assert verdict.covered is False
        assert verdict.public_exposure is True
        assert verdict.extendable is False  # anonymous exposure has no extend

    def test_viewer_can_edit_reflects_the_callers_chart_edit(self, org, analyst, analyst2, admin):
        chart = _narrow_chart(org, analyst)  # analyst owns it
        dashboard = _dashboard_with_charts(
            org, analyst2, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )
        assert _coverage(analyst, dashboard, chart).charts[0].viewer_can_edit is True  # owner
        assert _coverage(admin, dashboard, chart).charts[0].viewer_can_edit is True  # admin
        # analyst2: chart is Private and ungranted — can't extend it
        assert _coverage(analyst2, dashboard, chart).charts[0].viewer_can_edit is False


class TestCoverageEndpointGates:
    def test_bulk_mode_lists_only_under_covering_tiles(self, org, analyst):
        covered = _chart(org, analyst, title="Open Chart")
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [covered, private]
        )
        response = _coverage(analyst, dashboard)
        assert response.covered is False
        assert [v.chart_id for v in response.charts] == [private.id]

    def test_bulk_mode_all_covered_is_empty_and_covered(self, org, analyst):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [chart]
        )
        response = _coverage(analyst, dashboard)
        assert response.covered is True
        assert response.charts == []

    def test_requires_dashboard_edit(self, org, analyst, analyst2):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [chart]
        )
        with pytest.raises(HttpError) as excinfo:
            _coverage(analyst2, dashboard, chart)  # analyst2 has only view
        assert excinfo.value.status_code == 403

    def test_cross_org_dashboard_404(self, org, analyst):
        from ddpui.models.org import Org

        other_org = Org.objects.create(name="Coverage Other", slug="coverage-other-org")
        outsider_dashboard = Dashboard.objects.create(
            title="Other", org=other_org, analyst_level=AccessLevel.VIEW
        )
        with pytest.raises(HttpError) as excinfo:
            _coverage(analyst, outsider_dashboard)
        assert excinfo.value.status_code == 404
        other_org.delete()

    def test_cross_org_chart_404(self, org, analyst):
        from ddpui.models.org import Org

        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])
        other_org = Org.objects.create(name="Coverage Other2", slug="coverage-other-org2")
        foreign_chart = _chart(other_org, None, title="Foreign")
        with pytest.raises(HttpError) as excinfo:
            _coverage(analyst, dashboard, foreign_chart)
        assert excinfo.value.status_code == 404
        other_org.delete()

    def test_embed_preflight_works_for_a_chart_not_yet_a_tile(self, org, analyst):
        """The embed warning checks coverage BEFORE the chart is added."""
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [])
        candidate = _narrow_chart(org, analyst, title="Candidate")
        verdict = _coverage(analyst, dashboard, candidate).charts[0]
        assert verdict.covered is False
        assert verdict.role_gaps == ["analyst"]
