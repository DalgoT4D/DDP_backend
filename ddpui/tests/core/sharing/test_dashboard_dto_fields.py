"""Task 6b Part A: the dashboard LIST/DETAIL response DTO gains sharing
fields the frontend's ShareModal follow-up needs — `analyst_level`,
`member_level` (D1: straight off the model columns, replacing the old
`general_audience`/`general_level` pair) and `is_owner`/`is_creator`
(viewer-relative, computed from `owner_id`/`created_by_id` against the
caller's `orguser.id` — no extra query per row).

`is_owner` mirrors the resolver's ownership rule (owner_id wins;
created_by is the fallback when owner is null) via
`ddpui.core.ownership.is_owner` — NOT `can_delete_resource`, which also
admits admins who aren't literally the owner.

`is_creator` is a plain `created_by_id == viewer.id` comparison,
independent of the owner fallback.

Same fixtures/conventions as `test_detail_view_gate.py` (imported directly).
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.api.dashboard_native_api import get_dashboard, list_dashboards
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.services.dashboard_service import DashboardService
from ddpui.tests.api_tests.test_user_org_api import mock_request
from ddpui.tests.core.sharing.test_detail_view_gate import (
    _dashboard,
    _grant,
    admin,
    analyst,
    member,
    org,
    seed_db,
)

pytestmark = pytest.mark.django_db


class TestDashboardListDTOFields:
    def test_owner_sees_is_owner_true(self, org, member, analyst):
        dashboard = _dashboard(org, member, AccessLevel.EDIT, AccessLevel.EDIT)
        request = mock_request(member)

        [response] = list_dashboards(request)

        assert response.is_owner is True
        assert response.is_creator is True
        assert response.analyst_level == AccessLevel.EDIT
        assert response.member_level == AccessLevel.EDIT

    def test_creator_without_ownership_sees_is_owner_false(self, org, member, analyst):
        """owner_id wins over created_by: creator who no longer owns the
        dashboard is is_creator=True but is_owner=False."""
        dashboard = Dashboard.objects.create(
            title="Creator Not Owner",
            org=org,
            owner=analyst,
            created_by=member,
            analyst_level=AccessLevel.VIEW,
            member_level=AccessLevel.VIEW,
        )
        request = mock_request(member)

        [response] = list_dashboards(request)

        assert response.is_owner is False
        assert response.is_creator is True

    def test_member_with_general_access_sees_both_false(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)
        request = mock_request(member)

        [response] = list_dashboards(request)

        assert response.is_owner is False
        assert response.is_creator is False
        assert response.analyst_level == AccessLevel.VIEW
        assert response.member_level == AccessLevel.VIEW

    def test_member_with_grant_sees_both_false(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.NONE, AccessLevel.NONE)
        _grant(org, "dashboard", dashboard, member)
        request = mock_request(member)

        [response] = list_dashboards(request)

        assert response.is_owner is False
        assert response.is_creator is False

    def test_owner_null_falls_back_to_created_by(self, org, member):
        """owner is null (e.g. cleared by SET_NULL) -> created_by is the
        ownership fallback, mirroring the resolver's _is_owner."""
        dashboard = Dashboard.objects.create(
            title="Owner Null",
            org=org,
            owner=None,
            created_by=member,
            analyst_level=AccessLevel.NONE,
            member_level=AccessLevel.NONE,
        )
        request = mock_request(member)

        [response] = list_dashboards(request)

        assert response.is_owner is True
        assert response.is_creator is True

    def test_service_layer_list_query_count_unaffected(
        self, org, member, analyst, django_assert_num_queries
    ):
        """The new fields (analyst_level/member_level/is_owner/is_creator)
        read straight off columns already on the fetched row and never
        touch `.owner`/`.created_by` FK objects, so they add no query of
        their own. The binding "no N+1" guarantee for
        `DashboardService.list_dashboards` -- the queryset this endpoint's
        DTO loop iterates -- is `test_list_scoping.py::
        TestDashboardScoping::test_query_count_no_n_plus_one`, left
        untouched by this task and still green. (Full-endpoint
        serialization is a separate, pre-existing N+1 from
        `get_dashboard_response`'s `dashboard.filters.all()` and
        `to_json()` FK hops -- unrelated to Part A and out of this task's
        scope.)
        """
        for _ in range(5):
            _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)

        with django_assert_num_queries(1):
            list(DashboardService.list_dashboards(org=org, orguser=member))


class TestDashboardDetailDTOFields:
    def test_detail_carries_same_fields(self, org, member, analyst):
        dashboard = _dashboard(org, member, AccessLevel.NONE, AccessLevel.NONE)
        request = mock_request(member)

        response = get_dashboard(request, dashboard.id)

        assert response.is_owner is True
        assert response.is_creator is True
        assert response.analyst_level == AccessLevel.NONE
        assert response.member_level == AccessLevel.NONE

    def test_detail_non_owner_non_creator(self, org, member, analyst):
        dashboard = _dashboard(org, analyst, AccessLevel.VIEW, AccessLevel.VIEW)
        request = mock_request(member)

        response = get_dashboard(request, dashboard.id)

        assert response.is_owner is False
        assert response.is_creator is False
