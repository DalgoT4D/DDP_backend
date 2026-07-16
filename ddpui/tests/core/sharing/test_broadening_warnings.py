"""The dashboard-broadening warnings.

Three widening paths, each mirroring the shipped NARROWING warn-and-offer
contract (first call returns ``requires_confirmation`` naming the
under-covering charts and changes NOTHING; the re-send carries
``extend_chart_ids`` and/or ``proceed``):

- ``PUT /api/access/dashboard/{id}/general/`` raising a role level,
- ``POST /api/access/dashboard/{id}/grants/`` (single) and the bulk
  ``add_grant`` action,
- enabling the public link (``PUT /api/dashboards/{id}/share/`` and the
  bulk ``toggle_public`` action).

Plus the "extend writes exactly" contract (spec §3): raise each listed
chart's ``analyst_level`` to view (never past view, never lowered), copy
the dashboard's Analyst/Admin/group direct-grant principals onto the chart
at View, skip Member principals, leave ``member_level`` pinned.

Same conventions as test_access_api.py: route functions called directly
via ``mock_request(orguser)``.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import Mock, patch

import pytest
from ninja.errors import HttpError

from ddpui.api.access_api import bulk_access, create_grant, update_general_access
from ddpui.api.dashboard_native_api import toggle_dashboard_sharing
from ddpui.core.sharing import sharing_actions
from ddpui.core.sharing.exceptions import SharingPermissionError
from ddpui.models.general_access import AccessLevel
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import Invitation
from ddpui.models.resource_share import ResourceShare
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.schemas.access_schema import (
    BulkAccessRequest,
    BulkItemRef,
    BulkPublicToggle,
    GeneralAccessUpdate,
    GrantCreate,
)
from ddpui.schemas.dashboard_schema import DashboardShareToggle
from ddpui.tests.api_tests.test_user_org_api import mock_request, seed_db
from ddpui.tests.core.sharing.test_chart_coverage import (
    _chart_grant,
    _dashboard_grant,
    _narrow_chart,
    analyst2,
)
from ddpui.tests.core.sharing.test_chart_render_gate import (
    _chart,
    _dashboard_with_charts,
)
from ddpui.tests.core.sharing.test_detail_view_gate import admin, analyst, member, org

pytestmark = pytest.mark.django_db


def _put_general(caller, dashboard, analyst_level, member_level, **confirm):
    payload = GeneralAccessUpdate(analyst_level=analyst_level, member_level=member_level, **confirm)
    return update_general_access(mock_request(caller), "dashboard", str(dashboard.pk), payload)


def _chart_grant_rows(chart):
    return ResourceShare.objects.filter(resource_type="chart", resource_id=str(chart.pk))


@pytest.fixture
def public_sharing_enabled(org):
    prefs, _ = OrgPreferences.objects.get_or_create(org=org)
    prefs.enable_public_sharing = True
    prefs.save()
    yield prefs


# ================================================================================
# Widening path 1: set_general_access raising a role level
# ================================================================================


class TestGeneralAccessWidening:
    def test_analyst_raise_over_private_tile_requires_confirmation(self, org, analyst):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )

        response = _put_general(analyst, dashboard, AccessLevel.VIEW, AccessLevel.NONE)

        assert response["data"]["requires_confirmation"] is True
        named = response["data"]["under_covering_charts"]
        assert [c["chart_id"] for c in named] == [private.id]
        assert named[0]["title"] == "Salary Breakdown"
        assert named[0]["role_gaps"] == ["analyst"]
        # nothing changed
        dashboard.refresh_from_db()
        assert dashboard.analyst_level == AccessLevel.NONE

    def test_proceed_commits_without_touching_charts(self, org, analyst):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )

        response = _put_general(
            analyst, dashboard, AccessLevel.VIEW, AccessLevel.NONE, proceed=True
        )

        assert response["data"]["requires_confirmation"] is False
        dashboard.refresh_from_db()
        private.refresh_from_db()
        assert dashboard.analyst_level == AccessLevel.VIEW
        assert private.analyst_level == AccessLevel.NONE
        assert not _chart_grant_rows(private).exists()

    def test_extend_writes_exactly(self, org, analyst, analyst2, member, admin):
        """Extend: analyst_level raised to VIEW (not the dashboard's new
        'edit'), Analyst/Admin + group dashboard principals copied at View,
        Member principals skipped, member_level untouched."""
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        _dashboard_grant(org, dashboard, analyst2, permission="edit")
        _dashboard_grant(org, dashboard, member)
        _dashboard_grant(org, dashboard, admin)
        group = UserGroup.objects.create(org=org, name="Extend Group")
        _dashboard_grant(org, dashboard, group)

        response = _put_general(
            analyst,
            dashboard,
            AccessLevel.EDIT,
            AccessLevel.NONE,
            extend_chart_ids=[private.id],
        )

        assert response["data"]["requires_confirmation"] is False
        private.refresh_from_db()
        dashboard.refresh_from_db()
        assert dashboard.analyst_level == AccessLevel.EDIT
        assert private.analyst_level == AccessLevel.VIEW  # raised to view only
        assert private.member_level == AccessLevel.NONE  # pin intact

        rows = {
            (r.principal_type, r.principal_id): r.permission for r in _chart_grant_rows(private)
        }
        assert rows == {
            ("user", analyst2.id): "view",  # copied at View even though dashboard grant is edit
            ("user", admin.id): "view",
            ("group", group.id): "view",
        }  # member principal NOT copied

    def test_extend_never_downgrades_an_existing_edit_grant(self, org, analyst, analyst2):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        _dashboard_grant(org, dashboard, analyst2)
        _chart_grant(org, private, analyst2, permission="edit")

        # analyst2 already covered -> the only gap left is the analyst raise
        _put_general(
            analyst, dashboard, AccessLevel.VIEW, AccessLevel.NONE, extend_chart_ids=[private.id]
        )

        row = _chart_grant_rows(private).get(principal_id=analyst2.id)
        assert row.permission == "edit"

    def test_covered_tiles_widen_silently(self, org, analyst):
        open_chart = _chart(org, analyst)  # analyst_level=edit
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [open_chart]
        )
        response = _put_general(analyst, dashboard, AccessLevel.VIEW, AccessLevel.NONE)
        assert response["data"]["requires_confirmation"] is False
        dashboard.refresh_from_db()
        assert dashboard.analyst_level == AccessLevel.VIEW

    def test_member_raise_names_every_tile_informationally(self, org, analyst):
        open_chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [open_chart]
        )
        response = _put_general(analyst, dashboard, AccessLevel.EDIT, AccessLevel.VIEW)
        named = response["data"]["under_covering_charts"]
        assert response["data"]["requires_confirmation"] is True
        assert [c["chart_id"] for c in named] == [open_chart.id]
        assert named[0]["role_gaps"] == ["member"]
        assert named[0]["extendable"] is False

    def test_narrow_one_role_widen_other_returns_both_prompts(self, org, analyst, member):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.VIEW, [private]
        )
        member_grant = _dashboard_grant(org, dashboard, member)

        response = _put_general(analyst, dashboard, AccessLevel.VIEW, AccessLevel.NONE)

        data = response["data"]
        assert data["requires_confirmation"] is True
        assert [g["id"] for g in data["persisting_grants"]] == [member_grant.id]
        assert [c["chart_id"] for c in data["under_covering_charts"]] == [private.id]

    def test_extend_ids_must_be_subset_of_warned(self, org, analyst):
        private = _narrow_chart(org, analyst)
        stranger_chart = _chart(org, analyst, title="Not On Dashboard")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        with pytest.raises(HttpError) as excinfo:
            _put_general(
                analyst,
                dashboard,
                AccessLevel.VIEW,
                AccessLevel.NONE,
                extend_chart_ids=[stranger_chart.id],
            )
        assert excinfo.value.status_code == 400

    def test_extend_requires_edit_on_each_chart(self, org, analyst, analyst2):
        """analyst2 can edit + share the dashboard (grant) but holds no Edit
        on the Private chart — extend is a 403, proceed still available."""
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        _dashboard_grant(org, dashboard, analyst2, permission="edit")

        with pytest.raises(HttpError) as excinfo:
            _put_general(
                analyst2,
                dashboard,
                AccessLevel.VIEW,
                AccessLevel.NONE,
                extend_chart_ids=[private.id],
            )
        assert excinfo.value.status_code == 403
        dashboard.refresh_from_db()
        private.refresh_from_db()
        assert private.analyst_level == AccessLevel.NONE

    def test_non_dashboard_rtypes_never_broaden_warn(self, org, analyst):
        """Charts contain nothing — raising a chart's own level commits
        silently (the generic set_general_access path, unchanged)."""
        chart = _narrow_chart(org, analyst)
        payload = GeneralAccessUpdate(analyst_level="view", member_level="none")
        response = update_general_access(mock_request(analyst), "chart", str(chart.pk), payload)
        assert response["data"]["requires_confirmation"] is False


# ================================================================================
# Widening path 2: grant-add on a dashboard (single + bulk)
# ================================================================================


class TestGrantAddWidening:
    def test_grant_over_private_tile_requires_confirmation_and_writes_nothing(
        self, org, analyst, analyst2
    ):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        payload = GrantCreate(principal_type="user", principal_id=analyst2.id, permission="view")

        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)

        data = response["data"]
        assert data["requires_confirmation"] is True
        assert data["grant"] is None
        assert [c["chart_id"] for c in data["under_covering_charts"]] == [private.id]
        gap = data["under_covering_charts"][0]["principal_gaps"][0]
        assert gap["principal_id"] == analyst2.id
        assert not ResourceShare.objects.filter(
            resource_type="dashboard", resource_id=str(dashboard.pk)
        ).exists()

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_proceed_writes_grant_without_touching_chart(self, org, analyst, analyst2):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        payload = GrantCreate(
            principal_type="user", principal_id=analyst2.id, permission="view", proceed=True
        )

        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)

        assert response["data"]["requires_confirmation"] is False
        assert response["data"]["grant"]["principal_id"] == analyst2.id
        private.refresh_from_db()
        assert private.analyst_level == AccessLevel.NONE
        assert not _chart_grant_rows(private).exists()

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_extend_writes_grant_then_covers_the_new_principal(self, org, analyst, analyst2):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        payload = GrantCreate(
            principal_type="user",
            principal_id=analyst2.id,
            permission="edit",
            extend_chart_ids=[private.id],
        )

        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)

        assert response["data"]["grant"]["permission"] == "edit"
        private.refresh_from_db()
        # dashboard's analyst_level is still none -> no level raise;
        # the NEW principal (now a dashboard grant) is copied at View
        assert private.analyst_level == AccessLevel.NONE
        row = _chart_grant_rows(private).get(principal_id=analyst2.id)
        assert row.permission == "view"

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_member_principal_grant_warns_with_skipped_member(self, org, analyst, member):
        open_chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [open_chart]
        )
        payload = GrantCreate(principal_type="user", principal_id=member.id, permission="view")

        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        gap = response["data"]["under_covering_charts"][0]["principal_gaps"][0]
        assert gap["skipped_member"] is True

        # proceed: grant written, chart untouched (Member sharing deferred)
        payload = GrantCreate(
            principal_type="user", principal_id=member.id, permission="view", proceed=True
        )
        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert response["data"]["grant"]["principal_id"] == member.id
        assert not _chart_grant_rows(open_chart).exists()

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_new_principal_covered_via_chart_group_grant_no_warning(self, org, analyst, analyst2):
        """Resolver parity: the NEW principal's own group memberships count.
        A chart covered for them via a GROUP grant must not warn (regression:
        the coverage context only batch-loaded the dashboard's existing
        audience's memberships)."""
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        group = UserGroup.objects.create(org=org, name="Covered Guild")
        UserGroupMember.objects.create(
            group=group, orguser=analyst2, status=UserGroupMemberStatus.ACTIVE
        )
        _chart_grant(org, private, group)

        payload = GrantCreate(principal_type="user", principal_id=analyst2.id, permission="view")
        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)

        assert response["data"]["requires_confirmation"] is False
        assert response["data"]["grant"]["principal_id"] == analyst2.id

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_covered_principal_grants_silently(self, org, analyst, admin):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        payload = GrantCreate(principal_type="user", principal_id=admin.id, permission="view")
        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert response["data"]["requires_confirmation"] is False
        assert response["data"]["grant"]["principal_id"] == admin.id

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_unknown_email_invite_warns_before_any_invitation(self, org, admin):
        private = _narrow_chart(org, admin)
        dashboard = _dashboard_with_charts(
            org, admin, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        payload = GrantCreate(
            principal_type="user",
            email="future-analyst@test.com",
            permission="view",
            invite_role="analyst",
        )

        response = create_grant(mock_request(admin), "dashboard", str(dashboard.pk), payload)

        assert response["data"]["requires_confirmation"] is True
        gap = response["data"]["under_covering_charts"][0]["principal_gaps"][0]
        assert gap["principal_type"] == "invite"
        assert not Invitation.objects.filter(invited_email="future-analyst@test.com").exists()

        # proceed: ONE invitation + pending grant
        payload = GrantCreate(
            principal_type="user",
            email="future-analyst@test.com",
            permission="view",
            invite_role="analyst",
            proceed=True,
        )
        response = create_grant(mock_request(admin), "dashboard", str(dashboard.pk), payload)
        assert response["data"]["grant"]["status"] == "pending"
        assert Invitation.objects.filter(invited_email="future-analyst@test.com").count() == 1

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_group_grant_warns_then_extend_copies_group(self, org, analyst, member):
        open_chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [open_chart]
        )
        group = UserGroup.objects.create(org=org, name="Field Team")
        UserGroupMember.objects.create(
            group=group, orguser=member, status=UserGroupMemberStatus.ACTIVE
        )
        payload = GrantCreate(principal_type="group", principal_id=group.id, permission="view")

        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert response["data"]["requires_confirmation"] is True

        payload = GrantCreate(
            principal_type="group",
            principal_id=group.id,
            permission="view",
            extend_chart_ids=[open_chart.id],
        )
        response = create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert response["data"]["grant"]["principal_type"] == "group"
        row = _chart_grant_rows(open_chart).get(principal_type="group", principal_id=group.id)
        assert row.permission == "view"

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_bulk_add_grant_aggregates_confirmations(self, org, analyst, analyst2):
        private = _narrow_chart(org, analyst)
        warned_dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        clean_dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [_chart(org, analyst)]
        )
        clean_dashboard.analyst_level = AccessLevel.EDIT
        clean_dashboard.save(update_fields=["analyst_level"])

        request_payload = BulkAccessRequest(
            items=[
                BulkItemRef(rtype="dashboard", id=str(warned_dashboard.pk)),
                BulkItemRef(rtype="dashboard", id=str(clean_dashboard.pk)),
            ],
            action="add_grant",
            add_grant=GrantCreate(
                principal_type="user", principal_id=analyst2.id, permission="view"
            ),
        )
        response = bulk_access(mock_request(analyst), request_payload)

        data = response["data"]
        assert [c["id"] for c in data["requires_confirmation"]] == [str(warned_dashboard.pk)]
        assert [
            c["chart_id"] for c in data["requires_confirmation"][0]["under_covering_charts"]
        ] == [private.id]
        assert [a["id"] for a in data["applied"]] == [str(clean_dashboard.pk)]

        # re-send with extend for the warned dashboard's chart
        request_payload.add_grant.extend_chart_ids = [private.id]
        response = bulk_access(mock_request(analyst), request_payload)
        assert sorted(a["id"] for a in response["data"]["applied"]) == sorted(
            [str(warned_dashboard.pk), str(clean_dashboard.pk)]
        )
        row = _chart_grant_rows(private).get(principal_id=analyst2.id)
        assert row.permission == "view"

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_bulk_extend_ids_matching_no_selected_dashboard_400(self, org, analyst, analyst2):
        stray_chart = _chart(org, analyst, title="Stray")
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [_narrow_chart(org, analyst)]
        )
        request_payload = BulkAccessRequest(
            items=[BulkItemRef(rtype="dashboard", id=str(dashboard.pk))],
            action="add_grant",
            add_grant=GrantCreate(
                principal_type="user",
                principal_id=analyst2.id,
                permission="view",
                extend_chart_ids=[stray_chart.id],
            ),
        )
        with pytest.raises(HttpError) as excinfo:
            bulk_access(mock_request(analyst), request_payload)
        assert excinfo.value.status_code == 400


# ================================================================================
# Widening path 3: enabling the public link (single + bulk)
# ================================================================================


class TestPublicEnableWidening:
    def test_enable_with_tiles_requires_confirmation(self, org, analyst, public_sharing_enabled):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )

        response = toggle_dashboard_sharing(
            mock_request(analyst), dashboard.id, DashboardShareToggle(is_public=True)
        )

        assert response.requires_confirmation is True
        named = response.under_covering_charts
        assert [c.chart_id for c in named] == [chart.id]
        assert named[0].public_exposure is True
        assert named[0].extendable is False
        dashboard.refresh_from_db()
        assert dashboard.is_public is False
        assert not dashboard.public_share_token

    def test_proceed_enables(self, org, analyst, public_sharing_enabled):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )

        response = toggle_dashboard_sharing(
            mock_request(analyst), dashboard.id, DashboardShareToggle(is_public=True, proceed=True)
        )

        assert response.requires_confirmation is False
        assert response.is_public is True
        dashboard.refresh_from_db()
        assert dashboard.is_public is True
        assert dashboard.public_share_token

    def test_tileless_dashboard_enables_silently(self, org, analyst, public_sharing_enabled):
        dashboard = _dashboard_with_charts(org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [])
        response = toggle_dashboard_sharing(
            mock_request(analyst), dashboard.id, DashboardShareToggle(is_public=True)
        )
        assert response.is_public is True

    def test_disable_never_warns(self, org, analyst, public_sharing_enabled):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )
        dashboard.is_public = True
        dashboard.public_share_token = "tok-broadening-test"
        dashboard.save(update_fields=["is_public", "public_share_token"])

        response = toggle_dashboard_sharing(
            mock_request(analyst), dashboard.id, DashboardShareToggle(is_public=False)
        )
        assert response.requires_confirmation is False
        dashboard.refresh_from_db()
        assert dashboard.is_public is False

    def test_re_enable_of_already_public_dashboard_does_not_warn(
        self, org, analyst, public_sharing_enabled
    ):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )
        dashboard.is_public = True
        dashboard.public_share_token = "tok-already-public"
        dashboard.save(update_fields=["is_public", "public_share_token"])

        response = toggle_dashboard_sharing(
            mock_request(analyst), dashboard.id, DashboardShareToggle(is_public=True)
        )
        assert response.requires_confirmation is False
        assert response.is_public is True

    def test_bulk_toggle_public_confirmation_then_proceed(
        self, org, analyst, public_sharing_enabled
    ):
        chart = _chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.EDIT, AccessLevel.NONE, [chart]
        )
        request_payload = BulkAccessRequest(
            items=[BulkItemRef(rtype="dashboard", id=str(dashboard.pk))],
            action="toggle_public",
            toggle_public=BulkPublicToggle(is_public=True),
        )
        response = bulk_access(mock_request(analyst), request_payload)
        data = response["data"]
        assert [c["id"] for c in data["requires_confirmation"]] == [str(dashboard.pk)]
        dashboard.refresh_from_db()
        assert dashboard.is_public is False

        request_payload.toggle_public.proceed = True
        response = bulk_access(mock_request(analyst), request_payload)
        assert [a["id"] for a in response["data"]["applied"]] == [str(dashboard.pk)]
        dashboard.refresh_from_db()
        assert dashboard.is_public is True


# ================================================================================
# Bulk set_general widening (aggregated prompt + flat extend partition)
# ================================================================================


class TestBulkGeneralWidening:
    def test_aggregated_prompt_then_extend_partition(self, org, analyst):
        private = _narrow_chart(org, analyst)
        warned_dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [private]
        )
        clean_dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.NONE, AccessLevel.NONE, [_chart(org, analyst)]
        )

        request_payload = BulkAccessRequest(
            items=[
                BulkItemRef(rtype="dashboard", id=str(warned_dashboard.pk)),
                BulkItemRef(rtype="dashboard", id=str(clean_dashboard.pk)),
            ],
            action="set_general",
            set_general=GeneralAccessUpdate(analyst_level="view", member_level="none"),
        )
        response = bulk_access(mock_request(analyst), request_payload)
        data = response["data"]
        assert [c["id"] for c in data["requires_confirmation"]] == [str(warned_dashboard.pk)]
        assert [a["id"] for a in data["applied"]] == [str(clean_dashboard.pk)]
        warned_dashboard.refresh_from_db()
        assert warned_dashboard.analyst_level == AccessLevel.NONE

        # re-send with the flat extend list; partitioned per dashboard
        request_payload.set_general = GeneralAccessUpdate(
            analyst_level="view", member_level="none", extend_chart_ids=[private.id]
        )
        response = bulk_access(mock_request(analyst), request_payload)
        assert sorted(a["id"] for a in response["data"]["applied"]) == sorted(
            [str(warned_dashboard.pk), str(clean_dashboard.pk)]
        )
        warned_dashboard.refresh_from_db()
        private.refresh_from_db()
        assert warned_dashboard.analyst_level == AccessLevel.VIEW
        assert private.analyst_level == AccessLevel.VIEW


# ================================================================================
# The extend action's own permission rule (core)
# ================================================================================


class TestExtendPermissionRule:
    def test_extend_charts_requires_edit_on_each(self, org, analyst, analyst2):
        private = _narrow_chart(org, analyst)
        dashboard = _dashboard_with_charts(
            org, analyst, AccessLevel.VIEW, AccessLevel.NONE, [private]
        )
        with pytest.raises(SharingPermissionError):
            sharing_actions.extend_charts_to_cover_dashboard(analyst2, dashboard, [private])
        private.refresh_from_db()
        assert private.analyst_level == AccessLevel.NONE
