"""Charts join the sharing model.

Covers the chart-specific rules layered onto the (already generic) sharing
machinery:

- migration 0179: behavior-preserving field defaults (analyst "edit",
  member "none" — the backfill values) + the `can_share_charts` permission
  seed for existing databases (fixture rows cover fresh installs).
- the member-pin: `member_level` may only ever be "none" on a chart —
  model `clean()` and `set_general_access` both reject anything else.
- grant rules: user-principal grants Analyst/Admin only; group grants
  allowed (their Member members resolve to nothing); email invites only at
  an Analyst/Admin invite role; Member requesters blocked with a pointer
  at the dashboard request path.
- resolver: a Member viewer gets NO general/grant contribution on charts
  (member_sharing=False), including via group grants — ownership still
  admits them.
- list scoping: `ChartService.list_charts` through `accessible_filter`,
  per role.
- standalone gates: detail/update/dashboards endpoints resolver-gated;
  the container-context branch is regression-covered in
  test_chart_render_gate.py (unchanged).
- org-default seeding on create, with the member clamp.
- the deep-link map knows charts.

Same conventions as test_access_api.py: route functions called directly
via `mock_request(orguser)`.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from importlib import import_module
from unittest.mock import Mock, patch

import pytest
from django.apps import apps as live_apps
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.core.sharing.access_resolver import accessible_filter, effective_permission
from ddpui.core.sharing.deep_links import NOUN_BY_RTYPE, build_resource_url
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.models.visualization import Chart
from ddpui.services.chart_service import ChartData, ChartService
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

# module name starts with a digit — importable only via import_module
chart_migration = import_module("ddpui.migrations.0179_chart_general_access_levels")

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Chart Sharing Org", slug="chart-sharing-org", airbyte_workspace_id="w1"
    )
    yield org
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "chartshare-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "chartshare-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def analyst2(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "chartshare-analyst2")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "chartshare-member")
    yield ou
    ou.delete()


def _chart(org_obj, creator, owner=None, analyst_level=None, member_level=None, title="V11 Chart"):
    """A chart row. Levels default to the MODEL defaults (edit, none) — the
    behavior-preserving backfill values — unless a test narrows them."""
    kwargs = {}
    if analyst_level is not None:
        kwargs["analyst_level"] = analyst_level
    if member_level is not None:
        kwargs["member_level"] = member_level
    return Chart.objects.create(
        title=title,
        chart_type="bar",
        schema_name="public",
        table_name="beneficiaries",
        extra_config={"dimension_column": "category"},
        created_by=creator,
        owner=owner,
        last_modified_by=creator,
        org=org_obj,
        **kwargs,
    )


def _grant(org_obj, resource, principal_orguser, permission="view"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type="chart",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal_orguser.id,
        permission=permission,
        status="active",
    )


def _group_with(org_obj, *members):
    group = UserGroup.objects.create(org=org_obj, name="v11-group")
    for orguser in members:
        UserGroupMember.objects.create(
            group=group, orguser=orguser, status=UserGroupMemberStatus.ACTIVE
        )
    return group


def slugs_for(role_slug: str) -> set:
    role = Role.objects.get(slug=role_slug)
    return set(RolePermission.objects.filter(role=role).values_list("permission__slug", flat=True))


# ================================================================================
# Migration 0179: backfill values + can_share_charts seed
# ================================================================================


class TestMigration0179:
    def test_model_defaults_are_the_backfill_values(self, org, analyst):
        """The AddField defaults ARE the behavior-preserving backfill
        (decision #3): a chart row created with no explicit levels — exactly
        what every pre-v1.1 row becomes on migration day — is (edit, none)."""
        chart = _chart(org, analyst)
        chart.refresh_from_db()
        assert chart.analyst_level == AccessLevel.EDIT
        assert chart.member_level == AccessLevel.NONE

    def test_seed_fixtures_grant_can_share_charts_to_admin_and_analyst_not_member(self, seed_db):
        assert Permission.objects.filter(slug="can_share_charts").exists()
        assert "can_share_charts" in slugs_for("super-admin")
        assert "can_share_charts" in slugs_for("admin")
        assert "can_share_charts" in slugs_for("analyst")
        assert "can_share_charts" not in slugs_for("member")

    def test_migration_seeds_slug_and_role_grants(self, seed_db):
        Permission.objects.filter(slug="can_share_charts").delete()
        assert "can_share_charts" not in slugs_for("admin")

        chart_migration.seed_chart_share_permission(live_apps, None)

        assert Permission.objects.filter(slug="can_share_charts").exists()
        assert "can_share_charts" in slugs_for("super-admin")
        assert "can_share_charts" in slugs_for("admin")
        assert "can_share_charts" in slugs_for("analyst")
        assert "can_share_charts" not in slugs_for("member")

    def test_migration_is_idempotent_on_double_run(self, seed_db):
        chart_migration.seed_chart_share_permission(live_apps, None)
        before_permissions = Permission.objects.count()
        before_role_permissions = RolePermission.objects.count()

        chart_migration.seed_chart_share_permission(live_apps, None)

        assert Permission.objects.count() == before_permissions
        assert RolePermission.objects.count() == before_role_permissions

    def test_migration_reverse_removes_the_slug_only(self, seed_db):
        chart_migration.seed_chart_share_permission(live_apps, None)
        chart_migration.remove_chart_share_permission(live_apps, None)

        assert not Permission.objects.filter(slug="can_share_charts").exists()
        assert "can_share_charts" not in slugs_for("admin")
        # siblings untouched
        assert Permission.objects.filter(slug="can_share_dashboards").exists()
        # restore for the rest of the session (seed_db is session-scoped)
        chart_migration.seed_chart_share_permission(live_apps, None)


# ================================================================================
# The member-pin: member_level may only ever be "none" on a chart
# ================================================================================


class TestMemberPin:
    def test_model_clean_rejects_non_none_member_level(self, org, analyst):
        chart = _chart(org, analyst)
        chart.member_level = AccessLevel.VIEW
        with pytest.raises(ValidationError):
            chart.clean()

    def test_model_clean_accepts_member_none(self, org, analyst):
        chart = _chart(org, analyst)
        chart.clean()  # must not raise

    def test_general_access_update_rejects_member_level(self, org, admin, analyst):
        from ddpui.api.access_api import update_general_access
        from ddpui.schemas.access_schema import GeneralAccessUpdate

        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            update_general_access(
                mock_request(admin),
                "chart",
                str(chart.pk),
                GeneralAccessUpdate(analyst_level="view", member_level="view"),
            )
        assert excinfo.value.status_code == 400
        assert "Members" in str(excinfo.value.message)
        chart.refresh_from_db()
        assert chart.member_level == AccessLevel.NONE

    def test_general_access_update_analyst_levels_work(self, org, admin, analyst):
        from ddpui.api.access_api import update_general_access
        from ddpui.schemas.access_schema import GeneralAccessUpdate

        chart = _chart(org, analyst)
        response = update_general_access(
            mock_request(admin),
            "chart",
            str(chart.pk),
            GeneralAccessUpdate(analyst_level="view", member_level="none", remove_grant_ids=[]),
        )
        assert response["success"] is True
        chart.refresh_from_db()
        assert chart.analyst_level == AccessLevel.VIEW
        assert chart.member_level == AccessLevel.NONE


# ================================================================================
# Grant rules: Analyst/Admin principals only; groups allowed; invites
# Analyst/Admin-role only
# ================================================================================


class TestChartGrantRules:
    def test_admin_grants_chart_to_analyst(self, org, admin, analyst, analyst2):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        chart = _chart(org, analyst2, analyst_level=AccessLevel.NONE)
        with patch("ddpui.utils.awsses.send_resource_shared_email", Mock()):
            response = create_grant(
                mock_request(admin),
                "chart",
                str(chart.pk),
                GrantCreate(principal_type="user", principal_id=analyst.id, permission="view"),
            )
        assert response["success"] is True
        assert effective_permission(analyst, "chart", chart) == "view"

    def test_member_principal_grant_400(self, org, admin, analyst, member):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            create_grant(
                mock_request(admin),
                "chart",
                str(chart.pk),
                GrantCreate(principal_type="user", principal_id=member.id, permission="view"),
            )
        assert excinfo.value.status_code == 400
        assert "Members" in str(excinfo.value.message)
        assert not ResourceShare.objects.filter(
            resource_type="chart", resource_id=str(chart.pk)
        ).exists()

    def test_group_grant_allowed(self, org, admin, analyst, member):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        chart = _chart(org, analyst)
        group = _group_with(org, analyst, member)
        response = create_grant(
            mock_request(admin),
            "chart",
            str(chart.pk),
            GrantCreate(principal_type="group", principal_id=group.id, permission="view"),
        )
        assert response["success"] is True

    def test_member_role_email_invite_400_and_sends_nothing(self, org, admin, analyst):
        """The default invite role is Member — a chart share to an unknown
        email without an explicit Analyst/Admin invite_role is rejected
        BEFORE any invitation exists."""
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate
        from ddpui.models.org_user import Invitation

        chart = _chart(org, analyst)
        with patch("ddpui.utils.awsses.send_invite_user_email", Mock()) as mock_invite:
            with pytest.raises(HttpError) as excinfo:
                create_grant(
                    mock_request(admin),
                    "chart",
                    str(chart.pk),
                    GrantCreate(
                        principal_type="user", email="new-person@test.com", permission="view"
                    ),
                )
        assert excinfo.value.status_code == 400
        assert "Analyst or Admin" in str(excinfo.value.message)
        mock_invite.assert_not_called()
        assert not Invitation.objects.filter(invited_email="new-person@test.com").exists()
        assert not ResourceShare.objects.filter(
            resource_type="chart", resource_id=str(chart.pk)
        ).exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_admin_analyst_role_email_invite_creates_pending_grant(self, org, admin, analyst):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        chart = _chart(org, analyst)
        response = create_grant(
            mock_request(admin),
            "chart",
            str(chart.pk),
            GrantCreate(
                principal_type="user",
                email="future-analyst@test.com",
                permission="view",
                invite_role=ANALYST_ROLE,
            ),
        )
        assert response["success"] is True
        share = ResourceShare.objects.get(resource_type="chart", resource_id=str(chart.pk))
        assert share.status == "pending"
        assert share.pending_email == "future-analyst@test.com"

    def test_bulk_member_grant_skips_chart_applies_dashboard(self, org, admin, member, analyst):
        """Bulk add_grant with a Member principal: the chart item skips with
        validation_error, the dashboard item applies."""
        from ddpui.api.access_api import bulk_access
        from ddpui.models.dashboard import Dashboard
        from ddpui.schemas.access_schema import BulkAccessRequest, BulkItemRef, GrantCreate

        chart = _chart(org, analyst)
        dashboard = Dashboard.objects.create(
            title="V11 Dash", org=org, owner=admin, created_by=admin
        )
        payload = BulkAccessRequest(
            items=[
                BulkItemRef(rtype="chart", id=str(chart.pk)),
                BulkItemRef(rtype="dashboard", id=str(dashboard.pk)),
            ],
            action="add_grant",
            add_grant=GrantCreate(principal_type="user", principal_id=member.id, permission="view"),
        )
        with patch("ddpui.utils.awsses.send_resource_shared_email", Mock()):
            response = bulk_access(mock_request(admin), payload)
        data = response["data"]
        assert data["applied"] == [{"rtype": "dashboard", "id": str(dashboard.pk)}]
        assert {(s["rtype"], s["reason"]) for s in data["skipped"]} == {
            ("chart", "validation_error")
        }


# ================================================================================
# Resolver: Member viewers get nothing from chart general access or grants
# ================================================================================


class TestMemberResolutionExcluded:
    def test_direct_member_grant_row_resolves_to_nothing(self, org, analyst, member):
        chart = _chart(org, analyst)
        _grant(org, chart, member, permission="view")
        assert effective_permission(member, "chart", chart) is None

    def test_group_grant_resolves_to_nothing_for_member_but_admits_analyst(
        self, org, analyst, analyst2, member
    ):
        chart = _chart(org, analyst, analyst_level=AccessLevel.NONE)
        group = _group_with(org, analyst2, member)
        ResourceShare.objects.create(
            org=org,
            resource_type="chart",
            resource_id=str(chart.pk),
            principal_type="group",
            principal_id=group.id,
            permission="view",
            status="active",
        )
        assert effective_permission(member, "chart", chart) is None
        assert effective_permission(analyst2, "chart", chart) == "view"

    def test_member_owner_keeps_edit(self, org, analyst, member):
        chart = _chart(org, analyst, owner=member)
        assert effective_permission(member, "chart", chart) == "edit"

    def test_accessible_filter_excludes_grant_admitted_member(self, org, analyst, member):
        chart = _chart(org, analyst)
        _grant(org, chart, member, permission="view")
        owned = _chart(org, member, owner=member, title="Member Owned")

        visible = set(
            Chart.objects.filter(accessible_filter(member, "chart")).values_list("id", flat=True)
        )
        assert visible == {owned.id}

    def test_member_grant_row_on_dashboard_still_works(self, org, admin, member):
        """The exclusion is chart-specific: member_sharing=True rtypes keep
        the pre-v1.1 grant behavior."""
        from ddpui.models.dashboard import Dashboard

        dashboard = Dashboard.objects.create(
            title="V11 Member Dash", org=org, owner=admin, created_by=admin
        )
        ResourceShare.objects.create(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="user",
            principal_id=member.id,
            permission="view",
            status="active",
        )
        assert effective_permission(member, "dashboard", dashboard) == "view"


# ================================================================================
# Request-access: Member requesters blocked; Analyst+ flows work
# ================================================================================


class TestChartAccessRequests:
    def test_member_requester_400_with_dashboard_pointer(self, org, analyst, member):
        from ddpui.api.access_api import create_access_request
        from ddpui.schemas.access_schema import AccessRequestCreate

        chart = _chart(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            create_access_request(
                mock_request(member),
                "chart",
                str(chart.pk),
                AccessRequestCreate(requested_permission="view"),
            )
        assert excinfo.value.status_code == 400
        assert "request access to the dashboard instead" in str(excinfo.value.message)

    def test_analyst_without_access_can_request(self, org, analyst, analyst2):
        from ddpui.api.access_api import create_access_request
        from ddpui.schemas.access_schema import AccessRequestCreate

        chart = _chart(org, analyst, analyst_level=AccessLevel.NONE)
        response = create_access_request(
            mock_request(analyst2),
            "chart",
            str(chart.pk),
            AccessRequestCreate(requested_permission="view"),
        )
        assert response["success"] is True
        assert response["data"]["status"] == "pending"

    def test_approve_writes_a_working_chart_grant(self, org, analyst, analyst2):
        from ddpui.api.access_api import approve_access_request, create_access_request
        from ddpui.schemas.access_schema import AccessRequestCreate, AccessRequestDecision

        chart = _chart(org, analyst, owner=analyst, analyst_level=AccessLevel.NONE)
        created = create_access_request(
            mock_request(analyst2),
            "chart",
            str(chart.pk),
            AccessRequestCreate(requested_permission="view"),
        )
        request_id = created["data"]["id"]

        approve_access_request(
            mock_request(analyst), request_id, AccessRequestDecision(permission="view")
        )
        assert effective_permission(analyst2, "chart", chart) == "view"


# ================================================================================
# List scoping per role (ChartService.list_charts)
# ================================================================================


class TestChartListScoping:
    def test_analyst_day_one_sees_default_charts(self, org, admin, analyst):
        """Backfill-default charts (analyst_level=edit) stay visible to every
        analyst — day-one behavior unchanged."""
        chart = _chart(org, admin)
        charts, total = ChartService.list_charts(org=org, orguser=analyst)
        assert total == 1
        assert charts[0].id == chart.id

    def test_narrowed_chart_disappears_for_non_admitted_analyst(
        self, org, admin, analyst, analyst2
    ):
        visible = _chart(org, admin, title="Visible")
        narrowed = _chart(org, admin, analyst_level=AccessLevel.NONE, title="Narrowed")
        granted = _chart(org, admin, analyst_level=AccessLevel.NONE, title="Granted")
        _grant(org, granted, analyst, permission="view")
        owned = _chart(org, analyst, owner=analyst, analyst_level=AccessLevel.NONE, title="Owned")

        charts, total = ChartService.list_charts(org=org, orguser=analyst)
        ids = {c.id for c in charts}
        assert ids == {visible.id, granted.id, owned.id}
        assert narrowed.id not in ids
        assert total == 3

        # a different analyst without the grant sees neither narrowed chart
        charts2, _ = ChartService.list_charts(org=org, orguser=analyst2)
        assert {c.id for c in charts2} == {visible.id}

    def test_admin_sees_everything(self, org, admin, analyst):
        _chart(org, analyst, analyst_level=AccessLevel.NONE)
        _chart(org, analyst)
        _, total = ChartService.list_charts(org=org, orguser=admin)
        assert total == 2

    def test_member_sees_only_owned(self, org, admin, member):
        _chart(org, admin)  # analyst_level=edit — but Members have no level
        _grant(org, _chart(org, admin, title="Granted"), member)
        owned = _chart(org, member, owner=member, title="Member Owned")

        charts, total = ChartService.list_charts(org=org, orguser=member)
        assert total == 1
        assert charts[0].id == owned.id


# ================================================================================
# Standalone endpoint gates (detail / update / dashboards)
# ================================================================================


class TestStandaloneGates:
    def test_analyst_denied_detail_on_narrowed_chart(self, org, admin, analyst):
        from ddpui.api.charts_api import get_chart

        chart = _chart(org, admin, analyst_level=AccessLevel.NONE)
        with pytest.raises(HttpError) as excinfo:
            get_chart(mock_request(analyst), chart.id)
        assert excinfo.value.status_code == 403

    def test_granted_analyst_gets_detail_on_narrowed_chart(self, org, admin, analyst):
        from ddpui.api.charts_api import get_chart

        chart = _chart(org, admin, analyst_level=AccessLevel.NONE)
        _grant(org, chart, analyst, permission="view")
        response = get_chart(mock_request(analyst), chart.id)
        assert response.id == chart.id

    def test_view_level_analyst_cannot_update(self, org, admin, analyst):
        from ddpui.api.charts_api import update_chart
        from ddpui.schemas.chart_schemas import ChartUpdate

        chart = _chart(org, admin, analyst_level=AccessLevel.VIEW)
        with pytest.raises(HttpError) as excinfo:
            update_chart(mock_request(analyst), chart.id, ChartUpdate(title="Renamed"))
        assert excinfo.value.status_code == 403
        chart.refresh_from_db()
        assert chart.title != "Renamed"

    def test_edit_level_analyst_can_update(self, org, admin, analyst):
        from ddpui.api.charts_api import update_chart
        from ddpui.schemas.chart_schemas import ChartUpdate

        chart = _chart(org, admin)  # analyst_level defaults to edit
        response = update_chart(mock_request(analyst), chart.id, ChartUpdate(title="Renamed"))
        assert response.title == "Renamed"

    def test_narrowed_chart_hides_its_dashboards(self, org, admin, analyst):
        from ddpui.api.charts_api import get_chart_dashboards

        chart = _chart(org, admin, analyst_level=AccessLevel.NONE)
        with pytest.raises(HttpError) as excinfo:
            get_chart_dashboards(mock_request(analyst), chart.id)
        assert excinfo.value.status_code == 403

    def test_get_access_overview_works_for_charts(self, org, admin, analyst):
        """The generic /api/access/ read works for charts once registered."""
        from ddpui.api.access_api import get_access

        chart = _chart(org, admin, owner=admin)
        response = get_access(mock_request(analyst), "chart", str(chart.pk))
        data = response["data"]
        assert data["capabilities"] == {
            "general": True,
            "grants": True,
            "public_link": False,
            "requests": True,
        }
        assert data["general_access"] == {"analyst_level": "edit", "member_level": "none"}
        assert data["viewer"]["effective_permission"] == "edit"


# ================================================================================
# Org-default seeding on create (member clamped to none)
# ================================================================================


class TestCreateSeeding:
    def test_new_chart_seeds_org_default_analyst_level_and_clamps_member(self, org, analyst):
        OrgPreferences.objects.create(
            org=org,
            default_analyst_level=AccessLevel.EDIT,
            default_member_level=AccessLevel.VIEW,
        )
        chart = ChartService.create_chart(
            ChartData(
                title="Seeded",
                chart_type="bar",
                schema_name="public",
                table_name="t",
                extra_config={},
            ),
            analyst,
        )
        assert chart.analyst_level == AccessLevel.EDIT
        assert chart.member_level == AccessLevel.NONE  # clamped despite org default "view"

    def test_new_chart_without_prefs_falls_back_to_view_none(self, org, analyst):
        chart = ChartService.create_chart(
            ChartData(
                title="Fallback",
                chart_type="bar",
                schema_name="public",
                table_name="t",
                extra_config={},
            ),
            analyst,
        )
        assert chart.analyst_level == AccessLevel.VIEW  # (view, view) product fallback
        assert chart.member_level == AccessLevel.NONE  # member clamp


# ================================================================================
# Deep links
# ================================================================================


def test_deep_link_map_knows_charts():
    assert NOUN_BY_RTYPE["chart"] == "chart"
    assert build_resource_url("chart", 42).endswith("/charts/42")
