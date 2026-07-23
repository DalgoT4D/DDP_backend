"""
Tests for the Admin Portal API and its platform-admin gate.

Milestone 1 acceptance (features/admin-portal/v1/plan.md §6, §7):
  - non-platform-admin -> 403 on the guarded /admin/ping route
  - platform admin      -> 200 on the same route
  - /currentuserv2 surfaces is_platform_admin
"""

import pytest
from unittest.mock import Mock, patch
from django.core.management import call_command
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.api.admin_api import (
    get_admin_ping,
    get_admin_stats,
    get_admin_orgs,
    post_admin_org,
    get_admin_org,
    put_admin_org,
    post_admin_org_deactivate,
    post_admin_org_reactivate,
    get_admin_org_users,
    post_admin_org_user_invite,
    put_admin_org_user_role,
    post_admin_org_user_deactivate,
    post_admin_org_user_reactivate,
    get_admin_org_user_removal_impact,
    delete_admin_org_user,
    delete_admin_org_invitation,
    admin_router,
    post_admin_login,
    post_admin_logout,
    post_admin_token_refresh,
    get_admin_currentuser,
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminInviteUserSchema,
    AdminChangeRoleSchema,
    AdminLoginSchema,
)
from ddpui.api.user_org_api import get_current_user_v2, post_organization_user_invite_v1
from ddpui.core import orguserfunctions
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import (
    OrgUser,
    UserAttributes,
    Invitation,
    NewInvitationSchema,
    AcceptInvitationSchema,
)
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.auth import (
    CustomJwtAuthMiddleware,
    AdminJwtAuthMiddleware,
    ACCOUNT_MANAGER_ROLE,
    SUPER_ADMIN_ROLE,
    ANALYST_ROLE,
    GUEST_ROLE,
)
from ddpui.core.admin import admin_service
from rest_framework_simplejwt.tokens import AccessToken

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    """load the role/permission seed data the guard and currentuserv2 need"""
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


@pytest.fixture
def org():
    """an Org to hang OrgUsers off of"""
    org = Org.objects.create(name="admin-test-org", slug="admin-test-org")
    yield org
    org.delete()


@pytest.fixture
def authuser():
    """a django User"""
    user = User.objects.create(
        username="admin-test-user", email="admin-test-user@example.com", password="pw"
    )
    yield user
    user.delete()


@pytest.fixture
def orguser(authuser, org, seed_db):
    """an OrgUser with the account-manager role (which has can_view_orgusers)"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


def mock_request(orguser: OrgUser = None):
    """mirror the mock_request helper in test_user_org_api.py"""
    request = Mock()
    request.orguser = orguser
    request.permissions = []
    if orguser and orguser.new_role:
        permission_slugs = RolePermission.objects.filter(role=orguser.new_role).values_list(
            "permission__slug", flat=True
        )
        request.permissions = list(permission_slugs)
    return request


# ---- the guard: /admin/ping 403 vs 200 ----------------------------------------


def test_admin_ping_forbidden_for_non_platform_admin(orguser):
    """a user without is_platform_admin is refused with 403"""
    request = mock_request(orguser)
    # no UserAttributes row at all -> not a platform admin
    with pytest.raises(HttpError) as excinfo:
        get_admin_ping(request)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "platform admin access required"


def test_admin_ping_forbidden_when_flag_false(orguser):
    """a user whose is_platform_admin is explicitly False is refused with 403"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=False)
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_ping(request)
    assert excinfo.value.status_code == 403


def test_admin_ping_ok_for_platform_admin(orguser):
    """a platform admin gets 200 (the stub payload)"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    request = mock_request(orguser)
    response = get_admin_ping(request)
    assert response == {"detail": "pong"}


# ---- /currentuserv2 surfaces is_platform_admin --------------------------------


def test_currentuserv2_reports_platform_admin_true(orguser):
    """currentuserv2 returns is_platform_admin: true for a platform admin"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    request = mock_request(orguser)
    response = get_current_user_v2(request)
    assert len(response) == 1
    assert response[0].is_platform_admin is True


def test_currentuserv2_reports_platform_admin_false(orguser):
    """currentuserv2 defaults is_platform_admin to false for a normal user"""
    request = mock_request(orguser)
    response = get_current_user_v2(request)
    assert len(response) == 1
    assert response[0].is_platform_admin is False


# ---- /admin/stats: guarded + correct counts -----------------------------------


def test_admin_stats_forbidden_for_non_platform_admin(orguser):
    """a non-platform-admin is refused with 403 on /admin/stats"""
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_stats(request)
    assert excinfo.value.status_code == 403


def test_admin_stats_returns_counts_for_platform_admin(orguser):
    """
    /admin/stats returns real total_orgs and distinct-user total_users for an admin.

    total_users counts distinct users across orgs: the same user belonging to two
    orgs still counts once.
    """
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    # a second org that the same user also belongs to -> proves distinct-user count
    org2 = Org.objects.create(name="admin-test-org-2", slug="admin-test-org-2")
    OrgUser.objects.create(
        user=orguser.user,
        org=org2,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    request = mock_request(orguser)
    response = get_admin_stats(request)
    assert response.total_orgs == 2
    assert response.total_users == 1  # one distinct user across both orgs


# ---- org lifecycle: list / create / detail / edit / deactivate / reactivate ----


@pytest.fixture
def platform_admin_request(orguser):
    """a mock request from a platform admin"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    return mock_request(orguser)


def test_admin_orgs_forbidden_for_non_platform_admin(orguser):
    """the org list route is gated too — non-admin gets 403"""
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_orgs(request)
    assert excinfo.value.status_code == 403


def test_admin_list_orgs(platform_admin_request):
    """lists every org (active + inactive) with user counts"""
    Org.objects.create(name="Alpha Org", slug="alpha-org")
    Org.objects.create(name="Beta Org", slug="beta-org", is_active=False)
    response = get_admin_orgs(platform_admin_request)
    by_name = {o.name: o for o in response}
    assert "Alpha Org" in by_name
    assert by_name["Alpha Org"].is_active is True
    assert by_name["Beta Org"].is_active is False


@patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_admin_create_org_happy_path(mock_setup_airbyte, mock_connectors, platform_admin_request):
    """create org: Org + OrgPlans created; Airbyte workspace provisioned once"""
    mock_setup_airbyte.return_value = Mock(workspaceId="ws-abc")
    payload = AdminCreateOrgSchema(name="Bhumi")

    response = post_admin_org(platform_admin_request, payload)

    assert response.name == "Bhumi"
    assert response.slug == "bhumi"
    assert response.is_active is True
    org = Org.objects.filter(name="Bhumi").first()
    assert org is not None
    assert OrgPlans.objects.filter(org=org).count() == 1
    mock_setup_airbyte.assert_called_once()


@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_admin_create_org_rolls_back_on_airbyte_failure(mock_setup_airbyte, platform_admin_request):
    """a failed Airbyte call leaves ZERO trace — no orphaned Org or OrgPlans row"""
    mock_setup_airbyte.side_effect = Exception("airbyte is down")
    payload = AdminCreateOrgSchema(name="Bhumi")

    with pytest.raises(HttpError) as excinfo:
        post_admin_org(platform_admin_request, payload)

    assert excinfo.value.status_code == 400
    assert Org.objects.filter(name="Bhumi").count() == 0
    assert OrgPlans.objects.filter(org__name="Bhumi").count() == 0


def test_admin_org_detail_404(platform_admin_request):
    """detail of a missing org is 404"""
    with pytest.raises(HttpError) as excinfo:
        get_admin_org(platform_admin_request, 999999)
    assert excinfo.value.status_code == 404


def test_admin_edit_org_locks_slug(platform_admin_request):
    """edit updates name + viz_url but never the slug (locked post-create)"""
    org = Org.objects.create(name="Old Name", slug="old-name", is_active=True)
    payload = AdminUpdateOrgSchema(name="New Name", viz_url="https://viz.example.com")

    response = put_admin_org(platform_admin_request, org.id, payload)

    org.refresh_from_db()
    assert org.name == "New Name"
    assert org.viz_url == "https://viz.example.com/"  # HttpUrl str normalizes trailing slash
    assert org.slug == "old-name"  # LOCKED — unchanged
    assert response.slug == "old-name"
    assert response.viz_url == "https://viz.example.com/"


def test_admin_edit_org_updates_base_plan(platform_admin_request):
    """edit can change the plan (lives on OrgPlans)"""
    org = Org.objects.create(name="Plan Org", slug="plan-org")
    OrgPlans.objects.create(org=org, base_plan="Free Trial")
    payload = AdminUpdateOrgSchema(base_plan="Dalgo")

    response = put_admin_org(platform_admin_request, org.id, payload)

    assert OrgPlans.objects.get(org=org).base_plan == "Dalgo"
    assert response.base_plan == "Dalgo"


def test_admin_deactivate_and_reactivate_org(platform_admin_request):
    """deactivate flips is_active False; reactivate flips it back True"""
    org = Org.objects.create(name="Toggle Org", slug="toggle-org", is_active=True)

    deactivated = post_admin_org_deactivate(platform_admin_request, org.id)
    org.refresh_from_db()
    assert org.is_active is False
    assert deactivated.is_active is False

    reactivated = post_admin_org_reactivate(platform_admin_request, org.id)
    org.refresh_from_db()
    assert org.is_active is True
    assert reactivated.is_active is True


# ============================================================================
# Milestone 4 — Users tab: invite / role / deactivate / remove / cancel invite
# ============================================================================
# The platform admin (the `orguser` fixture, in "admin-test-org") is NOT a member
# of the target orgs below — every test exercises the cross-org path.


def _role(slug):
    return Role.objects.filter(slug=slug).first()


def _make_org(name, slug):
    return Org.objects.create(name=name, slug=slug)


def _make_member(org, email, role_slug):
    """create a User + OrgUser in `org` with `role_slug`; return the OrgUser"""
    user = User.objects.create(username=email, email=email)
    return OrgUser.objects.create(user=user, org=org, new_role=_role(role_slug))


@pytest.fixture
def akshara(seed_db):
    return _make_org("Akshara", "akshara")


@pytest.fixture
def bhumi(seed_db):
    return _make_org("Bhumi", "bhumi")


# ---- guard: the new routes are gated too --------------------------------------


def test_admin_users_routes_forbidden_for_non_platform_admin(orguser, akshara):
    """a non-platform-admin is refused with 403 on the Users-tab routes"""
    request = mock_request(orguser)
    for call in (
        lambda: get_admin_org_users(request, akshara.id),
        lambda: get_admin_org_user_removal_impact(request, akshara.id, 1),
        lambda: delete_admin_org_invitation(request, akshara.id, 1),
    ):
        with pytest.raises(HttpError) as excinfo:
            call()
        assert excinfo.value.status_code == 403


# ---- invite (cross-org) + invite-cap-skip -------------------------------------


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_admin_invite_into_org_records_target_org(platform_admin_request, akshara):
    """
    inviting a NEW email into Akshara creates an Invitation whose invited_in_org is
    Akshara — even though invited_by (the platform admin) belongs to a different org.
    This is what lets accept/cancel resolve the correct org cross-org.
    """
    payload = AdminInviteUserSchema(
        invited_email="newuser@akshara.org",
        invited_role_uuid=_role(GUEST_ROLE).uuid,
    )
    post_admin_org_user_invite(platform_admin_request, akshara.id, payload)

    inv = Invitation.objects.filter(invited_email="newuser@akshara.org").first()
    assert inv is not None
    assert inv.invited_in_org_id == akshara.id  # target org, not the admin's org
    assert inv.invited_by.org_id != akshara.id  # admin is NOT a member of Akshara
    assert inv.invited_new_role.slug == GUEST_ROLE


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_admin_invite_cap_skipped_for_platform_admin(platform_admin_request, akshara):
    """
    INVITE-CAP-SKIP (plan §8 #1): the platform admin's own role is account-manager
    (level 4). A regular inviter at level 4 CANNOT invite a super-admin (level 5) —
    proven by the contrast assertion below. Via the admin portal the cap is skipped,
    so the same admin CAN invite at super-admin.
    """
    super_admin_uuid = _role(SUPER_ADMIN_ROLE).uuid

    # contrast: as a regular single-org inviter, account-manager -> super-admin is refused
    regular_payload = NewInvitationSchema(
        invited_email="wouldberefused@akshara.org",
        invited_role_uuid=super_admin_uuid,
    )
    _, regular_error = orguserfunctions.invite_user_v1(
        platform_admin_request.orguser, regular_payload
    )
    assert regular_error == "Insufficient permissions for this operation"

    # via the admin portal the SAME admin may invite at super-admin (cap skipped)
    payload = AdminInviteUserSchema(
        invited_email="bigboss@akshara.org",
        invited_role_uuid=super_admin_uuid,
    )
    post_admin_org_user_invite(platform_admin_request, akshara.id, payload)

    inv = Invitation.objects.filter(invited_email="bigboss@akshara.org").first()
    assert inv is not None
    assert inv.invited_new_role.slug == SUPER_ADMIN_ROLE
    assert inv.invited_in_org_id == akshara.id


def test_admin_invite_into_missing_org_404(platform_admin_request):
    """inviting into a non-existent org is 404"""
    payload = AdminInviteUserSchema(
        invited_email="x@y.org", invited_role_uuid=_role(GUEST_ROLE).uuid
    )
    with pytest.raises(HttpError) as excinfo:
        post_admin_org_user_invite(platform_admin_request, 999999, payload)
    assert excinfo.value.status_code == 404


# ---- change role (cross-org, cap skipped) -------------------------------------


def test_admin_change_role_in_org(platform_admin_request, akshara):
    """the admin can change a member's role, even up to super-admin (cap skipped)"""
    member = _make_member(akshara, "member@akshara.org", GUEST_ROLE)
    payload = AdminChangeRoleSchema(role_uuid=_role(SUPER_ADMIN_ROLE).uuid)

    response = put_admin_org_user_role(platform_admin_request, akshara.id, member.id, payload)

    member.refresh_from_db()
    assert member.new_role.slug == SUPER_ADMIN_ROLE
    assert response.new_role_slug == SUPER_ADMIN_ROLE


def test_admin_change_role_wrong_org_404(platform_admin_request, akshara, bhumi):
    """changing the role of an ouid that belongs to a DIFFERENT org is 404"""
    bhumi_member = _make_member(bhumi, "member@bhumi.org", GUEST_ROLE)
    payload = AdminChangeRoleSchema(role_uuid=_role(ANALYST_ROLE).uuid)
    with pytest.raises(HttpError) as excinfo:
        # ask for the Bhumi member via the Akshara path
        put_admin_org_user_role(platform_admin_request, akshara.id, bhumi_member.id, payload)
    assert excinfo.value.status_code == 404


# ---- per-org deactivate: isolation --------------------------------------------


def test_admin_deactivate_user_in_org_only(platform_admin_request, akshara, bhumi):
    """
    DEACTIVATION SYMMETRY (acceptance): Priya is in both Akshara and Bhumi.
    Deactivating her in Akshara sets ONLY the Akshara OrgUser inactive — her Bhumi
    OrgUser stays active and the global User.is_active is untouched. Mirrors the M3
    org-symmetry test.
    """
    priya_user = User.objects.create(username="priya@ngo.org", email="priya@ngo.org")
    akshara_ou = OrgUser.objects.create(user=priya_user, org=akshara, new_role=_role(GUEST_ROLE))
    bhumi_ou = OrgUser.objects.create(user=priya_user, org=bhumi, new_role=_role(GUEST_ROLE))

    response = post_admin_org_user_deactivate(platform_admin_request, akshara.id, akshara_ou.id)

    akshara_ou.refresh_from_db()
    bhumi_ou.refresh_from_db()
    priya_user.refresh_from_db()
    assert akshara_ou.is_active is False  # deactivated HERE
    assert response.is_active is False
    assert bhumi_ou.is_active is True  # untouched in the other org
    assert priya_user.is_active is True  # global flag never touched

    # reactivation flips only the Akshara row back
    post_admin_org_user_reactivate(platform_admin_request, akshara.id, akshara_ou.id)
    akshara_ou.refresh_from_db()
    bhumi_ou.refresh_from_db()
    assert akshara_ou.is_active is True
    assert bhumi_ou.is_active is True


# ---- removal-impact count + orphaning -----------------------------------------


def test_admin_removal_impact_counts_are_accurate(platform_admin_request, akshara):
    """
    REMOVAL-IMPACT COUNT (plan §4.6 / research §5): the endpoint returns the exact
    number of dashboards/charts/reports that removal would orphan (their created_by set
    to NULL — kept, not deleted) — counted against real rows tied to the user via
    created_by, and scoped to THAT user (content by another user is not counted).
    """
    priya = _make_member(akshara, "priya@akshara.org", GUEST_ROLE)
    other = _make_member(akshara, "other@akshara.org", GUEST_ROLE)

    for i in range(3):
        Dashboard.objects.create(title=f"d{i}", org=akshara, created_by=priya)
    for i in range(5):
        Chart.objects.create(
            title=f"c{i}",
            chart_type="bar",
            schema_name="s",
            table_name="t",
            org=akshara,
            created_by=priya,
        )
    for i in range(2):
        ReportSnapshot.objects.create(title=f"r{i}", org=akshara, created_by=priya)

    # content owned by a DIFFERENT user must not be counted
    Dashboard.objects.create(title="not-priyas", org=akshara, created_by=other)

    impact = get_admin_org_user_removal_impact(platform_admin_request, akshara.id, priya.id)
    assert impact.dashboards_orphaned == 3
    assert impact.charts_orphaned == 5
    assert impact.reports_orphaned == 2


def test_admin_remove_user_orphans_content(platform_admin_request, akshara):
    """
    removing the user hard-deletes the OrgUser but KEEPS their Dashboards/Charts/Reports
    — all three created_by FKs are SET_NULL (Access Control v2 / PR #1428 switched
    Dashboard & Chart from CASCADE to SET_NULL; ReportSnapshot already was). The content
    survives with created_by=None; the removal-impact count is the orphan count.
    """
    priya = _make_member(akshara, "priya2@akshara.org", GUEST_ROLE)
    dash = Dashboard.objects.create(title="d", org=akshara, created_by=priya)
    chart = Chart.objects.create(
        title="c",
        chart_type="bar",
        schema_name="s",
        table_name="t",
        org=akshara,
        created_by=priya,
    )
    report = ReportSnapshot.objects.create(title="r", org=akshara, created_by=priya)
    priya_id = priya.id

    delete_admin_org_user(platform_admin_request, akshara.id, priya.id)

    assert not OrgUser.objects.filter(id=priya_id).exists()
    # content is KEPT (not cascade-deleted); only the creator link is cleared
    dash.refresh_from_db()
    chart.refresh_from_db()
    report.refresh_from_db()
    assert dash.created_by is None  # orphaned, not deleted
    assert chart.created_by is None  # orphaned, not deleted
    assert report.created_by is None  # orphaned, not deleted


# ---- org-scoped cancel invite -------------------------------------------------


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_admin_cancel_invite_is_org_scoped(platform_admin_request, akshara, bhumi):
    """
    ORG-SCOPED CANCEL (plan §8 / research §8): a Bhumi invitation cannot be cancelled
    through the Akshara path — the endpoint scopes by invited_in_org, so a wrong-org id
    yields 404 and the invite survives. Cancelling through the correct org succeeds.
    This is the fix for the loose global DELETE /users/invitations/delete/{id}.
    """
    # an invitation that belongs to Bhumi
    payload = AdminInviteUserSchema(
        invited_email="pending@bhumi.org", invited_role_uuid=_role(GUEST_ROLE).uuid
    )
    post_admin_org_user_invite(platform_admin_request, bhumi.id, payload)
    inv = Invitation.objects.filter(invited_email="pending@bhumi.org").first()
    assert inv.invited_in_org_id == bhumi.id

    # cancelling via ANOTHER org (Akshara) must fail with 404 and leave it intact
    with pytest.raises(HttpError) as excinfo:
        delete_admin_org_invitation(platform_admin_request, akshara.id, inv.id)
    assert excinfo.value.status_code == 404
    assert Invitation.objects.filter(id=inv.id).exists()

    # cancelling via the correct org (Bhumi) succeeds
    delete_admin_org_invitation(platform_admin_request, bhumi.id, inv.id)
    assert not Invitation.objects.filter(id=inv.id).exists()


# ---- users list ---------------------------------------------------------------


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
def test_admin_org_users_lists_members_and_pending(platform_admin_request, akshara):
    """the Users tab payload lists members (with per-org status) and pending invites"""
    member = _make_member(akshara, "member@akshara.org", GUEST_ROLE)
    post_admin_org_user_invite(
        platform_admin_request,
        akshara.id,
        AdminInviteUserSchema(
            invited_email="pending@akshara.org", invited_role_uuid=_role(GUEST_ROLE).uuid
        ),
    )

    response = get_admin_org_users(platform_admin_request, akshara.id)

    emails = {u.email for u in response.users}
    assert "member@akshara.org" in emails
    assert all(u.is_active for u in response.users)
    invited_emails = {i.invited_email for i in response.invitations}
    assert "pending@akshara.org" in invited_emails


# ============================================================================
# Week 1 — full admin lifecycle flow (one continuous story, real DB state)
# ============================================================================
# This is NOT a per-milestone unit test. It runs the whole super-admin journey —
# create org, invite, accept, change role, per-org + org-level deactivate/reactivate,
# cancel invite, remove-with-orphaning — as ONE narrative against real rows, so it proves
# the milestones COMPOSE, not just that each works in isolation. External deps only are
# mocked: Airbyte (org create), Redis (unavailable in tests), and SES creds forced
# absent so the invite exercises the real Part-1 dev fallback rather than a mocked email.


def _load_permissions(user, org_slug):
    """
    Run the REAL CustomJwtAuthMiddleware for (user, org_slug) against REAL DB rows,
    mocking only Redis (unavailable in tests). Returns the authenticated request on
    success; raises HttpError exactly where the app enforces org / per-org-user
    deactivation at permission-load (auth.py:173 / :183). This is how the flow proves a
    user is BLOCKED or ALLOWED — real enforcement, not a flag read.
    """
    request = Mock()
    request.headers = {"x-dalgo-org": org_slug}
    token = str(AccessToken.for_user(user))
    with patch("ddpui.auth.RedisClient.get_instance") as mock_redis, patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    ):
        mock_redis.return_value.get.return_value = None
        return CustomJwtAuthMiddleware().authenticate(request, token)


def _assert_blocked(user, org_slug, expected_message):
    """the middleware refuses this (user, org) at permission-load with a 403"""
    with pytest.raises(HttpError) as excinfo:
        _load_permissions(user, org_slug)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == expected_message


@patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_week1_full_admin_lifecycle_flow(
    mock_setup_airbyte, mock_connectors, platform_admin_request, seed_db
):
    """
    The whole Week-1 super-admin story end to end, on real DB state:

      1. admin creates an org via the portal (active, ZERO members)
      2. admin invites a user by email + role — succeeds with NO real SES (Part 1)
      3. the user accepts — becomes an OrgUser of the target org at the right role
      4. admin changes the user's role
      5. admin deactivates the user IN THIS ORG ONLY — blocked here, untouched elsewhere
      6. admin reactivates the user — access restored
      7. admin deactivates the ORG itself — user blocked despite being active
      8. admin reactivates the org — access restored
      9. admin invites a 2nd user then cancels it; a DIFFERENT org's invite can't be
         cancelled through this org's path (404) — org-scoping holds inside the flow
     10. admin removes the first user — orphan-impact count available first, then their
         content is orphaned (created_by NULLed, kept), not cascade-deleted

    Blocking is proven through the real auth middleware (see _load_permissions).
    """
    admin_request = platform_admin_request

    # -- Step 1: create an org via the admin portal --------------------------------
    mock_setup_airbyte.return_value = Mock(workspaceId="ws-akshara")
    created = post_admin_org(admin_request, AdminCreateOrgSchema(name="Akshara"))
    assert created.is_active is True
    assert created.user_count == 0  # a freshly created org has ZERO members
    org1 = Org.objects.get(id=created.id)
    assert OrgUser.objects.filter(org=org1).count() == 0

    # -- Step 2: invite a user — succeeds WITHOUT real SES (proves Part 1) ----------
    # DEBUG=True + no SES creds routes send_invite_user_email through the dev fallback,
    # so the invite completes and the Invitation row is created without any email mock.
    with patch("ddpui.utils.awsses.settings.DEBUG", True), patch(
        "ddpui.utils.awsses._ses_available", return_value=False
    ):
        post_admin_org_user_invite(
            admin_request,
            org1.id,
            AdminInviteUserSchema(
                invited_email="priya@akshara.org",
                invited_role_uuid=_role(ANALYST_ROLE).uuid,
            ),
        )
    invite = Invitation.objects.get(invited_email="priya@akshara.org")
    assert invite.invited_in_org_id == org1.id  # the TARGET org, not the admin's org
    assert invite.invited_by.org_id != org1.id  # admin is not a member of Akshara
    assert invite.invited_new_role.slug == ANALYST_ROLE

    # -- Step 3: the invited user accepts ------------------------------------------
    _, error = orguserfunctions.accept_invitation_v1(
        AcceptInvitationSchema(invite_code=invite.invite_code, password="Priya@12345")
    )
    assert error is None
    priya_ou = OrgUser.objects.get(user__email="priya@akshara.org", org=org1)
    priya_user = priya_ou.user
    assert priya_ou.new_role.slug == ANALYST_ROLE  # accepted at the invited role
    assert not Invitation.objects.filter(id=invite.id).exists()  # invite consumed

    # -- Step 4: admin changes the user's role -------------------------------------
    put_admin_org_user_role(
        admin_request,
        org1.id,
        priya_ou.id,
        AdminChangeRoleSchema(role_uuid=_role(ACCOUNT_MANAGER_ROLE).uuid),
    )
    priya_ou.refresh_from_db()
    assert priya_ou.new_role.slug == ACCOUNT_MANAGER_ROLE

    # -- Step 5: per-org deactivate — isolation across a SECOND org -----------------
    # Priya is also a member of a second org. Deactivating her in org1 must not touch
    # org2 at all. (Real second Org + OrgUser row, per the brief.)
    org2 = Org.objects.create(name="Bhumi", slug="bhumi")
    priya_ou2 = OrgUser.objects.create(user=priya_user, org=org2, new_role=_role(GUEST_ROLE))
    # sanity: before any deactivation she can load permissions in BOTH orgs
    assert _load_permissions(priya_user, org1.slug).orguser.id == priya_ou.id
    assert _load_permissions(priya_user, org2.slug).orguser.id == priya_ou2.id

    post_admin_org_user_deactivate(admin_request, org1.id, priya_ou.id)
    priya_ou.refresh_from_db()
    priya_ou2.refresh_from_db()
    priya_user.refresh_from_db()
    assert priya_ou.is_active is False
    assert priya_ou2.is_active is True  # the OTHER org is untouched
    assert priya_user.is_active is True  # the global User flag is never touched
    # (a) blocked in org1 at permission-load
    _assert_blocked(priya_user, org1.slug, "your access to this organization has been deactivated")
    # (b) org2 access completely unaffected
    assert _load_permissions(priya_user, org2.slug).orguser.id == priya_ou2.id

    # -- Step 6: reactivate the user — access restored in org1 ---------------------
    post_admin_org_user_reactivate(admin_request, org1.id, priya_ou.id)
    priya_ou.refresh_from_db()
    assert priya_ou.is_active is True
    assert _load_permissions(priya_user, org1.slug).orguser.id == priya_ou.id

    # -- Step 7: deactivate the ORG itself — user blocked despite being active ------
    post_admin_org_deactivate(admin_request, org1.id)
    org1.refresh_from_db()
    priya_ou.refresh_from_db()
    assert org1.is_active is False
    assert priya_ou.is_active is True  # the user is active; the ORG is what blocks now
    _assert_blocked(priya_user, org1.slug, "your organization has been deactivated")
    # org2 still fine — this org's deactivation is scoped to this org
    assert _load_permissions(priya_user, org2.slug).orguser.id == priya_ou2.id

    # -- Step 8: reactivate the org — access restored again ------------------------
    post_admin_org_reactivate(admin_request, org1.id)
    org1.refresh_from_db()
    assert org1.is_active is True
    assert _load_permissions(priya_user, org1.slug).orguser.id == priya_ou.id

    # -- Step 9: invite a second user, then cancel; cross-org cancel is 404 --------
    with patch("ddpui.utils.awsses.settings.DEBUG", True), patch(
        "ddpui.utils.awsses._ses_available", return_value=False
    ):
        post_admin_org_user_invite(
            admin_request,
            org1.id,
            AdminInviteUserSchema(
                invited_email="raj@akshara.org", invited_role_uuid=_role(GUEST_ROLE).uuid
            ),
        )
    raj_invite = Invitation.objects.get(invited_email="raj@akshara.org")
    delete_admin_org_invitation(admin_request, org1.id, raj_invite.id)
    assert not Invitation.objects.filter(id=raj_invite.id).exists()  # cancelled

    # a pending invite that belongs to org2 must NOT be cancellable via org1's path
    with patch("ddpui.utils.awsses.settings.DEBUG", True), patch(
        "ddpui.utils.awsses._ses_available", return_value=False
    ):
        post_admin_org_user_invite(
            admin_request,
            org2.id,
            AdminInviteUserSchema(
                invited_email="pending@bhumi.org",
                invited_role_uuid=_role(GUEST_ROLE).uuid,
            ),
        )
    bhumi_invite = Invitation.objects.get(invited_email="pending@bhumi.org")
    with pytest.raises(HttpError) as excinfo:
        delete_admin_org_invitation(admin_request, org1.id, bhumi_invite.id)
    assert excinfo.value.status_code == 404
    assert Invitation.objects.filter(id=bhumi_invite.id).exists()  # survived, org-scoped

    # -- Step 10: removal-impact THEN remove the first user (content is orphaned) ---
    # content owned by Priya's org1 membership — removal orphans it (created_by→NULL,
    # kept), it is NOT cascade-deleted (Access Control v2 / PR #1428).
    dashboards = [
        Dashboard.objects.create(title=f"d{i}", org=org1, created_by=priya_ou) for i in range(2)
    ]
    charts = [
        Chart.objects.create(
            title=f"c{i}",
            chart_type="bar",
            schema_name="s",
            table_name="t",
            org=org1,
            created_by=priya_ou,
        )
        for i in range(3)
    ]
    report = ReportSnapshot.objects.create(title="r", org=org1, created_by=priya_ou)

    # the orphan-impact count is available BEFORE the remove
    impact = get_admin_org_user_removal_impact(admin_request, org1.id, priya_ou.id)
    assert impact.dashboards_orphaned == 2
    assert impact.charts_orphaned == 3
    assert impact.reports_orphaned == 1

    priya_ou_id = priya_ou.id
    delete_admin_org_user(admin_request, org1.id, priya_ou.id)

    assert not OrgUser.objects.filter(id=priya_ou_id).exists()  # removed from org1
    # content is KEPT, orphaned (created_by SET_NULL) — not cascade-deleted
    for dash in dashboards:
        dash.refresh_from_db()
        assert dash.created_by is None  # orphaned, not deleted
    for chart in charts:
        chart.refresh_from_db()
        assert chart.created_by is None  # orphaned, not deleted
    report.refresh_from_db()
    assert report.created_by is None  # orphaned (SET_NULL), not deleted
    # the remove is org-scoped: Priya's Bhumi membership is untouched
    assert OrgUser.objects.filter(id=priya_ou2.id).exists()
    assert _load_permissions(priya_user, org2.slug).orguser.id == priya_ou2.id


# ---- the independent admin session: login / logout / refresh / currentuser ----
# Moved here from tests/core/test_admin_auth.py: these exercise admin_api view
# functions, so they belong with the other API tests. The middleware unit tests
# stay in tests/core/test_admin_auth.py.


def _mock_auth_redis():
    """mock Redis for the token mint, as the existing auth tests do"""
    return patch("ddpui.auth.RedisClient.get_instance"), patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    )


def test_admin_login_refuses_non_platform_admin():
    """Correct password but not a platform admin -> 403, and no cookie is set."""
    User.objects.create_user(username="ops@dalgo.org", email="ops@dalgo.org", password="Secret@123")
    with pytest.raises(HttpError) as excinfo:
        post_admin_login(
            mock_request(), AdminLoginSchema(username="ops@dalgo.org", password="Secret@123")
        )
    assert excinfo.value.status_code == 403


def test_admin_login_wrong_password_is_401():
    """Wrong password -> 401."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)
    with pytest.raises(HttpError) as excinfo:
        post_admin_login(
            mock_request(), AdminLoginSchema(username="admin@dalgo.org", password="nope")
        )
    assert excinfo.value.status_code == 401


def test_admin_login_sets_admin_cookies_for_platform_admin():
    """A platform admin gets admin_access_token + admin_refresh_token cookies."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_auth_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        response = post_admin_login(
            mock_request(), AdminLoginSchema(username="admin@dalgo.org", password="Secret@123")
        )

    assert response.status_code == 200
    assert "admin_access_token" in response.cookies
    assert "admin_refresh_token" in response.cookies


def test_admin_logout_clears_admin_cookies(platform_admin_request):
    """Admin logout deletes only the admin_* cookies (independent of the normal session)."""
    platform_admin_request.COOKIES = {}
    response = post_admin_logout(platform_admin_request)
    assert response.status_code == 200
    assert response.cookies["admin_access_token"].value == ""
    assert response.cookies["admin_refresh_token"].value == ""


def test_admin_logout_forbidden_for_non_platform_admin(orguser):
    """logout is gated like every other admin route — a non-admin is refused."""
    request = mock_request(orguser)
    request.COOKIES = {}
    with pytest.raises(HttpError) as excinfo:
        post_admin_logout(request)
    assert excinfo.value.status_code == 403


def test_admin_currentuser_reports_platform_admin(platform_admin_request):
    """currentuser returns the admin's email + is_platform_admin, via the admin session."""
    result = get_admin_currentuser(platform_admin_request)
    assert result["is_platform_admin"] is True
    assert result["email"] == platform_admin_request.orguser.user.email


def test_admin_token_refresh_without_cookie_is_401():
    request = mock_request()
    request.COOKIES = {}
    with pytest.raises(HttpError) as excinfo:
        post_admin_token_refresh(request)
    assert excinfo.value.status_code == 401


def test_admin_token_refresh_issues_new_admin_access():
    """A valid admin refresh token yields a new admin_access_token carrying session='admin'."""
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    redis_patch, roles_patch = _mock_auth_redis()
    with redis_patch as mock_redis, roles_patch:
        mock_redis.return_value.get.return_value = None
        token_data, _ = admin_service.issue_admin_session("admin@dalgo.org", "Secret@123")

    request = mock_request()
    request.COOKIES = {"admin_refresh_token": token_data["refresh"]}
    # the blacklist lookup now lives in the service, so patch it at its import site
    with patch("ddpui.core.admin.admin_service.RedisClient.get_instance") as mock_redis2:
        mock_redis2.return_value.get.return_value = None
        response = post_admin_token_refresh(request)

    assert "admin_access_token" in response.cookies
    access = AccessToken(response.cookies["admin_access_token"].value)
    assert access["session"] == "admin"


def test_admin_router_requires_admin_session():
    """
    Router-level auth: the admin router is guarded by AdminJwtAuthMiddleware, and the
    only routes opting out are the two that cannot require a session you don't have yet.

    Asserted by introspecting the router rather than issuing HTTP — this repo's testing
    skill calls view functions directly and does not use ninja's TestClient.
    """
    assert isinstance(admin_router.auth, AdminJwtAuthMiddleware)

    # auth_param is NOT_SET when a route inherits the router's auth, and None when a
    # route explicitly opts out via auth=None.
    opted_out = {
        path
        for path, view in admin_router.path_operations.items()
        for op in view.operations
        if op.auth_param is None
    }
    assert opted_out == {"/login/", "/token/refresh"}


def test_admin_session_rejected_without_admin_cookie():
    """No admin_access_token cookie and no bearer header -> the middleware does not
    authenticate, so ninja answers 401."""
    request = Mock()
    request.COOKIES = {}
    request.headers = {}
    assert AdminJwtAuthMiddleware()(request) is None
