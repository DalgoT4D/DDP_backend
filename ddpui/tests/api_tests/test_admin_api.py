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
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminInviteUserSchema,
    AdminChangeRoleSchema,
)
from ddpui.api.user_org_api import get_current_user_v2, post_organization_user_invite_v1
from ddpui.core import orguserfunctions
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import OrgUser, UserAttributes, Invitation, NewInvitationSchema
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.auth import (
    ACCOUNT_MANAGER_ROLE,
    SUPER_ADMIN_ROLE,
    ANALYST_ROLE,
    GUEST_ROLE,
)

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
    akshara_ou = OrgUser.objects.create(
        user=priya_user, org=akshara, new_role=_role(GUEST_ROLE)
    )
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


# ---- removal-impact count + cascade -------------------------------------------


def test_admin_removal_impact_counts_are_accurate(platform_admin_request, akshara):
    """
    REMOVAL-IMPACT COUNT (plan §4.6 / research §5): the endpoint returns the exact
    number of dashboards/charts that removal would cascade-delete and reports that
    would be orphaned — counted against real rows tied to the user via created_by,
    and scoped to THAT user (content by another user is not counted).
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
    assert impact.dashboards_deleted == 3
    assert impact.charts_deleted == 5
    assert impact.reports_orphaned == 2


def test_admin_remove_user_cascades_content(platform_admin_request, akshara):
    """
    removing the user hard-deletes the OrgUser and cascades their Dashboards/Charts
    (created_by CASCADE); their ReportSnapshots survive with created_by set NULL
    (SET_NULL). The count seen in the warning matches what actually disappears.
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
    assert not Dashboard.objects.filter(id=dash.id).exists()  # cascaded
    assert not Chart.objects.filter(id=chart.id).exists()  # cascaded
    report.refresh_from_db()
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
