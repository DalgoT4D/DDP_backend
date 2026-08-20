"""
Tests for the Admin Portal API and its platform-admin gate.

Milestone 1 acceptance (features/admin-portal/plan.md §6, §7):
  - non-platform-admin -> 403 on a guarded admin route
  - platform admin      -> 200 on the same route
  - /currentuserv2 surfaces is_platform_admin
"""

import json
import os
from unittest.mock import Mock, patch

import django
import pytest
from ninja.errors import HttpError
from ninja.constants import NOT_SET

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.admin_api import (
    get_admin_stats,
    get_admin_orgs,
    post_admin_org,
    get_admin_org,
    put_admin_org,
    get_admin_org_delete_impact,
    delete_admin_org,
    get_admin_org_users,
    post_admin_org_user_invite,
    put_admin_org_user_role,
    get_admin_org_user_removal_impact,
    delete_admin_org_user,
    delete_admin_org_invitation,
    admin_router,
    get_admin_currentuser,
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
    AdminChangeRoleSchema,
)
from ddpui.services.org_cleanup_service import OrgCleanupServiceError
from ddpui.routes import drf_authentication_failed_handler
from rest_framework.exceptions import AuthenticationFailed
from ddpui.api.user_org_api import (
    get_current_user_v2,
    post_organization_user_invite_v1,
    post_login_v2,
)
from ddpui.core import orguserfunctions
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import (
    OrgUser,
    UserAttributes,
    Invitation,
    NewInvitationSchema,
    AcceptInvitationSchema,
    LoginPayload,
)
from ddpui.models.role_based_access import Role
from ddpui.models.dashboard import Dashboard
from ddpui.models.visualization import Chart
from ddpui.models.report import ReportSnapshot
from ddpui.auth import (
    ACCOUNT_MANAGER_ROLE,
    SUPER_ADMIN_ROLE,
    ANALYST_ROLE,
    GUEST_ROLE,
)
from ddpui.core.admin import admin_service
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


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


# ---- the guard: /admin/currentuser 403 vs 200 ---------------------------------


def test_admin_currentuser_forbidden_for_non_platform_admin(orguser):
    """a user without is_platform_admin is refused with 403"""
    request = mock_request(orguser)
    # no UserAttributes row at all -> not a platform admin
    with pytest.raises(HttpError) as excinfo:
        get_admin_currentuser(request)
    assert excinfo.value.status_code == 403
    assert str(excinfo.value) == "platform admin access required"


def test_admin_currentuser_forbidden_when_flag_false(orguser):
    """a user whose is_platform_admin is explicitly False is refused with 403"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=False)
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_currentuser(request)
    assert excinfo.value.status_code == 403


def test_admin_currentuser_ok_for_platform_admin(orguser):
    """a platform admin gets 200"""
    UserAttributes.objects.create(user=orguser.user, is_platform_admin=True)
    request = mock_request(orguser)
    response = get_admin_currentuser(request)
    assert response == {"email": orguser.user.email, "is_platform_admin": True}


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


# ---- org lifecycle: list / create / detail / edit -----------------------------


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
    """lists every org"""
    Org.objects.create(name="Alpha Org", slug="alpha-org")
    Org.objects.create(name="Beta Org", slug="beta-org")
    response = get_admin_orgs(platform_admin_request)
    by_name = {o.name: o for o in response}
    assert "Alpha Org" in by_name
    assert "Beta Org" in by_name


@patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_admin_create_org_happy_path(mock_setup_airbyte, mock_connectors, platform_admin_request):
    """create org: Org + OrgPlans created; Airbyte workspace provisioned once"""
    mock_setup_airbyte.return_value = Mock(workspaceId="ws-abc")
    payload = AdminCreateOrgSchema(name="Bhumi")

    response = post_admin_org(platform_admin_request, payload)

    assert response.name == "Bhumi"
    assert response.slug == "bhumi"
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


@patch("ddpui.core.admin.admin_service.airbyte_service.delete_workspace")
@patch("ddpui.core.orgfunctions.create_org_plan")
@patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_admin_create_org_rolls_back_when_plan_creation_fails(
    mock_setup_airbyte,
    mock_connectors,
    mock_create_plan,
    mock_delete_workspace,
    platform_admin_request,
):
    """
    If the org is created but its plan fails, the Org DB row must roll back (else a
    half-created org with no plan shows up in the portal) AND the Airbyte workspace
    already provisioned for it must be explicitly deleted, since the DB rollback can't
    reach that external side effect.
    """
    mock_setup_airbyte.return_value = Mock(workspaceId="ws-abc")
    mock_create_plan.return_value = (None, "could not create plan")

    with pytest.raises(HttpError) as excinfo:
        post_admin_org(platform_admin_request, AdminCreateOrgSchema(name="Halfway"))

    assert excinfo.value.status_code == 400
    assert Org.objects.filter(name="Halfway").count() == 0  # rolled back, no orphan
    assert OrgPlans.objects.filter(org__name="Halfway").count() == 0
    mock_delete_workspace.assert_called_once()  # orphaned Airbyte workspace cleaned up


def test_admin_org_detail_404(platform_admin_request):
    """detail of a missing org is 404"""
    with pytest.raises(HttpError) as excinfo:
        get_admin_org(platform_admin_request, 999999)
    assert excinfo.value.status_code == 404


def test_admin_edit_org_locks_slug(platform_admin_request):
    """edit updates name + viz_url but never the slug (locked post-create)"""
    org = Org.objects.create(name="Old Name", slug="old-name")
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


# ---- org lifecycle: delete ------------------------------------------------------


def test_admin_org_delete_impact_forbidden_for_non_platform_admin(orguser):
    """the delete-impact route is gated too — non-admin gets 403"""
    org = Org.objects.create(name="Impact Org", slug="impact-org")
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_org_delete_impact(request, org.id)
    assert excinfo.value.status_code == 403


def test_admin_org_delete_impact_404(platform_admin_request):
    """delete-impact for a missing org is 404"""
    with pytest.raises(HttpError) as excinfo:
        get_admin_org_delete_impact(platform_admin_request, 999999)
    assert excinfo.value.status_code == 404


def test_admin_org_delete_impact_counts(platform_admin_request):
    """delete-impact reports the real counts of what a delete would destroy"""
    org = Org.objects.create(name="Impact Org", slug="impact-org")
    user = User.objects.create(username="impact-user", email="impact-user@example.com")
    OrgUser.objects.create(
        user=user, org=org, new_role=Role.objects.filter(slug=GUEST_ROLE).first()
    )
    Dashboard.objects.create(org=org, title="D1", created_by=None)
    Chart.objects.create(
        org=org,
        title="C1",
        chart_type="bar",
        schema_name="s",
        table_name="t",
        created_by=None,
    )

    response = get_admin_org_delete_impact(platform_admin_request, org.id)

    assert response.user_count == 1
    assert response.warehouse_count == 0
    assert response.connection_count == 0
    assert response.pipeline_count == 0
    assert response.dashboard_count == 1
    assert response.chart_count == 1
    assert response.report_count == 0


def test_admin_delete_org_forbidden_for_non_platform_admin(orguser):
    """the delete route is gated too — non-admin gets 403"""
    org = Org.objects.create(name="Delete-Guard Org", slug="delete-guard-org")
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        delete_admin_org(request, org.id)
    assert excinfo.value.status_code == 403


def test_admin_delete_org_404(platform_admin_request):
    """deleting a missing org is 404"""
    with pytest.raises(HttpError) as excinfo:
        delete_admin_org(platform_admin_request, 999999)
    assert excinfo.value.status_code == 404


@patch("ddpui.core.admin.admin_service.OrgCleanupService")
def test_admin_delete_org_happy_path(mock_cleanup_service_cls, platform_admin_request):
    """delete runs OrgCleanupService for real (dry_run=False) and removes the Org row"""
    org = Org.objects.create(name="Delete Me", slug="delete-me")
    org_id = org.id

    response = delete_admin_org(platform_admin_request, org_id)

    mock_cleanup_service_cls.assert_called_once_with(org, dry_run=False)
    mock_cleanup_service_cls.return_value.delete_org.assert_called_once()
    assert response == {"success": 1}
    # the real org.delete() only happens inside OrgCleanupService.delete_org, which is
    # mocked here — so this confirms the wiring, not the cascade (that's
    # test_org_cleanup_service.py's job)
    assert Org.objects.filter(id=org_id).exists()


@patch("ddpui.core.admin.admin_service.OrgCleanupService")
def test_admin_delete_org_propagates_cleanup_error_as_400(
    mock_cleanup_service_cls, platform_admin_request
):
    """an OrgCleanupServiceError (e.g. a transform task still used by a pipeline) is
    surfaced as a 400, not a 500"""
    org = Org.objects.create(name="Stuck Org", slug="stuck-org")
    mock_cleanup_service_cls.return_value.delete_org.side_effect = OrgCleanupServiceError(
        "org_task is being used in a deployment"
    )

    with pytest.raises(HttpError) as excinfo:
        delete_admin_org(platform_admin_request, org.id)

    assert excinfo.value.status_code == 400
    assert Org.objects.filter(id=org.id).exists()  # nothing deleted on failure


# ============================================================================
# Milestone 4 — Users tab: invite / role / remove / cancel invite
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
    payload = NewInvitationSchema(
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
    payload = NewInvitationSchema(
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
    payload = NewInvitationSchema(invited_email="x@y.org", invited_role_uuid=_role(GUEST_ROLE).uuid)
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


def test_admin_user_routes_refuse_an_orguser_from_another_org(
    platform_admin_request, akshara, bhumi
):
    """
    CROSS-ORG ISOLATION: every user route resolves the OrgUser through the target org in
    the URL, so passing a Bhumi orguser_id down Akshara's path is 404 — never a silent
    mutation of the other org's user. The role route has its own test; these two are the
    remaining mutating/reading routes that take an orguser_id.
    """
    bhumi_member = _make_member(bhumi, "victim@bhumi.org", GUEST_ROLE)

    for call in (
        lambda: get_admin_org_user_removal_impact(
            platform_admin_request, akshara.id, bhumi_member.id
        ),
        lambda: delete_admin_org_user(platform_admin_request, akshara.id, bhumi_member.id),
    ):
        with pytest.raises(HttpError) as excinfo:
            call()
        assert excinfo.value.status_code == 404

    # the other org's user is untouched by either attempt
    assert OrgUser.objects.filter(id=bhumi_member.id).exists()


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
    payload = NewInvitationSchema(
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
    """the Users tab payload lists members and pending invites"""
    member = _make_member(akshara, "member@akshara.org", GUEST_ROLE)
    post_admin_org_user_invite(
        platform_admin_request,
        akshara.id,
        NewInvitationSchema(
            invited_email="pending@akshara.org", invited_role_uuid=_role(GUEST_ROLE).uuid
        ),
    )

    response = get_admin_org_users(platform_admin_request, akshara.id)

    emails = {u.email for u in response.users}
    assert "member@akshara.org" in emails
    invited_emails = {i.invited_email for i in response.invitations}
    assert "pending@akshara.org" in invited_emails


# ============================================================================
# Week 1 — full admin lifecycle flow (one continuous story, real DB state)
# ============================================================================
# This is NOT a per-milestone unit test. It runs the whole super-admin journey —
# create org, invite, accept, change role, cancel invite, remove-with-orphaning — as ONE
# narrative against real rows, so it proves
# the milestones COMPOSE, not just that each works in isolation. External deps only are
# mocked: Airbyte (org create), Redis (unavailable in tests), and SES (email sending).


@patch("ddpui.utils.awsses.send_invite_user_email", Mock())
@patch("ddpui.core.orgfunctions.add_custom_connectors_to_workspace")
@patch("ddpui.core.orgfunctions.airbytehelpers.setup_airbyte_workspace_v1")
def test_week1_full_admin_lifecycle_flow(
    mock_setup_airbyte, mock_connectors, platform_admin_request, seed_db
):
    """
    The whole Week-1 super-admin story end to end, on real DB state:

      1. admin creates an org via the portal (ZERO members)
      2. admin invites a user by email + role (email sending mocked)
      3. the user accepts — becomes an OrgUser of the target org at the right role
      4. admin changes the user's role
      5. admin invites a 2nd user then cancels it; a DIFFERENT org's invite can't be
         cancelled through this org's path (404) — org-scoping holds inside the flow
      6. admin removes the first user — orphan-impact count available first, then their
         content is orphaned (created_by NULLed, kept), not cascade-deleted
    """
    admin_request = platform_admin_request

    # -- Step 1: create an org via the admin portal --------------------------------
    mock_setup_airbyte.return_value = Mock(workspaceId="ws-akshara")
    created = post_admin_org(admin_request, AdminCreateOrgSchema(name="Akshara"))
    assert created.user_count == 0  # a freshly created org has ZERO members
    org1 = Org.objects.get(id=created.id)
    assert OrgUser.objects.filter(org=org1).count() == 0

    # -- Step 2: invite a user (email sending mocked at the decorator level) --------
    post_admin_org_user_invite(
        admin_request,
        org1.id,
        NewInvitationSchema(
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

    # a second org that Priya also belongs to — used below to prove removal is org-scoped
    org2 = Org.objects.create(name="Bhumi", slug="bhumi")
    priya_ou2 = OrgUser.objects.create(user=priya_user, org=org2, new_role=_role(GUEST_ROLE))

    # -- Step 5: invite a second user, then cancel; cross-org cancel is 404 --------
    post_admin_org_user_invite(
        admin_request,
        org1.id,
        NewInvitationSchema(
            invited_email="raj@akshara.org", invited_role_uuid=_role(GUEST_ROLE).uuid
        ),
    )
    raj_invite = Invitation.objects.get(invited_email="raj@akshara.org")
    delete_admin_org_invitation(admin_request, org1.id, raj_invite.id)
    assert not Invitation.objects.filter(id=raj_invite.id).exists()  # cancelled

    # a pending invite that belongs to org2 must NOT be cancellable via org1's path
    post_admin_org_user_invite(
        admin_request,
        org2.id,
        NewInvitationSchema(
            invited_email="pending@bhumi.org",
            invited_role_uuid=_role(GUEST_ROLE).uuid,
        ),
    )
    bhumi_invite = Invitation.objects.get(invited_email="pending@bhumi.org")
    with pytest.raises(HttpError) as excinfo:
        delete_admin_org_invitation(admin_request, org1.id, bhumi_invite.id)
    assert excinfo.value.status_code == 404
    assert Invitation.objects.filter(id=bhumi_invite.id).exists()  # survived, org-scoped

    # -- Step 6: removal-impact THEN remove the first user (content is orphaned) ----
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


# ---- router auth: no separate admin session ----------------------------------
# The admin portal signs in through the shared POST /api/v2/login/ and the admin
# router inherits the API-wide CustomJwtAuthMiddleware, exactly like every other
# router. Authority is @platform_admin_required on each route, so the guarantee worth
# holding is: a signed-in NON-admin is refused. Asserted by calling the view functions
# directly, per this repo's testing skill (no ninja TestClient).


def test_admin_router_has_no_separate_session_auth():
    """
    The admin router carries no auth of its own — it inherits the API-level
    CustomJwtAuthMiddleware. Regression guard for the removed AdminJwtAuthMiddleware:
    if someone re-binds a bespoke session onto this router, this fails.

    Asserted by introspecting the router rather than issuing HTTP, per the testing skill.
    """
    assert admin_router.auth is NOT_SET

    # No route opts out of auth either: the login/token-refresh routes that used
    # auth=None are gone, so nothing on the admin router is public.
    opted_out = {
        path
        for path, view in admin_router.path_operations.items()
        for op in view.operations
        if op.auth_param is None
    }
    assert opted_out == set()


def test_admin_currentuser_refuses_signed_in_non_platform_admin(orguser):
    """
    A perfectly valid NORMAL session is not enough to reach the admin API. The session
    is now shared, so the whole guarantee rests on @platform_admin_required: a signed-in
    user without the flag gets 403, not admin identity.
    """
    request = mock_request(orguser)
    with pytest.raises(HttpError) as excinfo:
        get_admin_currentuser(request)
    assert excinfo.value.status_code == 403


def test_admin_currentuser_reports_platform_admin(platform_admin_request):
    """A platform admin resolves identity — this is what AdminGuard reads."""
    result = get_admin_currentuser(platform_admin_request)
    assert result == {
        "email": platform_admin_request.orguser.user.email,
        "is_platform_admin": True,
    }


# ---- the contract the admin sign-in depends on -------------------------------


def test_v2_login_response_carries_is_platform_admin(seed_db):
    """
    The admin app signs in through the SHARED POST /api/v2/login/ and decides whether to
    admit the user from is_platform_admin in that response body. post_login_v2 has no
    `response=` schema (it must return a JsonResponse to set cookies), so nothing else
    pins this key — without this test, dropping it from lookup_user() would silently
    break the admin sign-in. See lookup_user() in ddpui/core/orguserfunctions.py.
    """
    user = User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )
    UserAttributes.objects.create(user=user, is_platform_admin=True)

    with patch("ddpui.auth.RedisClient.get_instance") as mock_redis, patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    ):
        mock_redis.return_value.get.return_value = None
        response = post_login_v2(
            mock_request(), LoginPayload(username="admin@dalgo.org", password="Secret@123")
        )

    body = json.loads(response.content)
    assert body["is_platform_admin"] is True
    assert body["email"] == "admin@dalgo.org"
    # and the shared session cookies are what got set — no admin_* cookie any more
    assert "access_token" in response.cookies
    assert "refresh_token" in response.cookies
    assert not any(name.startswith("admin_") for name in response.cookies)


def test_v2_login_reports_is_platform_admin_false_for_a_normal_user(seed_db):
    """
    The negative half of the contract. The shared login SUCCEEDS for any valid account —
    it does not know about platform admins — so a normal user must come back with
    is_platform_admin False, not a missing key and not an error. This is exactly what the
    admin sign-in form refuses on, so a regression here would silently let a non-admin
    into the admin shell (the backend would still 403 every route, but the UX breaks).
    """
    User.objects.create_user(username="ops@dalgo.org", email="ops@dalgo.org", password="Secret@123")
    # no UserAttributes row at all -> lookup_user creates one, defaulting the flag False

    with patch("ddpui.auth.RedisClient.get_instance") as mock_redis, patch(
        "ddpui.auth.set_roles_and_permissions_in_redis", return_value={}
    ):
        mock_redis.return_value.get.return_value = None
        response = post_login_v2(
            mock_request(), LoginPayload(username="ops@dalgo.org", password="Secret@123")
        )

    body = json.loads(response.content)
    assert "is_platform_admin" in body  # present, not merely falsy-by-absence
    assert body["is_platform_admin"] is False
    assert body["email"] == "ops@dalgo.org"
    # the sign-in still succeeded — cookies are set even though they are not an admin
    assert "access_token" in response.cookies


def test_v2_login_bad_credentials_maps_to_401_not_500():
    """
    Wrong password on the shared login raises DRF's AuthenticationFailed. These are
    ninja views, so DRF's own handler never runs and it used to fall through to the
    generic Exception handler as a 500. routes.drf_authentication_failed_handler maps it
    to the 401 it already declares — which is what the admin sign-in form renders.
    """
    User.objects.create_user(
        username="admin@dalgo.org", email="admin@dalgo.org", password="Secret@123"
    )

    with pytest.raises(AuthenticationFailed) as excinfo:
        post_login_v2(mock_request(), LoginPayload(username="admin@dalgo.org", password="nope"))

    # the handler registered in routes.py turns that exception into a 401 response
    response = drf_authentication_failed_handler(mock_request(), excinfo.value)
    assert response.status_code == 401
