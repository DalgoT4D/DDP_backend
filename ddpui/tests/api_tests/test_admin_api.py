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
    AdminCreateOrgSchema,
    AdminUpdateOrgSchema,
)
from ddpui.api.user_org_api import get_current_user_v2
from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans
from ddpui.models.org_user import OrgUser, UserAttributes
from ddpui.models.role_based_access import Role, RolePermission
from ddpui.auth import ACCOUNT_MANAGER_ROLE

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
