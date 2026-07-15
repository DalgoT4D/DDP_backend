"""Task 5: the sharing mutations — `/api/access/*` endpoints backed by
`sharing_actions.py`.

Route functions are called directly (as the rest of the API test suite does)
via `mock_request(orguser)`, which exercises the real permission machinery:
`request.permissions` is built from the seeded RolePermission rows, so the
dynamic share-slug gate behaves exactly as it would behind the middleware.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from unittest.mock import Mock, patch
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.core.orguserfunctions import accept_invitation_v1
from ddpui.core.sharing.access_resolver import effective_permission
from ddpui.core.sharing.shareable_types import RESOURCE_TYPES
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org
from ddpui.models.org_user import AcceptInvitationSchema, Invitation, OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.models.user_group import UserGroup, UserGroupMember, UserGroupMemberStatus
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Access Api Org", slug="access-api-org", airbyte_workspace_id="w1"
    )
    yield org
    # KPI.metric is PROTECT — delete KPIs before the Metric/Org CASCADE runs.
    KPI.objects.filter(org=org).delete()
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "accessapi-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "accessapi-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def analyst2(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "accessapi-analyst2")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "accessapi-member")
    yield ou
    ou.delete()


def _dashboard(org_obj, owner, audience=GeneralAudience.PRIVATE, level=GeneralLevel.VIEW):
    return Dashboard.objects.create(
        title="Access Api Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        general_audience=audience,
        general_level=level,
    )


def _grant(org_obj, rtype, resource, principal_orguser, permission="view", status="active"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=principal_orguser.id,
        permission=permission,
        status=status,
    )


# ================================================================================
# Registry: rtype -> share-permission-slug mapping is data, not if/else
# ================================================================================


def test_registry_maps_every_rtype_to_a_share_permission_slug():
    expected = {
        "dashboard": "can_share_dashboards",
        "report": "can_share_reports",
        "alert": "can_share_alerts",
        "metric": "can_share_metrics",
        "kpi": "can_share_kpis",
    }
    actual = {rtype: entry.share_permission_slug for rtype, entry in RESOURCE_TYPES.items()}
    assert actual == expected


# ================================================================================
# GET /api/access/{rtype}/{resource_id}/
# ================================================================================


class TestGetAccess:
    def test_overview_shows_owner_general_and_grants(self, org, analyst, analyst2, member):
        from ddpui.api.access_api import get_access

        dashboard = _dashboard(org, analyst, GeneralAudience.ANALYSTS_PLUS, GeneralLevel.VIEW)
        active = _grant(org, "dashboard", dashboard, member, permission="view")
        pending = ResourceShare.objects.create(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="user",
            principal_id=None,
            permission="view",
            status="pending",
            pending_email="future@test.com",
        )

        response = get_access(mock_request(analyst2), "dashboard", str(dashboard.pk))

        assert response["success"] is True
        data = response["data"]
        assert data["resource_type"] == "dashboard"
        assert data["resource_id"] == str(dashboard.pk)
        assert data["capabilities"] == {
            "general": True,
            "grants": True,
            "public_link": True,
            "requests": True,
        }
        assert data["owner"]["orguser_id"] == analyst.id
        assert data["owner"]["email"] == analyst.user.email
        assert data["general_access"] == {"audience": "analysts_plus", "level": "view"}
        by_id = {g["id"]: g for g in data["grants"]}
        assert by_id[active.id]["principal_type"] == "user"
        assert by_id[active.id]["principal_id"] == member.id
        assert by_id[active.id]["email"] == member.user.email
        assert by_id[active.id]["permission"] == "view"
        assert by_id[active.id]["status"] == "active"
        assert by_id[pending.id]["status"] == "pending"
        assert by_id[pending.id]["email"] == "future@test.com"
        assert data["viewer"] == {"effective_permission": "view", "is_owner": False}

    def test_overview_includes_group_grant_name_and_member_count(
        self, org, analyst, analyst2, member
    ):
        from ddpui.api.access_api import get_access

        dashboard = _dashboard(org, analyst)
        group = UserGroup.objects.create(org=org, name="Funders", created_by=analyst)
        UserGroupMember.objects.create(group=group, orguser=member, status="active")
        UserGroupMember.objects.create(group=group, orguser=analyst2, status="active")
        group_grant = ResourceShare.objects.create(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="group",
            principal_id=group.id,
            permission="view",
            status="active",
        )

        response = get_access(mock_request(analyst), "dashboard", str(dashboard.pk))
        by_id = {g["id"]: g for g in response["data"]["grants"]}
        assert by_id[group_grant.id]["principal_type"] == "group"
        assert by_id[group_grant.id]["principal_id"] == group.id
        assert by_id[group_grant.id]["name"] == "Funders"
        assert by_id[group_grant.id]["member_count"] == 2

    def test_member_with_view_can_read_overview_without_share_slug(self, org, analyst, member):
        """The GET is gated by resolver view only — no can_share_* slug."""
        from ddpui.api.access_api import get_access

        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS, GeneralLevel.VIEW)
        response = get_access(mock_request(member), "dashboard", str(dashboard.pk))
        assert response["success"] is True
        assert response["data"]["viewer"] == {"effective_permission": "view", "is_owner": False}

    def test_viewer_without_view_access_is_denied(self, org, analyst, member):
        from ddpui.api.access_api import get_access

        dashboard = _dashboard(org, analyst, GeneralAudience.PRIVATE)
        with pytest.raises(HttpError) as excinfo:
            get_access(mock_request(member), "dashboard", str(dashboard.pk))
        assert excinfo.value.status_code == 403

    def test_capability_flags_echoed_for_metric(self, org, analyst):
        from ddpui.api.access_api import get_access

        metric = Metric.objects.create(
            org=org,
            name="m1",
            schema_name="s",
            table_name="t",
            column="c",
            aggregation="sum",
            created_by=analyst,
            owner=analyst,
            general_audience=GeneralAudience.ANALYSTS_PLUS,
            general_level=GeneralLevel.VIEW,
        )
        response = get_access(mock_request(analyst), "metric", str(metric.pk))
        data = response["data"]
        assert data["capabilities"]["grants"] is False
        assert data["viewer"] == {"effective_permission": "edit", "is_owner": True}

    def test_unknown_rtype_404(self, org, admin):
        from ddpui.api.access_api import get_access

        with pytest.raises(HttpError) as excinfo:
            get_access(mock_request(admin), "chart", "1")
        assert excinfo.value.status_code == 404

    def test_cross_org_resource_404(self, org, admin, analyst):
        from ddpui.api.access_api import get_access

        other_org = Org.objects.create(name="Other Org", slug="access-other-org")
        other_admin = _make_orguser(other_org, ADMIN_ROLE, "accessapi-other-admin")
        dashboard = _dashboard(other_org, other_admin)
        with pytest.raises(HttpError) as excinfo:
            get_access(mock_request(admin), "dashboard", str(dashboard.pk))
        assert excinfo.value.status_code == 404


# ================================================================================
# POST /api/access/{rtype}/{resource_id}/grants/
# ================================================================================


def _post_grant(caller, rtype, resource, principal, permission="view", principal_type="user"):
    from ddpui.api.access_api import create_grant
    from ddpui.schemas.access_schema import GrantCreate

    payload = GrantCreate(
        principal_type=principal_type,
        principal_id=principal.id if hasattr(principal, "id") else principal,
        permission=permission,
    )
    return create_grant(mock_request(caller), rtype, str(resource.pk), payload)


def _post_grant_email(caller, rtype, resource, email, permission="view", invite_role=None):
    from ddpui.api.access_api import create_grant
    from ddpui.schemas.access_schema import GrantCreate

    payload = GrantCreate(
        principal_type="user", email=email, permission=permission, invite_role=invite_role
    )
    return create_grant(mock_request(caller), rtype, str(resource.pk), payload)


class TestCreateGrant:
    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_owner_grants_view_then_edit_updates_in_place(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)

        response = _post_grant(analyst, "dashboard", dashboard, member, "view")
        assert response["success"] is True
        assert response["data"]["principal_id"] == member.id
        assert response["data"]["permission"] == "view"
        assert response["data"]["status"] == "active"

        # duplicate grant for the same principal updates, doesn't stack
        response = _post_grant(analyst, "dashboard", dashboard, member, "edit")
        rows = ResourceShare.objects.filter(
            resource_type="dashboard", resource_id=str(dashboard.pk), principal_id=member.id
        )
        assert rows.count() == 1
        assert rows.first().permission == "edit"

    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_editor_via_grant_can_grant_up_to_edit(self, org, analyst, analyst2, member):
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="edit")

        response = _post_grant(analyst2, "dashboard", dashboard, member, "edit")
        assert response["data"]["permission"] == "edit"

    def test_viewer_level_sharer_is_blocked(self, org, analyst, analyst2, member):
        dashboard = _dashboard(org, analyst)
        _grant(org, "dashboard", dashboard, analyst2, permission="view")

        with pytest.raises(HttpError) as excinfo:
            _post_grant(analyst2, "dashboard", dashboard, member, "view")
        assert excinfo.value.status_code == 403

    def test_member_blocked_by_slug(self, org, analyst, member):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS, GeneralLevel.EDIT)

        with pytest.raises(HttpError) as excinfo:
            _post_grant(member, "dashboard", dashboard, analyst, "view")
        assert excinfo.value.status_code == 404  # has_permission semantics: 404 unauthorized

    def test_audience_principal_is_deferred_by_design(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            _post_grant(analyst, "dashboard", dashboard, member, "view", principal_type="audience")
        assert excinfo.value.status_code == 400

    def test_group_principal_grant_now_accepted(self, org, analyst, member):
        """Task 7 flips the group-principal 400 deferral: a same-org group id
        is now a valid grant target."""
        dashboard = _dashboard(org, analyst)
        group = UserGroup.objects.create(org=org, name="Funders", created_by=analyst)

        response = _post_grant(
            analyst, "dashboard", dashboard, group, "view", principal_type="group"
        )

        assert response["success"] is True
        assert response["data"]["principal_type"] == "group"
        assert response["data"]["principal_id"] == group.id
        assert response["data"]["name"] == "Funders"
        assert response["data"]["permission"] == "view"
        assert response["data"]["status"] == "active"

    def test_group_principal_duplicate_grant_updates_in_place(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        group = UserGroup.objects.create(org=org, name="Funders", created_by=analyst)

        _post_grant(analyst, "dashboard", dashboard, group, "view", principal_type="group")
        _post_grant(analyst, "dashboard", dashboard, group, "edit", principal_type="group")

        rows = ResourceShare.objects.filter(
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="group",
            principal_id=group.id,
        )
        assert rows.count() == 1
        assert rows.first().permission == "edit"

    def test_cross_org_group_principal_rejected(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        other_org = Org.objects.create(name="Other Org 4", slug="access-other-org-4")
        outsider_group = UserGroup.objects.create(org=other_org, name="Outsiders", created_by=None)

        with pytest.raises(HttpError) as excinfo:
            _post_grant(
                analyst, "dashboard", dashboard, outsider_group, "view", principal_type="group"
            )
        assert excinfo.value.status_code == 404

    def test_grant_on_grantless_rtype_400_via_capability_flag(self, org, analyst, member):
        metric = Metric.objects.create(
            org=org,
            name="m2",
            schema_name="s",
            table_name="t",
            column="c",
            aggregation="sum",
            created_by=analyst,
            owner=analyst,
        )
        with pytest.raises(HttpError) as excinfo:
            _post_grant(analyst, "metric", metric, member, "view")
        assert excinfo.value.status_code == 400

    def test_cross_org_principal_404(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        other_org = Org.objects.create(name="Other Org 2", slug="access-other-org-2")
        outsider = _make_orguser(other_org, ANALYST_ROLE, "accessapi-outsider")

        with pytest.raises(HttpError) as excinfo:
            _post_grant(analyst, "dashboard", dashboard, outsider, "view")
        assert excinfo.value.status_code == 404


# ================================================================================
# POST /api/access/{rtype}/{resource_id}/grants/ — email path (Task 9)
# ================================================================================


class TestCreateGrantByEmail:
    @patch("ddpui.utils.awsses.send_resource_shared_email", Mock())
    def test_existing_user_email_grants_instantly_no_invitation(self, org, analyst, member):
        """Sharing with an email that already belongs to an OrgUser in this
        org resolves to that OrgUser immediately -- no Invitation, no
        pending row (activation path 2)."""
        dashboard = _dashboard(org, analyst)

        response = _post_grant_email(analyst, "dashboard", dashboard, member.user.email, "view")

        assert response["success"] is True
        assert response["data"]["principal_id"] == member.id
        assert response["data"]["email"] == member.user.email
        assert response["data"]["status"] == "active"
        assert not Invitation.objects.filter(invited_email__iexact=member.user.email).exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_unknown_email_creates_member_invitation_and_pending_grant(self, org, analyst):
        dashboard = _dashboard(org, analyst)

        response = _post_grant_email(analyst, "dashboard", dashboard, "future@test.com", "view")

        assert response["success"] is True
        assert response["data"]["principal_id"] is None
        assert response["data"]["email"] == "future@test.com"
        assert response["data"]["status"] == "pending"

        invitation = Invitation.objects.get(invited_email="future@test.com", invited_by=analyst)
        assert invitation.invited_new_role.slug == MEMBER_ROLE

        share = ResourceShare.objects.get(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            pending_email="future@test.com",
        )
        assert share.status == "pending"
        assert share.principal_id is None

        # pending rows never admit anyone -- pin against principal_match_q / overview
        pending_orguser = _make_orguser(org, MEMBER_ROLE, "future-imposter")
        assert effective_permission(pending_orguser, "dashboard", dashboard) is None
        pending_orguser.delete()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_analyst_shares_unknown_email_invitation_role_is_member(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        _post_grant_email(analyst, "dashboard", dashboard, "analyst-invitee@test.com", "view")
        invitation = Invitation.objects.get(invited_email="analyst-invitee@test.com")
        assert invitation.invited_new_role.slug == MEMBER_ROLE

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_admin_shares_unknown_email_invitation_role_is_member(self, org, admin):
        dashboard = _dashboard(org, admin)
        _post_grant_email(admin, "dashboard", dashboard, "admin-invitee@test.com", "view")
        invitation = Invitation.objects.get(invited_email="admin-invitee@test.com")
        assert invitation.invited_new_role.slug == MEMBER_ROLE

    # ---- invite_role (Phase C3): the share-flow invite may carry a role ----

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_non_admin_invite_role_escalation_403_and_nothing_created(self, org, analyst):
        """Requesting a non-Member invite role is admin-only: an analyst
        asking for invite_role='analyst' 403s BEFORE any invitation email or
        pending grant row exists."""
        dashboard = _dashboard(org, analyst)

        with pytest.raises(HttpError) as excinfo:
            _post_grant_email(
                analyst, "dashboard", dashboard, "escalate@test.com", "view", invite_role="analyst"
            )
        assert excinfo.value.status_code == 403
        assert not Invitation.objects.filter(invited_email__iexact="escalate@test.com").exists()
        assert not ResourceShare.objects.filter(pending_email__iexact="escalate@test.com").exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_non_admin_invite_role_member_still_allowed(self, org, analyst):
        response = _post_grant_email(
            analyst,
            "dashboard",
            _dashboard(org, analyst),
            "plain@test.com",
            "view",
            invite_role="member",
        )
        assert response["data"]["status"] == "pending"
        invitation = Invitation.objects.get(invited_email="plain@test.com")
        assert invitation.invited_new_role.slug == MEMBER_ROLE

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_admin_invite_role_analyst_invitation_carries_role(self, org, admin):
        dashboard = _dashboard(org, admin)

        response = _post_grant_email(
            admin,
            "dashboard",
            dashboard,
            "future-analyst@test.com",
            "view",
            invite_role="analyst",
        )

        assert response["data"]["status"] == "pending"
        invitation = Invitation.objects.get(invited_email="future-analyst@test.com")
        assert invitation.invited_new_role.slug == ANALYST_ROLE

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_admin_invite_role_admin_invitation_carries_role(self, org, admin):
        dashboard = _dashboard(org, admin)

        _post_grant_email(
            admin, "dashboard", dashboard, "future-admin@test.com", "view", invite_role="admin"
        )

        invitation = Invitation.objects.get(invited_email="future-admin@test.com")
        assert invitation.invited_new_role.slug == ADMIN_ROLE

    def test_invalid_invite_role_400(self, org, admin):
        dashboard = _dashboard(org, admin)

        with pytest.raises(HttpError) as excinfo:
            _post_grant_email(
                admin, "dashboard", dashboard, "x@test.com", "view", invite_role="super-admin"
            )
        assert excinfo.value.status_code == 400
        assert not Invitation.objects.filter(invited_email__iexact="x@test.com").exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_reinviting_same_pending_email_dedupes_and_keeps_original_role(
        self, org, analyst, admin
    ):
        """PINS `invite_user_v1`'s same-org re-invite branch (the one
        `_invite_email_once` delegates to): an email with an existing
        pending `Invitation` in this org, shared again -- even by an admin
        requesting a HIGHER invite_role the second time -- does NOT create
        a second `Invitation` row and does NOT change `invited_new_role`.
        Only `invited_on`/`expires_at` move (a resend), and the invite
        email fires again. The role is fixed at whatever the FIRST invite
        resolved to; a later, more-privileged caller can't silently
        upgrade a pending invitee's role by re-sharing with them."""
        dashboard = _dashboard(org, analyst)
        email = "repeat-share-invitee@test.com"

        _post_grant_email(analyst, "dashboard", dashboard, email, "view")  # default: Member
        invitation = Invitation.objects.get(invited_email__iexact=email, invited_by__org=org)
        assert invitation.invited_new_role.slug == MEMBER_ROLE
        first_invited_on = invitation.invited_on

        # admin re-shares with the SAME email, asking for a higher role --
        # the dedupe branch ignores invite_role entirely.
        _post_grant_email(admin, "dashboard", dashboard, email, "view", invite_role="analyst")

        assert (
            Invitation.objects.filter(invited_email__iexact=email, invited_by__org=org).count() == 1
        )
        invitation.refresh_from_db()
        assert invitation.invited_new_role.slug == MEMBER_ROLE  # unchanged, not escalated
        assert invitation.invited_on >= first_invited_on  # refreshed, not stale

        # still exactly one pending grant row on this resource, not stacked
        assert (
            ResourceShare.objects.filter(
                org=org,
                resource_type="dashboard",
                resource_id=str(dashboard.pk),
                pending_email__iexact=email,
            ).count()
            == 1
        )

    def test_invite_role_ignored_for_known_org_email(self, org, analyst, member):
        """invite_role is only consulted on the invite path: sharing with an
        existing org member's email grants instantly and never re-roles them
        — so a non-admin sending it isn't a 403 either."""
        dashboard = _dashboard(org, analyst)

        response = _post_grant_email(
            analyst, "dashboard", dashboard, member.user.email, "view", invite_role="admin"
        )

        assert response["data"]["status"] == "active"
        member.refresh_from_db()
        assert member.new_role.slug == MEMBER_ROLE

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_accept_grants_invited_user_the_chosen_role(self, org, admin):
        """Extends the T9 activation contract: an admin's invite_role choice
        sticks — accepting the invitation creates the OrgUser AS that role,
        and their resolver access reflects it (an analyst editor is NOT
        capped to view the way a Member is)."""
        dashboard = _dashboard(org, admin)
        _post_grant_email(
            admin, "dashboard", dashboard, "newanalyst@test.com", "edit", invite_role="analyst"
        )

        invitation = Invitation.objects.get(invited_email="newanalyst@test.com")
        assert invitation.invited_new_role.slug == ANALYST_ROLE

        payload = AcceptInvitationSchema(invite_code=invitation.invite_code, password="password123")
        new_orguser, error = accept_invitation_v1(payload)
        assert error is None

        new_orguser_obj = OrgUser.objects.get(user_id=new_orguser.user_id, org=org)
        assert new_orguser_obj.new_role.slug == ANALYST_ROLE

        share = ResourceShare.objects.get(
            org=org, resource_type="dashboard", resource_id=str(dashboard.pk)
        )
        assert share.status == "active"
        assert share.principal_id == new_orguser_obj.id

        assert effective_permission(new_orguser_obj, "dashboard", dashboard) == "edit"

    def test_email_and_principal_id_together_400(self, org, analyst, member):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        dashboard = _dashboard(org, analyst)
        payload = GrantCreate(
            principal_type="user",
            principal_id=member.id,
            email=member.user.email,
            permission="view",
        )
        with pytest.raises(HttpError) as excinfo:
            create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert excinfo.value.status_code == 400

    def test_email_on_group_principal_400(self, org, analyst):
        from ddpui.api.access_api import create_grant
        from ddpui.schemas.access_schema import GrantCreate

        dashboard = _dashboard(org, analyst)
        group = UserGroup.objects.create(org=org, name="Email Group Test", created_by=analyst)
        payload = GrantCreate(
            principal_type="group", principal_id=group.id, email="x@test.com", permission="view"
        )
        with pytest.raises(HttpError) as excinfo:
            create_grant(mock_request(analyst), "dashboard", str(dashboard.pk), payload)
        assert excinfo.value.status_code == 400

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_accept_activates_pending_grant_and_group_membership(self, org, analyst):
        """End-to-end: accepting the invitation flips the pending grant to
        active with the new OrgUser's id, flips a pending group membership
        for the same email to active, and the new user's resolver access
        reflects the now-active grant."""
        dashboard = _dashboard(org, analyst)
        _post_grant_email(analyst, "dashboard", dashboard, "newperson@test.com", "edit")

        group = UserGroup.objects.create(org=org, name="Accept Test Group", created_by=analyst)
        pending_member = UserGroupMember.objects.create(
            group=group, pending_email="newperson@test.com", status=UserGroupMemberStatus.PENDING
        )

        invitation = Invitation.objects.get(invited_email="newperson@test.com")
        payload = AcceptInvitationSchema(invite_code=invitation.invite_code, password="password123")
        new_orguser, error = accept_invitation_v1(payload)
        assert error is None

        new_orguser_obj = OrgUser.objects.get(user_id=new_orguser.user_id, org=org)
        share = ResourceShare.objects.get(
            org=org, resource_type="dashboard", resource_id=str(dashboard.pk)
        )
        assert share.status == "active"
        assert share.principal_id == new_orguser_obj.id
        assert share.pending_email is None

        pending_member.refresh_from_db()
        assert pending_member.status == UserGroupMemberStatus.ACTIVE
        assert pending_member.orguser_id == new_orguser_obj.id
        assert pending_member.pending_email is None

        # the grant row itself is "edit" (asserted above), but the resolver
        # caps Members at "view" regardless of grant level (Step 5) -- the
        # invited user's role is Member (Part C), so that's what they see
        assert effective_permission(new_orguser_obj, "dashboard", dashboard) == "view"

        # and the list-scoping path (the sharing modal's "the new user sees
        # the resource in their list" case) admits the dashboard too
        from ddpui.core.sharing.access_resolver import accessible_filter

        assert (
            Dashboard.objects.filter(accessible_filter(new_orguser_obj, "dashboard"))
            .filter(id=dashboard.id)
            .exists()
        )

    def test_activation_drops_pending_membership_when_already_an_active_member(self, org, analyst):
        """`add_member`'s `get_or_create` is keyed on `orguser`, not
        `pending_email` -- if an active `(group, orguser)` row already
        exists for this person by the time their pending row would be
        activated (e.g. someone added them to the group directly, by
        `orguser_id`, before they accepted their invite), converting the
        pending row too would violate the `(group, orguser)` unique
        constraint. It must be dropped instead, leaving exactly one active
        membership."""
        from ddpui.core.orguserfunctions import activate_pending_shares_and_memberships

        member_orguser = _make_orguser(org, MEMBER_ROLE, "race-test-member")
        group = UserGroup.objects.create(org=org, name="Race Test Group", created_by=analyst)
        UserGroupMember.objects.create(
            group=group, orguser=member_orguser, status=UserGroupMemberStatus.ACTIVE
        )
        pending_member = UserGroupMember.objects.create(
            group=group,
            pending_email=member_orguser.user.email,
            status=UserGroupMemberStatus.PENDING,
        )

        activate_pending_shares_and_memberships(member_orguser.user.email, org, member_orguser)

        assert not UserGroupMember.objects.filter(id=pending_member.id).exists()
        assert (
            UserGroupMember.objects.filter(
                group=group, orguser=member_orguser, status=UserGroupMemberStatus.ACTIVE
            ).count()
            == 1
        )

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_cross_org_accept_does_not_activate_other_orgs_pending_share(self, org, analyst, admin):
        """The same email invited to two orgs: accepting the invite in one
        org must not activate the pending grant that belongs to the other
        org -- `Invitation` has no org FK, matching must go through
        `invited_by__org`."""
        other_org = Org.objects.create(name="Other Org 5", slug="access-other-org-5")
        other_admin = _make_orguser(other_org, ADMIN_ROLE, "accessapi-other-admin5")
        other_dashboard = _dashboard(other_org, other_admin)

        dashboard = _dashboard(org, analyst)

        _post_grant_email(analyst, "dashboard", dashboard, "shared-email@test.com", "view")
        # a second, independent invite+pending-share to the SAME email in a different org
        _post_grant_email(
            other_admin, "dashboard", other_dashboard, "shared-email@test.com", "edit"
        )

        this_org_invitation = Invitation.objects.get(
            invited_email="shared-email@test.com", invited_by=analyst
        )
        payload = AcceptInvitationSchema(
            invite_code=this_org_invitation.invite_code, password="password123"
        )
        new_orguser, error = accept_invitation_v1(payload)
        assert error is None
        new_orguser_obj = OrgUser.objects.get(user_id=new_orguser.user_id, org=org)

        this_org_share = ResourceShare.objects.get(
            org=org, resource_type="dashboard", resource_id=str(dashboard.pk)
        )
        assert this_org_share.status == "active"
        assert this_org_share.principal_id == new_orguser_obj.id

        other_org_share = ResourceShare.objects.get(
            org=other_org, resource_type="dashboard", resource_id=str(other_dashboard.pk)
        )
        assert other_org_share.status == "pending"
        assert other_org_share.principal_id is None
        assert other_org_share.pending_email == "shared-email@test.com"

        # the other org's invitation is untouched
        assert Invitation.objects.filter(
            invited_email="shared-email@test.com", invited_by=other_admin
        ).exists()


# ================================================================================
# DELETE /api/access/{rtype}/{resource_id}/grants/{grant_id}/
# ================================================================================


class TestDeleteGrant:
    def test_owner_revokes_grant(self, org, analyst, member):
        from ddpui.api.access_api import delete_grant

        dashboard = _dashboard(org, analyst)
        share = _grant(org, "dashboard", dashboard, member)

        response = delete_grant(mock_request(analyst), "dashboard", str(dashboard.pk), share.id)
        assert response["success"] is True
        assert not ResourceShare.objects.filter(id=share.id).exists()

    def test_grant_of_another_resource_404(self, org, analyst, member):
        from ddpui.api.access_api import delete_grant

        dashboard = _dashboard(org, analyst)
        other_dashboard = _dashboard(org, analyst)
        share = _grant(org, "dashboard", other_dashboard, member)

        with pytest.raises(HttpError) as excinfo:
            delete_grant(mock_request(analyst), "dashboard", str(dashboard.pk), share.id)
        assert excinfo.value.status_code == 404
        assert ResourceShare.objects.filter(id=share.id).exists()

    def test_wrong_org_grant_404(self, org, analyst, member):
        from ddpui.api.access_api import delete_grant

        other_org = Org.objects.create(name="Other Org 3", slug="access-other-org-3")
        other_admin = _make_orguser(other_org, ADMIN_ROLE, "accessapi-other-admin3")
        other_dashboard = _dashboard(other_org, other_admin)
        other_member = _make_orguser(other_org, MEMBER_ROLE, "accessapi-other-member3")
        share = _grant(other_org, "dashboard", other_dashboard, other_member)

        with pytest.raises(HttpError) as excinfo:
            delete_grant(mock_request(analyst), "dashboard", str(other_dashboard.pk), share.id)
        # the resource itself is cross-org -> 404 before the grant is even looked at
        assert excinfo.value.status_code == 404
        assert ResourceShare.objects.filter(id=share.id).exists()


# ================================================================================
# PUT /api/access/{rtype}/{resource_id}/general/ — warn-and-offer protocol
# ================================================================================


def _put_general(caller, rtype, resource, audience, level="view", remove_grant_ids=None):
    from ddpui.api.access_api import update_general_access
    from ddpui.schemas.access_schema import GeneralAccessUpdate

    payload = GeneralAccessUpdate(audience=audience, level=level, remove_grant_ids=remove_grant_ids)
    return update_general_access(mock_request(caller), rtype, str(resource.pk), payload)


class TestUpdateGeneralAccess:
    def test_widening_applies_immediately(self, org, analyst):
        dashboard = _dashboard(org, analyst, GeneralAudience.PRIVATE)

        response = _put_general(analyst, "dashboard", dashboard, "all_users", "view")
        assert response["data"]["requires_confirmation"] is False
        assert response["data"]["general_access"] == {"audience": "all_users", "level": "view"}
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.ALL_USERS

    def test_narrowing_with_active_grants_warns_and_changes_nothing(self, org, analyst, member):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        share = _grant(org, "dashboard", dashboard, member)

        response = _put_general(analyst, "dashboard", dashboard, "private", "view")
        assert response["data"]["requires_confirmation"] is True
        assert [g["id"] for g in response["data"]["persisting_grants"]] == [share.id]
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.ALL_USERS  # unchanged
        assert ResourceShare.objects.filter(id=share.id).exists()  # untouched

    def test_resend_with_remove_grant_ids_commits_and_removes(self, org, analyst, member):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        share = _grant(org, "dashboard", dashboard, member)

        response = _put_general(
            analyst, "dashboard", dashboard, "private", "view", remove_grant_ids=[share.id]
        )
        assert response["data"]["requires_confirmation"] is False
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.PRIVATE
        assert not ResourceShare.objects.filter(id=share.id).exists()

    def test_resend_with_empty_remove_list_commits_keeping_grants(self, org, analyst, member):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        share = _grant(org, "dashboard", dashboard, member)

        response = _put_general(
            analyst, "dashboard", dashboard, "admins", "view", remove_grant_ids=[]
        )
        assert response["data"]["requires_confirmation"] is False
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.ADMINS
        assert ResourceShare.objects.filter(id=share.id).exists()  # deliberately kept

    def test_narrowing_with_no_grants_applies_immediately(self, org, analyst):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS)

        response = _put_general(analyst, "dashboard", dashboard, "private", "view")
        assert response["data"]["requires_confirmation"] is False
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.PRIVATE

    def test_invalid_audience_400(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        with pytest.raises(HttpError) as excinfo:
            _put_general(analyst, "dashboard", dashboard, "everyone-on-earth", "view")
        assert excinfo.value.status_code == 400

    def test_remove_grant_ids_of_other_resource_404_and_rolls_back(self, org, analyst, member):
        dashboard = _dashboard(org, analyst, GeneralAudience.ALL_USERS)
        other_dashboard = _dashboard(org, analyst)
        foreign_share = _grant(org, "dashboard", other_dashboard, member)

        with pytest.raises(HttpError) as excinfo:
            _put_general(
                analyst,
                "dashboard",
                dashboard,
                "private",
                "view",
                remove_grant_ids=[foreign_share.id],
            )
        assert excinfo.value.status_code == 404
        dashboard.refresh_from_db()
        assert dashboard.general_audience == GeneralAudience.ALL_USERS  # nothing committed
        assert ResourceShare.objects.filter(id=foreign_share.id).exists()


# ================================================================================
# D1: "shared a resource with you" notification email
# ================================================================================


class TestResourceSharedEmail:
    def test_new_grant_to_active_user_sends_email(self, org, analyst, member):
        dashboard = _dashboard(org, analyst)
        with patch("ddpui.utils.awsses.send_resource_shared_email") as mock_send:
            _post_grant(analyst, "dashboard", dashboard, member, "view")
        assert mock_send.call_count == 1
        assert mock_send.call_args.kwargs["to_email"] == member.user.email

    def test_permission_update_does_not_send_email(self, org, analyst, member):
        """The second (in-place update) grant must not re-notify."""
        dashboard = _dashboard(org, analyst)
        with patch("ddpui.utils.awsses.send_resource_shared_email") as mock_send:
            _post_grant(analyst, "dashboard", dashboard, member, "view")  # created -> sends
            _post_grant(analyst, "dashboard", dashboard, member, "edit")  # update -> silent
        assert mock_send.call_count == 1

    def test_group_grant_does_not_send_email(self, org, analyst):
        dashboard = _dashboard(org, analyst)
        group = UserGroup.objects.create(org=org, name="Funders", created_by=analyst)
        with patch("ddpui.utils.awsses.send_resource_shared_email") as mock_send:
            _post_grant(analyst, "dashboard", dashboard, group, "view", principal_type="group")
        assert mock_send.call_count == 0

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_invite_path_does_not_send_shared_email(self, org, analyst):
        """An unknown email goes through the invite/pending path, which sends
        its own invitation email -- the D1 'shared with you' email must NOT
        also fire (no double-send)."""
        dashboard = _dashboard(org, analyst)
        with patch("ddpui.utils.awsses.send_resource_shared_email") as mock_send:
            _post_grant_email(analyst, "dashboard", dashboard, "future@test.com", "view")
        assert mock_send.call_count == 0

    def test_inactive_user_grant_does_not_send_email(self, org, analyst, member):
        member.user.is_active = False
        member.user.save(update_fields=["is_active"])
        dashboard = _dashboard(org, analyst)
        with patch("ddpui.utils.awsses.send_resource_shared_email") as mock_send:
            _post_grant(analyst, "dashboard", dashboard, member, "view")
        assert mock_send.call_count == 0

    def test_email_send_failure_does_not_break_the_grant(self, org, analyst, member):
        """A raising SES send is logged and swallowed -- the grant row still
        commits and the endpoint returns success."""
        dashboard = _dashboard(org, analyst)
        with patch(
            "ddpui.utils.awsses.send_resource_shared_email",
            side_effect=RuntimeError("SES down"),
        ):
            response = _post_grant(analyst, "dashboard", dashboard, member, "view")
        assert response["success"] is True
        assert ResourceShare.objects.filter(
            resource_type="dashboard", resource_id=str(dashboard.pk), principal_id=member.id
        ).exists()

    def test_subject_and_body_format(self, org, analyst, member):
        """Pin the composed subject + plain-text body (names/types/link only,
        never resource data)."""
        dashboard = _dashboard(org, analyst)  # title == "Access Api Dashboard"
        with patch("ddpui.utils.awsses.send_text_message") as mock_send_text:
            _post_grant(analyst, "dashboard", dashboard, member, "edit")

        assert mock_send_text.call_count == 1
        to_email, subject, message = mock_send_text.call_args.args
        assert to_email == member.user.email
        assert subject == f"{analyst.user.email} shared a dashboard with you"
        assert "Access Api Dashboard · dashboard —" in message
        assert "You have been granted Edit access to this dashboard." in message
        assert f"/dashboards/{dashboard.pk}" in message
