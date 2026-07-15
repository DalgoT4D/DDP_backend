"""Task 7 Part B: `/api/groups/*` — CRUD + membership.

Route functions are called directly (as the rest of the API test suite
does) via `mock_request(orguser)`, which exercises the real permission
machinery: `request.permissions` is built from the seeded RolePermission
rows, so the `can_view_user_groups`/`can_manage_user_groups` gates behave
exactly as they would behind the middleware.
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
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import GeneralAudience
from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, OrgUser
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
        name="Groups Api Org", slug="groups-api-org", airbyte_workspace_id="w1"
    )
    yield org
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def admin(org, seed_db):
    return _make_orguser(org, ADMIN_ROLE, "groupsapi-admin")


@pytest.fixture
def analyst(org, seed_db):
    return _make_orguser(org, ANALYST_ROLE, "groupsapi-analyst")


@pytest.fixture
def analyst2(org, seed_db):
    return _make_orguser(org, ANALYST_ROLE, "groupsapi-analyst2")


@pytest.fixture
def member(org, seed_db):
    return _make_orguser(org, MEMBER_ROLE, "groupsapi-member")


# ================================================================================
# POST /api/groups/ — create
# ================================================================================


class TestCreateGroup:
    def test_analyst_creates_group(self, org, analyst):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        response = create_group(mock_request(analyst), GroupCreate(name="Funders"))
        assert response["success"] is True
        data = response["data"]
        assert data["name"] == "Funders"
        assert data["member_count"] == 0
        assert data["shared_resource_count"] == 0
        assert data["created_by"]["orguser_id"] == analyst.id

    def test_member_blocked_by_slug(self, org, member):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        with pytest.raises(HttpError) as excinfo:
            create_group(mock_request(member), GroupCreate(name="Funders"))
        assert excinfo.value.status_code == 404  # has_permission semantics: 404 unauthorized

    def test_name_collision_within_org_is_400(self, org, analyst):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        create_group(mock_request(analyst), GroupCreate(name="Funders"))
        with pytest.raises(HttpError) as excinfo:
            create_group(mock_request(analyst), GroupCreate(name="Funders"))
        assert excinfo.value.status_code == 400

    def test_same_name_allowed_in_different_org(self, org, analyst):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        other_org = Org.objects.create(name="Other Groups Org", slug="other-groups-org")
        other_analyst = _make_orguser(other_org, ANALYST_ROLE, "groupsapi-other-analyst")

        create_group(mock_request(analyst), GroupCreate(name="Funders"))
        # no exception -> succeeds
        create_group(mock_request(other_analyst), GroupCreate(name="Funders"))


# ================================================================================
# GET /api/groups/ — list
# ================================================================================


class TestListGroups:
    def test_list_shows_member_and_shared_resource_counts(self, org, analyst, member):
        from ddpui.api.groups_api import create_group, list_groups
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        UserGroupMember.objects.create(group_id=group["id"], orguser=member, status="active")
        dashboard = Dashboard.objects.create(title="D1", org=org, owner=analyst, created_by=analyst)
        ResourceShare.objects.create(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="group",
            principal_id=group["id"],
            permission="view",
            status="active",
        )

        response = list_groups(mock_request(analyst))
        by_id = {g.id: g for g in response["data"]}
        assert by_id[group["id"]].member_count == 1
        assert by_id[group["id"]].shared_resource_count == 1

    def test_list_member_preview_capped_at_4_active_only(self, org, analyst):
        """Phase A / A2: the list path returns up to 4 ACTIVE member emails
        for the avatar stack; pending invites are excluded and a group with
        no members gets an empty preview."""
        from ddpui.api.groups_api import create_group, list_groups
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        empty_group = create_group(mock_request(analyst), GroupCreate(name="Empty"))["data"]

        active_emails = []
        for i in range(5):
            member_orguser = _make_orguser(org, MEMBER_ROLE, f"groupsapi-preview-{i}")
            UserGroupMember.objects.create(
                group_id=group["id"], orguser=member_orguser, status="active"
            )
            active_emails.append(member_orguser.user.email)
        UserGroupMember.objects.create(
            group_id=group["id"], pending_email="pending@test.com", status="pending"
        )

        response = list_groups(mock_request(analyst))
        by_id = {g.id: g for g in response["data"]}

        preview = by_id[group["id"]].member_preview
        assert len(preview) == 4
        assert set(preview) <= set(active_emails)
        assert "pending@test.com" not in preview
        assert by_id[group["id"]].member_count == 5
        assert by_id[empty_group["id"]].member_preview == []

    def test_list_scoped_to_org(self, org, analyst):
        from ddpui.api.groups_api import create_group, list_groups
        from ddpui.schemas.group_schema import GroupCreate

        other_org = Org.objects.create(name="Other Groups Org 2", slug="other-groups-org-2")
        other_analyst = _make_orguser(other_org, ANALYST_ROLE, "groupsapi-other-analyst2")
        create_group(mock_request(other_analyst), GroupCreate(name="Other Group"))

        response = list_groups(mock_request(analyst))
        assert response["data"] == []

    def test_member_blocked_by_slug(self, org, member):
        from ddpui.api.groups_api import list_groups

        with pytest.raises(HttpError) as excinfo:
            list_groups(mock_request(member))
        assert excinfo.value.status_code == 404


# ================================================================================
# GET /api/groups/{id} — detail
# ================================================================================


class TestGetGroup:
    def test_detail_includes_members(self, org, analyst, member):
        from ddpui.api.groups_api import create_group, get_group
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        UserGroupMember.objects.create(group_id=group["id"], orguser=member, status="active")

        response = get_group(mock_request(analyst), group["id"])
        data = response["data"]
        assert data["name"] == "Funders"
        assert len(data["members"]) == 1
        assert data["members"][0]["orguser_id"] == member.id
        assert data["members"][0]["email"] == member.user.email

    def test_active_member_role_reflects_their_org_role(self, org, analyst, member):
        """F5: an active member's row carries their org-role slug, read off
        their OrgUser -- not hardcoded to any particular role."""
        from ddpui.api.groups_api import create_group, get_group
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        UserGroupMember.objects.create(group_id=group["id"], orguser=member, status="active")

        response = get_group(mock_request(analyst), group["id"])
        member_row = response["data"]["members"][0]
        assert member_row["role"] == member.new_role.slug

    def test_pending_member_role_is_none(self, org, analyst):
        """Pending-email rows have no OrgUser yet, so role stays None."""
        from ddpui.api.groups_api import create_group, get_group
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        UserGroupMember.objects.create(
            group_id=group["id"], pending_email="new.person@test.com", status="pending"
        )

        response = get_group(mock_request(analyst), group["id"])
        member_row = response["data"]["members"][0]
        assert member_row["status"] == "pending"
        assert member_row["role"] is None

    def test_cross_org_group_404(self, org, analyst):
        from ddpui.api.groups_api import create_group, get_group
        from ddpui.schemas.group_schema import GroupCreate

        other_org = Org.objects.create(name="Other Groups Org 3", slug="other-groups-org-3")
        other_analyst = _make_orguser(other_org, ANALYST_ROLE, "groupsapi-other-analyst3")
        group = create_group(mock_request(other_analyst), GroupCreate(name="Other Group"))["data"]

        with pytest.raises(HttpError) as excinfo:
            get_group(mock_request(analyst), group["id"])
        assert excinfo.value.status_code == 404


# ================================================================================
# PUT /api/groups/{id} — rename
# ================================================================================


class TestUpdateGroup:
    def test_creator_can_rename(self, org, analyst):
        from ddpui.api.groups_api import create_group, update_group
        from ddpui.schemas.group_schema import GroupCreate, GroupUpdate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        response = update_group(mock_request(analyst), group["id"], GroupUpdate(name="Donors"))
        assert response["data"]["name"] == "Donors"

    def test_admin_can_rename_even_if_not_creator(self, org, analyst, admin):
        from ddpui.api.groups_api import create_group, update_group
        from ddpui.schemas.group_schema import GroupCreate, GroupUpdate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        response = update_group(mock_request(admin), group["id"], GroupUpdate(name="Donors"))
        assert response["data"]["name"] == "Donors"

    def test_non_creator_non_admin_analyst_forbidden(self, org, analyst, analyst2):
        from ddpui.api.groups_api import create_group, update_group
        from ddpui.schemas.group_schema import GroupCreate, GroupUpdate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            update_group(mock_request(analyst2), group["id"], GroupUpdate(name="Donors"))
        assert excinfo.value.status_code == 403

    def test_rename_collision_400(self, org, analyst):
        from ddpui.api.groups_api import create_group, update_group
        from ddpui.schemas.group_schema import GroupCreate, GroupUpdate

        create_group(mock_request(analyst), GroupCreate(name="Donors"))
        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            update_group(mock_request(analyst), group["id"], GroupUpdate(name="Donors"))
        assert excinfo.value.status_code == 400


# ================================================================================
# DELETE /api/groups/{id}
# ================================================================================


class TestDeleteGroup:
    def test_creator_deletes_group_and_its_grants_stop_admitting(self, org, analyst, member):
        from ddpui.api.groups_api import create_group, delete_group
        from ddpui.core.sharing.access_resolver import effective_permission
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        UserGroupMember.objects.create(group_id=group["id"], orguser=member, status="active")
        dashboard = Dashboard.objects.create(
            title="D2",
            org=org,
            owner=analyst,
            created_by=analyst,
            general_audience=GeneralAudience.PRIVATE,
        )
        ResourceShare.objects.create(
            org=org,
            resource_type="dashboard",
            resource_id=str(dashboard.pk),
            principal_type="group",
            principal_id=group["id"],
            permission="view",
            status="active",
        )
        assert effective_permission(member, "dashboard", dashboard) == "view"

        response = delete_group(mock_request(analyst), group["id"])
        assert response["success"] is True
        assert not UserGroup.objects.filter(id=group["id"]).exists()
        assert not ResourceShare.objects.filter(
            principal_type="group", principal_id=group["id"]
        ).exists()
        assert effective_permission(member, "dashboard", dashboard) is None

    def test_non_creator_non_admin_forbidden(self, org, analyst, analyst2):
        from ddpui.api.groups_api import create_group, delete_group
        from ddpui.schemas.group_schema import GroupCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            delete_group(mock_request(analyst2), group["id"])
        assert excinfo.value.status_code == 403
        assert UserGroup.objects.filter(id=group["id"]).exists()

    def test_cross_org_delete_404(self, org, analyst):
        from ddpui.api.groups_api import create_group, delete_group
        from ddpui.schemas.group_schema import GroupCreate

        other_org = Org.objects.create(name="Other Groups Org 4", slug="other-groups-org-4")
        other_analyst = _make_orguser(other_org, ANALYST_ROLE, "groupsapi-other-analyst4")
        group = create_group(mock_request(other_analyst), GroupCreate(name="Other"))["data"]

        with pytest.raises(HttpError) as excinfo:
            delete_group(mock_request(analyst), group["id"])
        assert excinfo.value.status_code == 404


# ================================================================================
# POST /api/groups/{id}/members
# ================================================================================


@patch("ddpui.utils.awsses.send_added_to_group_email", Mock())
class TestAddMember:
    def test_creator_adds_member(self, org, analyst, member):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        response = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id)
        )
        assert response["success"] is True
        assert response["data"]["orguser_id"] == member.id
        assert response["data"]["status"] == "active"
        assert UserGroupMember.objects.filter(group_id=group["id"], orguser=member).exists()

    def test_duplicate_add_is_idempotent(self, org, analyst, member):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
        # second add: no-op, no exception, still exactly one row
        add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
        assert UserGroupMember.objects.filter(group_id=group["id"], orguser=member).count() == 1

    def test_cross_org_orguser_rejected(self, org, analyst):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        other_org = Org.objects.create(name="Other Groups Org 5", slug="other-groups-org-5")
        outsider = _make_orguser(other_org, MEMBER_ROLE, "groupsapi-outsider")
        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]

        with pytest.raises(HttpError) as excinfo:
            add_member(
                mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=outsider.id)
            )
        assert excinfo.value.status_code in (400, 404)

    def test_non_creator_non_admin_forbidden(self, org, analyst, analyst2, member):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            add_member(mock_request(analyst2), group["id"], GroupMemberCreate(orguser_id=member.id))
        assert excinfo.value.status_code == 403

    def test_both_orguser_id_and_email_rejected(self, org, analyst, member):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            add_member(
                mock_request(analyst),
                group["id"],
                GroupMemberCreate(orguser_id=member.id, email=member.user.email),
            )
        assert excinfo.value.status_code == 400

    def test_neither_orguser_id_nor_email_rejected(self, org, analyst):
        from ddpui.api.groups_api import add_member, create_group
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        with pytest.raises(HttpError) as excinfo:
            add_member(mock_request(analyst), group["id"], GroupMemberCreate())
        assert excinfo.value.status_code == 400


# ================================================================================
# POST /api/groups/{id}/members -- email path (M4 / batch 2b)
# ================================================================================


@patch("ddpui.utils.awsses.send_added_to_group_email", Mock())
class TestAddMemberByEmail:
    def _create_group(self, analyst):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        return create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]

    def test_existing_user_email_adds_instantly(self, org, analyst, member):
        """An email matching an existing org member resolves like the
        orguser_id path -- no Invitation, active membership immediately."""
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        response = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(email=member.user.email)
        )
        assert response["success"] is True
        assert response["data"]["orguser_id"] == member.id
        assert response["data"]["status"] == "active"
        assert not Invitation.objects.filter(invited_email__iexact=member.user.email).exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_unknown_email_creates_invitation_and_pending_row(self, org, analyst):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        response = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(email="future@test.com")
        )

        assert response["success"] is True
        assert response["data"]["orguser_id"] is None
        assert response["data"]["pending_email"] == "future@test.com"
        assert response["data"]["status"] == "pending"

        invitation = Invitation.objects.get(invited_email="future@test.com", invited_by=analyst)
        # No invite-role picker in the group-invite design (unlike the share
        # modal's) -- every group invite lands at Member, no escalation path.
        assert invitation.invited_new_role.slug == MEMBER_ROLE

        assert UserGroupMember.objects.filter(
            group_id=group["id"],
            pending_email="future@test.com",
            status=UserGroupMemberStatus.PENDING,
        ).exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_pending_row_activates_through_existing_signup_path(self, org, analyst):
        """Proves the new email-invite path's PENDING row activates through
        the SAME `orguserfunctions.activate_pending_shares_and_memberships`
        used by the share flow -- no bespoke activation logic here."""
        from ddpui.api.groups_api import add_member
        from ddpui.core.orguserfunctions import accept_invitation_v1
        from ddpui.models.org_user import AcceptInvitationSchema
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        added = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(email="newmember@test.com")
        )["data"]
        assert added["status"] == "pending"

        invitation = Invitation.objects.get(invited_email="newmember@test.com")
        payload = AcceptInvitationSchema(invite_code=invitation.invite_code, password="password123")
        new_orguser, error = accept_invitation_v1(payload)
        assert error is None

        new_orguser_obj = OrgUser.objects.get(user_id=new_orguser.user_id, org=org)
        member_row = UserGroupMember.objects.get(id=added["id"])
        assert member_row.status == UserGroupMemberStatus.ACTIVE
        assert member_row.orguser_id == new_orguser_obj.id
        assert member_row.pending_email is None

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_pending_email_add_does_not_send_added_to_group_email(self, org, analyst):
        """D2's "added to a group" email fires only for ACTIVE members --
        staging a pending invite must not fire it."""
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        with patch("ddpui.utils.awsses.send_added_to_group_email") as mock_send:
            add_member(
                mock_request(analyst), group["id"], GroupMemberCreate(email="quiet@test.com")
            )
        assert mock_send.call_count == 0

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_analyst_cannot_escalate_invite_role(self, org, analyst):
        """The design's "Assign new invites role" banner mirrors the share
        modal's admin-only picker (`_resolve_invite_role`) -- a non-admin
        creator requesting a non-Member invite role is rejected, same as the
        share flow."""
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        with pytest.raises(HttpError) as excinfo:
            add_member(
                mock_request(analyst),
                group["id"],
                GroupMemberCreate(email="escalate@test.com", invite_role=ADMIN_ROLE),
            )
        assert excinfo.value.status_code == 403
        assert not Invitation.objects.filter(invited_email="escalate@test.com").exists()

    @patch("ddpui.utils.awsses.send_invite_user_email", Mock())
    def test_admin_can_invite_at_a_higher_role(self, org, admin):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(admin)
        add_member(
            mock_request(admin),
            group["id"],
            GroupMemberCreate(email="future-analyst@test.com", invite_role=ANALYST_ROLE),
        )
        invitation = Invitation.objects.get(invited_email="future-analyst@test.com")
        assert invitation.invited_new_role.slug == ANALYST_ROLE


# ================================================================================
# DELETE /api/groups/{id}/members/{member_id}
# ================================================================================


@patch("ddpui.utils.awsses.send_added_to_group_email", Mock())
class TestRemoveMember:
    def test_creator_removes_member(self, org, analyst, member):
        from ddpui.api.groups_api import add_member, create_group, remove_member
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        added = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id)
        )["data"]

        response = remove_member(mock_request(analyst), group["id"], added["id"])
        assert response["success"] is True
        assert not UserGroupMember.objects.filter(id=added["id"]).exists()

    def test_member_of_other_group_404(self, org, analyst, member):
        from ddpui.api.groups_api import add_member, create_group, remove_member
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group1 = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        group2 = create_group(mock_request(analyst), GroupCreate(name="Donors"))["data"]
        added = add_member(
            mock_request(analyst), group1["id"], GroupMemberCreate(orguser_id=member.id)
        )["data"]

        with pytest.raises(HttpError) as excinfo:
            remove_member(mock_request(analyst), group2["id"], added["id"])
        assert excinfo.value.status_code == 404
        assert UserGroupMember.objects.filter(id=added["id"]).exists()

    def test_non_creator_non_admin_forbidden(self, org, analyst, analyst2, member):
        from ddpui.api.groups_api import add_member, create_group, remove_member
        from ddpui.schemas.group_schema import GroupCreate, GroupMemberCreate

        group = create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]
        added = add_member(
            mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id)
        )["data"]

        with pytest.raises(HttpError) as excinfo:
            remove_member(mock_request(analyst2), group["id"], added["id"])
        assert excinfo.value.status_code == 403
        assert UserGroupMember.objects.filter(id=added["id"]).exists()


# ================================================================================
# D2: "added to a group" notification email
# ================================================================================


class TestAddedToGroupEmail:
    def _create_group(self, analyst):
        from ddpui.api.groups_api import create_group
        from ddpui.schemas.group_schema import GroupCreate

        return create_group(mock_request(analyst), GroupCreate(name="Funders"))["data"]

    def test_new_member_sends_email(self, org, analyst, member):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        with patch("ddpui.utils.awsses.send_added_to_group_email") as mock_send:
            add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
        assert mock_send.call_count == 1
        assert mock_send.call_args.kwargs["to_email"] == member.user.email

    def test_repeat_add_does_not_send_email(self, org, analyst, member):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        with patch("ddpui.utils.awsses.send_added_to_group_email") as mock_send:
            add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
            add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
        assert mock_send.call_count == 1  # only the first, genuinely-new add

    def test_inactive_member_does_not_send_email(self, org, analyst, member):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        member.user.is_active = False
        member.user.save(update_fields=["is_active"])
        group = self._create_group(analyst)
        with patch("ddpui.utils.awsses.send_added_to_group_email") as mock_send:
            add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))
        assert mock_send.call_count == 0

    def test_email_send_failure_does_not_break_add_member(self, org, analyst, member):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)
        with patch(
            "ddpui.utils.awsses.send_added_to_group_email",
            side_effect=RuntimeError("SES down"),
        ):
            response = add_member(
                mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id)
            )
        assert response["success"] is True
        assert UserGroupMember.objects.filter(group_id=group["id"], orguser=member).exists()

    def test_subject_and_body_format(self, org, analyst, member):
        from ddpui.api.groups_api import add_member
        from ddpui.schemas.group_schema import GroupMemberCreate

        group = self._create_group(analyst)  # name == "Funders"
        with patch("ddpui.utils.awsses.send_text_message") as mock_send_text:
            add_member(mock_request(analyst), group["id"], GroupMemberCreate(orguser_id=member.id))

        assert mock_send_text.call_count == 1
        to_email, subject, message = mock_send_text.call_args.args
        assert to_email == member.user.email
        assert subject == f"You have been added to the Funders group by {analyst.user.email}"
        assert (
            "You now automatically inherit access to all resources shared with the Funders group."
            in message
        )
