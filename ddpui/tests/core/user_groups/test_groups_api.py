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
from django.contrib.auth.models import User
from ninja.errors import HttpError

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import GeneralAudience
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.models.user_group import UserGroup, UserGroupMember
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


# ================================================================================
# DELETE /api/groups/{id}/members/{member_id}
# ================================================================================


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
