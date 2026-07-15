"""Task 7 Part C.1: the resolver's `get_group_ids` seam wired to real
`UserGroupMember` rows, as the DEFAULT — every existing caller (lists,
detail gates, access endpoints) that doesn't pass `get_group_ids` must pick
up group membership with no edits.

Complements ddpui/tests/core/sharing/test_access_resolver.py, which only
exercises the seam with a stubbed `get_group_ids`. This file is the one
that proves the real lookup works end-to-end, and that wiring it in didn't
add a query to the list-scoping hot path.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User

from ddpui.auth import ANALYST_ROLE, MEMBER_ROLE
from ddpui.core.sharing.access_resolver import accessible_filter, effective_permission
from ddpui.core.sharing.gates import require_edit_access
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.models.user_group import UserGroup, UserGroupMember
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ninja.errors import HttpError

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Group Resolver Org", slug="group-resolver-org", airbyte_workspace_id="w1"
    )
    yield org
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first()
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def analyst(org, seed_db):
    return _make_orguser(org, ANALYST_ROLE, "groupres-analyst")


@pytest.fixture
def group_member(org, seed_db):
    return _make_orguser(org, MEMBER_ROLE, "groupres-groupmember")


@pytest.fixture
def analyst_group_member(org, seed_db):
    """An Analyst-tier group member — used where the test wants an
    edit-level group grant to actually resolve to edit (a Member-tier
    viewer's own GRANT contribution is capped at "view" regardless of the
    grant's permission, per the ladder's step 4; that's a separate,
    already-tested rule, not what this file covers)."""
    return _make_orguser(org, ANALYST_ROLE, "groupres-analyst-groupmember")


@pytest.fixture
def non_member(org, seed_db):
    return _make_orguser(org, MEMBER_ROLE, "groupres-nonmember")


def _dashboard(org_obj, owner, analyst_level=AccessLevel.NONE, member_level=AccessLevel.NONE):
    return Dashboard.objects.create(
        title="Group Resolver Dashboard",
        org=org_obj,
        owner=owner,
        created_by=owner,
        analyst_level=analyst_level,
        member_level=member_level,
    )


def _group_with_member(org_obj, creator, member):
    group = UserGroup.objects.create(org=org_obj, name="Funders", created_by=creator)
    UserGroupMember.objects.create(group=group, orguser=member, status="active")
    return group


def _group_grant(org_obj, rtype, resource, group, permission="view"):
    return ResourceShare.objects.create(
        org=org_obj,
        resource_type=rtype,
        resource_id=str(resource.pk),
        principal_type="group",
        principal_id=group.id,
        permission=permission,
        status="active",
    )


class TestDefaultGroupIdsWiring:
    def test_group_member_gets_view_via_effective_permission_default(
        self, org, analyst, group_member, non_member
    ):
        group = _group_with_member(org, analyst, group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="view")

        assert effective_permission(group_member, "dashboard", dashboard) == "view"
        assert effective_permission(non_member, "dashboard", dashboard) is None

    def test_group_member_sees_resource_in_accessible_filter_default(
        self, org, analyst, group_member, non_member
    ):
        group = _group_with_member(org, analyst, group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="view")

        member_visible = set(
            Dashboard.objects.filter(accessible_filter(group_member, "dashboard")).values_list(
                "id", flat=True
            )
        )
        non_member_visible = set(
            Dashboard.objects.filter(accessible_filter(non_member, "dashboard")).values_list(
                "id", flat=True
            )
        )
        assert dashboard.id in member_visible
        assert dashboard.id not in non_member_visible

    def test_group_edit_grant_passes_require_edit_access(self, org, analyst, analyst_group_member):
        group = _group_with_member(org, analyst, analyst_group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="edit")

        # No exception -> access admitted.
        require_edit_access(analyst_group_member, "dashboard", dashboard)

    def test_group_view_grant_fails_require_edit_access(self, org, analyst, group_member):
        group = _group_with_member(org, analyst, group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="view")

        with pytest.raises(HttpError) as excinfo:
            require_edit_access(group_member, "dashboard", dashboard)
        assert excinfo.value.status_code == 403

    def test_deleted_group_grant_no_longer_admits(self, org, analyst, group_member):
        group = _group_with_member(org, analyst, group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="view")
        assert effective_permission(group_member, "dashboard", dashboard) == "view"

        group.delete()
        # a dangling group grant (principal referencing a now-deleted group id)
        # must not keep admitting people, even if the ResourceShare row itself
        # were to survive the group delete.
        assert effective_permission(group_member, "dashboard", dashboard) is None

    def test_accessible_filter_stays_one_query_with_default_group_lookup(
        self, org, analyst, group_member, django_assert_num_queries
    ):
        group = _group_with_member(org, analyst, group_member)
        dashboard = _dashboard(org, owner=analyst)
        _group_grant(org, "dashboard", dashboard, group, permission="view")

        with django_assert_num_queries(1):
            list(
                Dashboard.objects.filter(accessible_filter(group_member, "dashboard")).values_list(
                    "id", flat=True
                )
            )
