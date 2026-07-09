"""Truth-table tests for the Resource Sharing access resolver
(``ddpui.core.sharing.access_resolver``).

Pure read-only decision ladder — real ORM objects via fixtures (no mocks for
the resource/viewer graph), stubbed ``get_group_ids`` (no Redis, no HTTP).
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User

from ddpui.auth import ADMIN_ROLE, ANALYST_ROLE, MEMBER_ROLE, SUPER_ADMIN_ROLE
from ddpui.core.sharing.access_resolver import (
    accessible_filter,
    effective_permission,
    principal_match_q,
)
from ddpui.core.sharing.shareable_types import RESOURCE_TYPES
from ddpui.models.dashboard import Dashboard
from ddpui.models.general_access import GeneralAudience, GeneralLevel
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def org():
    org = Org.objects.create(name="Resolver Org", slug="resolver-org", airbyte_workspace_id="w1")
    yield org
    org.delete()


@pytest.fixture
def other_org():
    org = Org.objects.create(
        name="Other Resolver Org", slug="other-resolver-org", airbyte_workspace_id="w2"
    )
    yield org
    org.delete()


def _make_orguser(org_obj, role_slug, username):
    user = User.objects.create(username=username, email=f"{username}@test.com")
    role = Role.objects.filter(slug=role_slug).first() if role_slug else None
    return OrgUser.objects.create(user=user, org=org_obj, new_role=role)


@pytest.fixture
def super_admin(org, seed_db):
    ou = _make_orguser(org, SUPER_ADMIN_ROLE, "resolver-superadmin")
    yield ou
    ou.delete()


@pytest.fixture
def admin(org, seed_db):
    ou = _make_orguser(org, ADMIN_ROLE, "resolver-admin")
    yield ou
    ou.delete()


@pytest.fixture
def analyst(org, seed_db):
    ou = _make_orguser(org, ANALYST_ROLE, "resolver-analyst")
    yield ou
    ou.delete()


@pytest.fixture
def member(org, seed_db):
    ou = _make_orguser(org, MEMBER_ROLE, "resolver-member")
    yield ou
    ou.delete()


@pytest.fixture
def other_org_admin(other_org, seed_db):
    ou = _make_orguser(other_org, ADMIN_ROLE, "resolver-other-org-admin")
    yield ou
    ou.delete()


def _dashboard(org_obj, owner=None, created_by=None, audience=None, level=None):
    return Dashboard.objects.create(
        title="Resolver Test Dashboard",
        org=org_obj,
        owner=owner,
        created_by=created_by,
        general_audience=audience or GeneralAudience.PRIVATE,
        general_level=level or GeneralLevel.VIEW,
    )


# ================================================================================
# Cell 1: super-admin / admin -> edit on everything in-org
# ================================================================================


def test_super_admin_gets_edit_even_on_private_resource(org, super_admin, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    assert effective_permission(super_admin, "dashboard", resource) == "edit"


def test_admin_gets_edit_even_on_private_resource(org, admin, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    assert effective_permission(admin, "dashboard", resource) == "edit"


# ================================================================================
# Cell 2/3: owner -> edit (even private); owner-fallback to created_by
# ================================================================================


def test_owner_gets_edit_even_when_general_is_private(org, analyst):
    resource = _dashboard(org, owner=analyst, audience=GeneralAudience.PRIVATE)
    assert effective_permission(analyst, "dashboard", resource) == "edit"


def test_owner_fallback_to_created_by_when_owner_is_null(org, analyst):
    resource = _dashboard(org, owner=None, created_by=analyst, audience=GeneralAudience.PRIVATE)
    assert effective_permission(analyst, "dashboard", resource) == "edit"


def test_non_owner_non_creator_analyst_denied_on_private(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    assert effective_permission(analyst, "dashboard", resource) is None


# ================================================================================
# Cell 4: general access matrix — audience tier x role tier
# ================================================================================


@pytest.mark.parametrize(
    "audience,role_fixture,expected",
    [
        # private admits nobody via general access, at any tier
        (GeneralAudience.PRIVATE, "member", None),
        (GeneralAudience.PRIVATE, "analyst", None),
        # admins-only tier: analyst/member excluded
        (GeneralAudience.ADMINS, "member", None),
        (GeneralAudience.ADMINS, "analyst", None),
        # analysts_plus: analyst and above admitted, member excluded
        (GeneralAudience.ANALYSTS_PLUS, "member", None),
        (GeneralAudience.ANALYSTS_PLUS, "analyst", "view"),
        # all_users: every org member admitted
        (GeneralAudience.ALL_USERS, "member", "view"),
        (GeneralAudience.ALL_USERS, "analyst", "view"),
        # admin: org-wide override (step 1) beats general access at every
        # tier, including private -- always "edit" regardless of audience.
        (GeneralAudience.PRIVATE, "admin", "edit"),
        (GeneralAudience.ADMINS, "admin", "edit"),
        (GeneralAudience.ANALYSTS_PLUS, "admin", "edit"),
        (GeneralAudience.ALL_USERS, "admin", "edit"),
    ],
)
def test_general_access_audience_by_role_tier(
    request, org, member, analyst, admin, audience, role_fixture, expected
):
    viewer = request.getfixturevalue(role_fixture)
    # Owned by nobody in this fixture set (a stranger orguser), so ownership
    # never masks the general-access decision under test.
    stranger = member if role_fixture != "member" else analyst
    resource = _dashboard(org, owner=stranger, audience=audience, level=GeneralLevel.VIEW)
    assert effective_permission(viewer, "dashboard", resource) == expected


def test_general_access_grants_the_resources_own_level_not_a_hardcoded_view(org, analyst, member):
    """Pins step 3 actually returning `resource.general_level` rather than a
    hardcoded "view" -- every other matrix cell above uses level=VIEW, so
    without this cell a regression collapsing step 3 to "view" would still
    pass the full suite."""
    resource = _dashboard(
        org, owner=member, audience=GeneralAudience.ALL_USERS, level=GeneralLevel.EDIT
    )
    assert effective_permission(analyst, "dashboard", resource) == "edit"


# ================================================================================
# Cell 5: general_level=edit + Member -> capped at view
# ================================================================================


def test_member_capped_at_view_even_when_general_level_is_edit(org, member, analyst):
    resource = _dashboard(
        org,
        owner=analyst,
        audience=GeneralAudience.ALL_USERS,
        level=GeneralLevel.EDIT,
    )
    assert effective_permission(member, "dashboard", resource) == "view"


# ================================================================================
# Cell 6: grants — user grant, group grant (injected stub), best-of
# ================================================================================


def test_user_grant_view(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="view",
        status="active",
    )
    assert effective_permission(analyst, "dashboard", resource) == "view"


def test_user_grant_edit(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="edit",
        status="active",
    )
    assert effective_permission(analyst, "dashboard", resource) == "edit"


def test_group_grant_via_injected_stub(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="group",
        principal_id=42,
        permission="view",
        status="active",
    )
    assert effective_permission(analyst, "dashboard", resource, get_group_ids=lambda v: {42}) == (
        "view"
    )
    # Without the stub returning that group id, the grant does not match.
    assert (
        effective_permission(analyst, "dashboard", resource, get_group_ids=lambda v: set()) is None
    )


def test_best_of_general_view_and_grant_edit_is_edit_for_non_member(org, analyst, member):
    resource = _dashboard(
        org, owner=member, audience=GeneralAudience.ANALYSTS_PLUS, level=GeneralLevel.VIEW
    )
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="edit",
        status="active",
    )
    assert effective_permission(analyst, "dashboard", resource) == "edit"


# ================================================================================
# Cell 7: pending grant row -> grants nothing
# ================================================================================


def test_pending_grant_grants_nothing(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="edit",
        status="pending",
    )
    assert effective_permission(analyst, "dashboard", resource) is None


# ================================================================================
# Cell 8: manually-inserted principal_type="audience" row -> grants nothing
# ================================================================================


def test_audience_principal_type_row_grants_nothing(org, analyst, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="audience",
        principal_value="analysts_plus",
        permission="edit",
        status="active",
    )
    assert effective_permission(analyst, "dashboard", resource) is None


# ================================================================================
# Cell 9: cross-org viewer -> None
# ================================================================================


def test_cross_org_viewer_denied_even_if_admin_in_own_org(org, other_org_admin, member):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.ALL_USERS)
    assert effective_permission(other_org_admin, "dashboard", resource) is None


# ================================================================================
# Cell 10: null role / legacy slug -> None, no exception
# ================================================================================


def test_null_role_denied_not_crash(org, member):
    viewer = _make_orguser(org, None, "resolver-null-role")
    resource = _dashboard(org, owner=member, audience=GeneralAudience.ALL_USERS)
    try:
        result = effective_permission(viewer, "dashboard", resource)
    finally:
        viewer.delete()
    assert result is None


def test_legacy_unknown_role_slug_denied_not_crash(org, member):
    legacy_role = Role.objects.create(slug="legacy-viewer", name="Legacy Viewer", level=1)
    viewer = OrgUser.objects.create(
        user=User.objects.create(username="resolver-legacy", email="resolver-legacy@test.com"),
        org=org,
        new_role=legacy_role,
    )
    resource = _dashboard(org, owner=member, audience=GeneralAudience.ALL_USERS)
    try:
        result = effective_permission(viewer, "dashboard", resource)
    finally:
        viewer.delete()
        legacy_role.delete()
    assert result is None


def test_null_role_with_explicit_grant_still_grants_access(org, member):
    """Documented interpretation: grants (step 4) are evaluated independent
    of role and are NOT gated by a known role rank -- a viewer whose role
    was deleted/never set can still use an explicit share. Only the
    role-gated general-access path (step 3) and the Member cap (step 5)
    depend on a resolvable role slug."""
    viewer = _make_orguser(org, None, "resolver-null-role-grant")
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=viewer.id,
        permission="edit",
        status="active",
    )
    try:
        result = effective_permission(viewer, "dashboard", resource)
    finally:
        viewer.delete()
    assert result == "edit"


# ================================================================================
# Cell 11: accessible_filter — a Member sees exactly the admitted set, in ONE query
# ================================================================================


def test_accessible_filter_member_sees_exactly_admitted_set(
    django_assert_num_queries, org, member, analyst
):
    # Private, not owned/created by member, no grant -> NOT visible.
    private_hidden = _dashboard(org, owner=analyst, audience=GeneralAudience.PRIVATE)
    # all_users tier admits every member -> visible via general access.
    general_visible = _dashboard(org, owner=analyst, audience=GeneralAudience.ALL_USERS)
    # analysts_plus tier excludes member -> NOT visible via general access.
    tier_excluded = _dashboard(org, owner=analyst, audience=GeneralAudience.ANALYSTS_PLUS)
    # Private but explicitly granted to member -> visible via grant.
    granted_visible = _dashboard(org, owner=analyst, audience=GeneralAudience.PRIVATE)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(granted_visible.pk),
        principal_type="user",
        principal_id=member.id,
        permission="view",
        status="active",
    )
    # Private but owned by member -> visible via ownership.
    owned_visible = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)

    with django_assert_num_queries(1):
        visible_ids = set(
            Dashboard.objects.filter(accessible_filter(member, "dashboard")).values_list(
                "id", flat=True
            )
        )

    assert visible_ids == {general_visible.id, granted_visible.id, owned_visible.id}
    assert private_hidden.id not in visible_ids
    assert tier_excluded.id not in visible_ids


# ================================================================================
# Cell 12: registry contract — every registered rtype's model has the
# shareable contract attrs; `chart` is not registered.
# ================================================================================


def test_registry_contract_attrs_present_on_every_registered_model():
    contract_attrs = ("general_audience", "general_level", "owner", "created_by", "org")
    assert RESOURCE_TYPES, "registry should not be empty"
    for rtype, entry in RESOURCE_TYPES.items():
        for attr in contract_attrs:
            assert hasattr(entry.model, attr), f"{rtype}'s model missing '{attr}'"


def test_chart_is_not_registered():
    assert "chart" not in RESOURCE_TYPES


# ================================================================================
# Bonus: principal_match_q directly (also exercised indirectly above via
# effective_permission and accessible_filter)
# ================================================================================


def test_principal_match_q_matches_user_and_group_excludes_pending_and_audience(
    org, analyst, member
):
    resource = _dashboard(org, owner=member, audience=GeneralAudience.PRIVATE)
    user_grant = ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="view",
        status="active",
    )
    group_grant = ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="group",
        principal_id=99,
        permission="view",
        status="active",
    )
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=analyst.id,
        permission="edit",
        status="pending",
    )
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="audience",
        principal_value="all_users",
        permission="edit",
        status="active",
    )

    matched_ids = set(
        ResourceShare.objects.filter(
            principal_match_q(analyst, get_group_ids=lambda v: {99})
        ).values_list("id", flat=True)
    )
    assert matched_ids == {user_grant.id, group_grant.id}
