"""Truth-table tests for the Resource Sharing access resolver
(``ddpui.core.sharing.access_resolver``).

Pure read-only decision ladder — real ORM objects via fixtures (no mocks for
the resource/viewer graph), stubbed ``get_group_ids`` (no Redis, no HTTP).

D1 (permission-model rework): general access is now one independent
``AccessLevel`` per role (``analyst_level``/``member_level``) instead of an
(audience, level) threshold pair — Admins are never stored (always full
access at step 1). See ``access_resolver`` module docstring for the updated
decision ladder.
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
from ddpui.models.general_access import AccessLevel
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


def _dashboard(org_obj, owner=None, created_by=None, analyst_level=None, member_level=None):
    return Dashboard.objects.create(
        title="Resolver Test Dashboard",
        org=org_obj,
        owner=owner,
        created_by=created_by,
        analyst_level=analyst_level or AccessLevel.NONE,
        member_level=member_level or AccessLevel.NONE,
    )


# ================================================================================
# Cell 1: super-admin / admin -> edit on everything in-org
# ================================================================================


def test_super_admin_gets_edit_even_on_locked_down_resource(org, super_admin, member):
    resource = _dashboard(org, owner=member)
    assert effective_permission(super_admin, "dashboard", resource) == "edit"


def test_admin_gets_edit_even_on_locked_down_resource(org, admin, member):
    resource = _dashboard(org, owner=member)
    assert effective_permission(admin, "dashboard", resource) == "edit"


# ================================================================================
# Cell 2/3: owner -> edit (even locked-down); owner-fallback to created_by
# ================================================================================


def test_owner_gets_edit_even_when_general_access_is_locked_down(org, analyst):
    resource = _dashboard(org, owner=analyst)
    assert effective_permission(analyst, "dashboard", resource) == "edit"


def test_owner_fallback_to_created_by_when_owner_is_null(org, analyst):
    resource = _dashboard(org, owner=None, created_by=analyst)
    assert effective_permission(analyst, "dashboard", resource) == "edit"


def test_non_owner_non_creator_analyst_denied_when_locked_down(org, analyst, member):
    resource = _dashboard(org, owner=member)
    assert effective_permission(analyst, "dashboard", resource) is None


# ================================================================================
# Cell 4: general access matrix — per-role level x viewer role (D1)
# ================================================================================


@pytest.mark.parametrize(
    "analyst_level,member_level,role_fixture,expected",
    [
        # none/none admits nobody via general access, at any tier
        (AccessLevel.NONE, AccessLevel.NONE, "member", None),
        (AccessLevel.NONE, AccessLevel.NONE, "analyst", None),
        # analyst_level=view, member_level=none: analyst admitted, member excluded
        (AccessLevel.VIEW, AccessLevel.NONE, "member", None),
        (AccessLevel.VIEW, AccessLevel.NONE, "analyst", "view"),
        # both view: every org member admitted
        (AccessLevel.VIEW, AccessLevel.VIEW, "member", "view"),
        (AccessLevel.VIEW, AccessLevel.VIEW, "analyst", "view"),
        # D1: member_level is independently settable, INCLUDING "edit" --
        # the whole point of this rework ("Analyst=Edit, Member=View" and
        # its mirror must both be storable).
        (AccessLevel.NONE, AccessLevel.EDIT, "member", "edit"),
        (AccessLevel.VIEW, AccessLevel.EDIT, "member", "edit"),
        (AccessLevel.EDIT, AccessLevel.VIEW, "analyst", "edit"),
        (AccessLevel.EDIT, AccessLevel.VIEW, "member", "view"),
        # admin: org-wide override (step 1) beats general access at every
        # level, including none/none -- always "edit".
        (AccessLevel.NONE, AccessLevel.NONE, "admin", "edit"),
        (AccessLevel.EDIT, AccessLevel.EDIT, "admin", "edit"),
    ],
)
def test_general_access_by_role_and_level(
    request, org, member, analyst, admin, analyst_level, member_level, role_fixture, expected
):
    viewer = request.getfixturevalue(role_fixture)
    # Owned by nobody in this fixture set (a stranger orguser), so ownership
    # never masks the general-access decision under test.
    stranger = member if role_fixture != "member" else analyst
    resource = _dashboard(
        org, owner=stranger, analyst_level=analyst_level, member_level=member_level
    )
    assert effective_permission(viewer, "dashboard", resource) == expected


def test_general_access_grants_the_resources_own_level_not_a_hardcoded_view(org, analyst, member):
    """Pins step 3 actually returning the role's stored level rather than a
    hardcoded "view" -- every other matrix cell above uses level=VIEW, so
    without this cell a regression collapsing step 3 to "view" would still
    pass the full suite."""
    resource = _dashboard(org, owner=member, analyst_level=AccessLevel.EDIT)
    assert effective_permission(analyst, "dashboard", resource) == "edit"


# ================================================================================
# Cell 5: the Member cap moved with D1 -- it no longer applies to the
# general-access contribution (member_level="edit" is a real outcome, see
# Cell 4 above), but it's UNCHANGED for the grant contribution, since
# direct/group grants are explicitly untouched by this rework.
# ================================================================================


def test_member_grant_capped_at_view_even_when_grant_permission_is_edit(org, member, analyst):
    resource = _dashboard(org, owner=analyst)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(resource.pk),
        principal_type="user",
        principal_id=member.id,
        permission="edit",
        status="active",
    )
    assert effective_permission(member, "dashboard", resource) == "view"


def test_analyst_grant_is_not_capped(org, analyst, member):
    """Contrast with the Member-only cap above: an Analyst's own grant is
    never capped, at any level."""
    resource = _dashboard(org, owner=member)
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
# Cell 6: grants — user grant, group grant (injected stub), best-of
# ================================================================================


def test_user_grant_view(org, analyst, member):
    resource = _dashboard(org, owner=member)
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
    resource = _dashboard(org, owner=member)
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
    resource = _dashboard(org, owner=member)
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
    resource = _dashboard(org, owner=member, analyst_level=AccessLevel.VIEW)
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
    resource = _dashboard(org, owner=member)
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
    resource = _dashboard(org, owner=member)
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
    resource = _dashboard(
        org, owner=member, analyst_level=AccessLevel.VIEW, member_level=AccessLevel.VIEW
    )
    assert effective_permission(other_org_admin, "dashboard", resource) is None


# ================================================================================
# Cell 10: null role / legacy slug -> None, no exception
# ================================================================================


def test_null_role_denied_not_crash(org, member):
    viewer = _make_orguser(org, None, "resolver-null-role")
    resource = _dashboard(
        org, owner=member, analyst_level=AccessLevel.VIEW, member_level=AccessLevel.VIEW
    )
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
    resource = _dashboard(
        org, owner=member, analyst_level=AccessLevel.VIEW, member_level=AccessLevel.VIEW
    )
    try:
        result = effective_permission(viewer, "dashboard", resource)
    finally:
        viewer.delete()
        legacy_role.delete()
    assert result is None


def test_null_role_with_explicit_grant_still_grants_access(org, member):
    """Documented interpretation: grants (step 4) are evaluated independent
    of role and are NOT gated by a resolvable role slug -- a viewer whose
    role was deleted/never set can still use an explicit share. Only the
    role-gated general-access path (step 3) and the Member grant cap (step
    4) depend on a resolvable role slug."""
    viewer = _make_orguser(org, None, "resolver-null-role-grant")
    resource = _dashboard(org, owner=member)
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
    # Locked down, not owned/created by member, no grant -> NOT visible.
    private_hidden = _dashboard(org, owner=analyst)
    # member_level=view admits every Member -> visible via general access.
    general_visible = _dashboard(
        org, owner=analyst, analyst_level=AccessLevel.VIEW, member_level=AccessLevel.VIEW
    )
    # member_level=none excludes Member even though analyst_level=view -> NOT visible.
    tier_excluded = _dashboard(org, owner=analyst, analyst_level=AccessLevel.VIEW)
    # Locked down but explicitly granted to member -> visible via grant.
    granted_visible = _dashboard(org, owner=analyst)
    ResourceShare.objects.create(
        org=org,
        resource_type="dashboard",
        resource_id=str(granted_visible.pk),
        principal_type="user",
        principal_id=member.id,
        permission="view",
        status="active",
    )
    # Locked down but owned by member -> visible via ownership.
    owned_visible = _dashboard(org, owner=member)

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
    contract_attrs = ("analyst_level", "member_level", "owner", "created_by", "org")
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
    resource = _dashboard(org, owner=member)
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
