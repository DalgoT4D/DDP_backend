"""
Access Control v2 — role-collapse behavior of the `migrate_rbac_v2_roles` command.

Fresh installs get the 3-role world from seed fixtures; this command transforms an
existing pre-v2 four-role database. The test builds a pre-v2 world inside the test
transaction and runs the command, asserting the remap.
"""

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.utils import timezone

from ddpui.management.commands import migrate_rbac_v2_roles as cmd
from ddpui.models.org import Org
from ddpui.models.org_user import Invitation, OrgUser
from ddpui.models.role_based_access import Permission, Role, RolePermission

pytestmark = pytest.mark.django_db


def _grant(role, *slugs):
    for slug in slugs:
        RolePermission.objects.create(role=role, permission=Permission.objects.get(slug=slug))


@pytest.fixture
def old_world():
    """A clean pre-v2 role world built within the test transaction (rolled back after)."""
    RolePermission.objects.all().delete()
    Role.objects.all().delete()
    Permission.objects.all().delete()
    call_command("loaddata", "002_permissions.json")

    roles = {
        "super-admin": Role.objects.create(slug="super-admin", name="Super User", level=5),
        "account-manager": Role.objects.create(
            slug="account-manager", name="Account Manager", level=4
        ),
        "pipeline-manager": Role.objects.create(
            slug="pipeline-manager", name="Pipeline Manager", level=3
        ),
        "analyst": Role.objects.create(slug="analyst", name="Analyst", level=2),
        "guest": Role.objects.create(slug="guest", name="Guest", level=1),
    }
    _grant(roles["super-admin"], "can_create_org", "can_view_dashboards")
    _grant(roles["account-manager"], "can_create_pipeline", "can_delete_dashboards")
    _grant(roles["pipeline-manager"], "can_create_pipeline", "can_run_pipeline")
    # analyst: infra-write (to be stripped) + infra-view (kept) + content (kept)
    _grant(
        roles["analyst"],
        "can_sync_sources",
        "can_run_orgtask",
        "can_create_dbt_model",  # stripped
        "can_view_warehouses",  # kept
        "can_create_dashboards",
        "can_delete_charts",  # kept
    )
    # guest: a write slug 0137 grants in prod (must be stripped) + a view + infra-view
    _grant(roles["guest"], "can_create_dashboards", "can_view_dashboards", "can_view_warehouses")

    org = Org.objects.create(name="Org", slug="org")

    def make_orguser(username, role):
        user = User.objects.create_user(username=username, password="x")
        return OrgUser.objects.create(user=user, org=org, new_role=role)

    ousers = {
        "am": make_orguser("am", roles["account-manager"]),
        "pm": make_orguser("pm", roles["pipeline-manager"]),
        "guest": make_orguser("guest", roles["guest"]),
    }
    Invitation.objects.create(
        invited_email="invitee@x.org",
        invited_by=ousers["am"],
        invited_on=timezone.now(),
        invite_code="code-1",
        invited_new_role=roles["pipeline-manager"],
    )
    return {"roles": roles, "ousers": ousers}


def _run():
    call_command("migrate_rbac_v2_roles")


def slugs_for(role_slug):
    role = Role.objects.get(slug=role_slug)
    return set(RolePermission.objects.filter(role=role).values_list("permission__slug", flat=True))


def test_collapses_to_three_customer_roles(old_world):
    _run()
    existing = set(Role.objects.values_list("slug", flat=True))
    assert {"super-admin", "admin", "analyst", "member"} == existing
    assert "account-manager" not in existing
    assert "pipeline-manager" not in existing
    assert "guest" not in existing


def test_repoints_users_and_invites(old_world):
    _run()
    ousers = old_world["ousers"]
    assert OrgUser.objects.get(pk=ousers["am"].pk).new_role.slug == "admin"
    assert OrgUser.objects.get(pk=ousers["pm"].pk).new_role.slug == "admin"  # PM merged
    assert OrgUser.objects.get(pk=ousers["guest"].pk).new_role.slug == "member"
    # the pending pipeline-manager invite survived (repointed, not cascade-deleted)
    assert Invitation.objects.count() == 1
    assert Invitation.objects.first().invited_new_role.slug == "admin"


def test_strips_analyst_infra_write_keeps_view(old_world):
    _run()
    analyst = slugs_for("analyst")
    assert not ({"can_sync_sources", "can_run_orgtask", "can_create_dbt_model"} & analyst)
    assert "can_view_warehouses" in analyst
    assert {"can_create_dashboards", "can_delete_charts"} <= analyst


def test_makes_member_view_only(old_world):
    _run()
    member = slugs_for("member")
    assert member == set(cmd.MEMBER_SLUGS)
    # the prod-drift create slug is gone
    assert "can_create_dashboards" not in member
    assert "can_view_warehouses" not in member  # infra view stripped


def test_leaves_super_admin_untouched(old_world):
    _run()
    assert "can_create_org" in slugs_for("super-admin")


def test_idempotent_second_run_is_noop(old_world):
    _run()
    _run()  # must not raise or alter the collapsed world
    existing = set(Role.objects.values_list("slug", flat=True))
    assert {"super-admin", "admin", "analyst", "member"} == existing
