"""
Access Control v2 — role-collapse + permission-remap invariants.

Validates the *seed* (fresh-install) world: the 3 customer roles (admin / analyst /
member) plus super-admin, with the correct permission slug sets. The data migration
(0165) mirrors these same changes onto existing DBs; see test_rbac_v2_migration.py.
"""

import pytest
from django.core.management import call_command

from ddpui.models.role_based_access import Role, RolePermission

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        call_command("loaddata", "001_roles.json")
        call_command("loaddata", "002_permissions.json")
        call_command("loaddata", "003_role_permissions.json")


def slugs_for(role_slug: str) -> set[str]:
    """Return the set of permission slugs granted to the role."""
    role = Role.objects.get(slug=role_slug)
    return set(RolePermission.objects.filter(role=role).values_list("permission__slug", flat=True))


# the four content resources that members may view and analysts may fully manage
CONTENT_VIEW = {"can_view_dashboards", "can_view_charts", "can_view_metrics", "can_view_kpis"}
# infra "view" slugs — analysts keep these, members must NOT have them
INFRA_VIEW = {"can_view_warehouses", "can_view_sources", "can_view_dbt_workspace"}
# infra "write/run" slugs analysts must lose
INFRA_WRITE = {
    "can_sync_sources",
    "can_create_dbt_model",
    "can_create_dbt_workspace",
    "can_run_orgtask",
    "can_create_orgtask",
}


def test_three_customer_roles_plus_superadmin_exist(seed_db):
    existing = set(Role.objects.values_list("slug", flat=True))
    # exact match: the collapsed roles are gone AND no unexpected role slipped in
    assert {"super-admin", "admin", "analyst", "member"} == existing


def test_role_levels(seed_db):
    assert Role.objects.get(slug="super-admin").level == 5
    assert Role.objects.get(slug="admin").level == 4
    assert Role.objects.get(slug="analyst").level == 2
    assert Role.objects.get(slug="member").level == 1


def test_super_admin_untouched(seed_db):
    # super-admin keeps its uniquely-privileged slug
    assert "can_create_org" in slugs_for("super-admin")


def test_admin_has_full_management(seed_db):
    admin = slugs_for("admin")
    assert {"can_create_pipeline", "can_delete_dashboards", "can_edit_warehouse"} <= admin


def test_analyst_keeps_infra_view_loses_infra_write(seed_db):
    analyst = slugs_for("analyst")
    # read-only on infra
    assert INFRA_VIEW <= analyst
    assert not (INFRA_WRITE & analyst)
    # but full management of content — including metrics + KPIs
    assert {"can_create_dashboards", "can_delete_charts", "can_share_dashboards"} <= analyst
    assert {
        "can_create_metrics",
        "can_edit_metrics",
        "can_delete_metrics",
        "can_create_kpis",
        "can_edit_kpis",
        "can_delete_kpis",
    } <= analyst


def test_member_is_view_only_content(seed_db):
    member = slugs_for("member")
    # can view the four content resources
    assert CONTENT_VIEW <= member
    # strictly view-only: every slug is a view slug or an explicitly allowed non-view
    # basic — a blocklist of write prefixes would let can_run_* / can_sync_* slip through
    allowed_non_view = {"can_request_llm_analysis_feature", "public"}
    non_view = {s for s in member if not s.startswith("can_view_")}
    assert non_view <= allowed_non_view
    # Members can view warehouses + warehouse data (needed for the chart-builder
    # dataset selector to work when a Member has resource-EDIT on a chart). Dbt
    # workspace + sources remain hidden.
    member_infra_block = INFRA_VIEW - {"can_view_warehouses"}
    assert not (member_infra_block & member)
