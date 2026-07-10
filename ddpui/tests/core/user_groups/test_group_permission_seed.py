"""Task 7 Part B: the two group-management permission slugs — seed fixtures
(fresh installs) and the 0173 data migration (existing databases).

Mirrors ddpui/tests/core/sharing/test_share_permission_seed.py exactly.
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from importlib import import_module
from unittest.mock import patch

import pytest
from django.apps import apps as live_apps

from ddpui.models.role_based_access import Permission, Role, RolePermission
from ddpui.tests.api_tests.test_user_org_api import seed_db  # noqa: E402

# module name starts with a digit — importable only via import_module
seed_migration = import_module("ddpui.migrations.0173_seed_user_group_permissions")

pytestmark = pytest.mark.django_db

NEW_SLUGS = {"can_manage_user_groups", "can_view_user_groups"}


def slugs_for(role_slug: str) -> set:
    role = Role.objects.get(slug=role_slug)
    return set(RolePermission.objects.filter(role=role).values_list("permission__slug", flat=True))


# ================================================================================
# Fresh-install world: seed fixtures
# ================================================================================


def test_seed_fixtures_contain_the_group_slugs(seed_db):
    assert NEW_SLUGS <= set(Permission.objects.values_list("slug", flat=True))


def test_seed_grants_group_slugs_to_admin_and_analyst_not_member(seed_db):
    assert NEW_SLUGS <= slugs_for("super-admin")
    assert NEW_SLUGS <= slugs_for("admin")
    assert NEW_SLUGS <= slugs_for("analyst")
    assert not (NEW_SLUGS & slugs_for("member"))


# ================================================================================
# Existing-database world: the 0173 data migration
# ================================================================================


def test_migration_seeds_slugs_and_role_grants(seed_db):
    Permission.objects.filter(slug__in=NEW_SLUGS).delete()
    assert not (NEW_SLUGS & slugs_for("admin"))

    seed_migration.seed_group_permissions(live_apps, None)

    assert NEW_SLUGS <= set(Permission.objects.values_list("slug", flat=True))
    assert NEW_SLUGS <= slugs_for("super-admin")
    assert NEW_SLUGS <= slugs_for("admin")
    assert NEW_SLUGS <= slugs_for("analyst")
    assert not (NEW_SLUGS & slugs_for("member"))


def test_migration_is_idempotent_on_double_run(seed_db):
    seed_migration.seed_group_permissions(live_apps, None)
    before_permissions = Permission.objects.count()
    before_role_permissions = RolePermission.objects.count()

    seed_migration.seed_group_permissions(live_apps, None)

    assert Permission.objects.count() == before_permissions
    assert RolePermission.objects.count() == before_role_permissions


def test_migration_survives_redis_being_down(seed_db):
    with patch(
        "ddpui.utils.redis_client.RedisClient.get_instance",
        side_effect=ConnectionError("no redis"),
    ):
        seed_migration.seed_group_permissions(live_apps, None)
    assert NEW_SLUGS <= set(Permission.objects.values_list("slug", flat=True))


def test_migration_reverse_removes_the_slugs(seed_db):
    seed_migration.seed_group_permissions(live_apps, None)
    seed_migration.remove_group_permissions(live_apps, None)

    assert not (NEW_SLUGS & set(Permission.objects.values_list("slug", flat=True)))
    assert not (NEW_SLUGS & slugs_for("admin"))
    # can_share_dashboards is untouched
    assert Permission.objects.filter(slug="can_share_dashboards").exists()
