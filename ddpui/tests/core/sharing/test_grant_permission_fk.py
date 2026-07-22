"""The grant FK (``ResourceShare.granted_permission``) must track the varchar
level on every write path — create, upgrade, update_or_create — and map
through RTYPE_LEVEL_SLUG."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.sharing.permission_map import reset_permission_id_cache
from ddpui.models.org import Org
from ddpui.models.resource_share import ResourceShare
from ddpui.models.role_based_access import Permission
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


@pytest.fixture
def org(seed_db):
    reset_permission_id_cache()
    return Org.objects.create(name="FKOrg", slug="fk-org", airbyte_workspace_id="wfk")


def _grant(org, level, rtype="dashboard", resource_id="1"):
    return ResourceShare.objects.create(
        org=org,
        resource_type=rtype,
        resource_id=resource_id,
        principal_type="user",
        principal_id=1,
        permission=level,
    )


def test_create_sets_fk_from_level(org):
    share = _grant(org, "edit")
    assert share.granted_permission.slug == "can_edit_dashboards"


def test_level_change_moves_fk(org):
    share = _grant(org, "view")
    assert share.granted_permission.slug == "can_view_dashboards"
    share.permission = "edit"
    share.save()
    share.refresh_from_db()
    assert share.granted_permission.slug == "can_edit_dashboards"


def test_update_or_create_upgrade_syncs_fk(org):
    _grant(org, "view", resource_id="7")
    share, created = ResourceShare.objects.update_or_create(
        org=org,
        resource_type="dashboard",
        resource_id="7",
        principal_type="user",
        principal_id=1,
        defaults={"permission": "edit"},
    )
    assert created is False
    share.refresh_from_db()
    assert share.granted_permission.slug == "can_edit_dashboards"


def test_unmapped_rtype_leaves_fk_null(org):
    share = _grant(org, "view", rtype="report")
    assert share.granted_permission_id is None


def test_permission_row_is_delete_protected(org):
    _grant(org, "edit")
    with pytest.raises(Exception) as excinfo:
        Permission.objects.get(slug="can_edit_dashboards").delete()
    assert "ProtectedError" in type(excinfo.value).__name__
