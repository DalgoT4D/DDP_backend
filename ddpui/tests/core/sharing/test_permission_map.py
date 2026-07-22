"""RTYPE_LEVEL_SLUG is the one rtype<->slug source — these tests pin its
completeness against the registry and the seeded Permission rows."""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.sharing.permission_map import (
    IMPLIES,
    RTYPE_LEVEL_SLUG,
    implied_closure,
    permission_id_for,
    reset_permission_id_cache,
    slug_for,
)
from ddpui.core.sharing.shareable_types import RESOURCE_TYPES
from ddpui.models.role_based_access import Permission
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

# Known seed gap: reports have only can_share_reports, no view/edit slugs.
UNMAPPED_RTYPES = {"report"}


def test_every_mapped_rtype_has_view_and_edit():
    mapped_rtypes = {rtype for (rtype, _) in RTYPE_LEVEL_SLUG}
    for rtype in mapped_rtypes:
        assert (rtype, "view") in RTYPE_LEVEL_SLUG
        assert (rtype, "edit") in RTYPE_LEVEL_SLUG


def test_map_covers_registry_except_known_gap():
    mapped_rtypes = {rtype for (rtype, _) in RTYPE_LEVEL_SLUG}
    assert set(RESOURCE_TYPES) - mapped_rtypes == UNMAPPED_RTYPES


def test_every_mapped_slug_exists_in_seeds(seed_db):
    seeded = set(Permission.objects.values_list("slug", flat=True))
    missing = set(RTYPE_LEVEL_SLUG.values()) - seeded
    assert missing == set()


def test_implies_is_edit_to_view_per_rtype():
    assert IMPLIES["can_edit_dashboards"] == "can_view_dashboards"
    assert len(IMPLIES) == len(RTYPE_LEVEL_SLUG) // 2


def test_implied_closure_adds_view_for_edit():
    assert implied_closure({"can_edit_dashboards"}) == {
        "can_edit_dashboards",
        "can_view_dashboards",
    }
    # view implies nothing further; unknown slugs pass through untouched
    assert implied_closure({"can_view_dashboards", "can_share_reports"}) == {
        "can_view_dashboards",
        "can_share_reports",
    }


def test_slug_for_unmapped_rtype_is_none():
    assert slug_for("report", "view") is None
    assert slug_for("dashboard", "delete") is None


def test_permission_id_for_resolves_seeded_slug(seed_db):
    reset_permission_id_cache()
    pk = permission_id_for("dashboard", "edit")
    assert pk == Permission.objects.get(slug="can_edit_dashboards").id
    assert permission_id_for("report", "view") is None
