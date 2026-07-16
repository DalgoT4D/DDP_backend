"""Org-default General access seeded at resource creation.

D1 (permission-model rework): `get_org_role_level_defaults` is the one
shared helper the 5 create paths (dashboard/report/alert/metric/kpi) call
to seed `analyst_level`/`member_level` on a newly created resource: the
org's configured defaults (OrgPreferences.default_analyst_level/
default_member_level) when set, else the model defaults (none/none).

Tests:
1. No OrgPreferences row -> (view, view) -- the pre-per-role product
   default for orgs that have never configured sharing, not the model
   field defaults (none/none).
2. Row with explicit non-default values -> those values
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.sharing.general_access_defaults import get_org_role_level_defaults
from ddpui.models.general_access import AccessLevel
from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(
        name="General Access Defaults Test Org",
        slug="gen-access-def-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


class TestGetOrgRoleLevelDefaults:
    def test_no_preferences_row_falls_back_to_view_view(self, org):
        """Pre-per-role product default for unconfigured orgs: (view, view),
        not the model field defaults (none, none)."""
        assert not OrgPreferences.objects.filter(org=org).exists()
        analyst_level, member_level = get_org_role_level_defaults(org.id)
        assert analyst_level == AccessLevel.VIEW
        assert member_level == AccessLevel.VIEW

    def test_row_with_explicit_defaults(self, org):
        OrgPreferences.objects.create(
            org=org,
            default_analyst_level=AccessLevel.EDIT,
            default_member_level=AccessLevel.VIEW,
        )
        analyst_level, member_level = get_org_role_level_defaults(org.id)
        assert analyst_level == AccessLevel.EDIT
        assert member_level == AccessLevel.VIEW
