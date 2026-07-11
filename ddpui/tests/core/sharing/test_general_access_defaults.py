"""Task 11 Part C: org-default General access seeded at resource creation.

`get_org_general_defaults` is the one shared helper the 5 create paths
(dashboard/report/alert/metric/kpi) call to seed `general_audience`/
`general_level` on a newly created resource: the org's configured
defaults (OrgPreferences.default_general_audience/level) when set, else
the model defaults (all_users/view).

Tests:
1. No OrgPreferences row -> (all_users, view)
2. Row with explicit non-default values -> those values
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.sharing.general_access_defaults import get_org_general_defaults
from ddpui.models.general_access import GeneralAudience, GeneralLevel
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


class TestGetOrgGeneralDefaults:
    def test_no_preferences_row_falls_back_to_model_defaults(self, org):
        assert not OrgPreferences.objects.filter(org=org).exists()
        audience, level = get_org_general_defaults(org.id)
        assert audience == GeneralAudience.ALL_USERS
        assert level == GeneralLevel.VIEW

    def test_row_with_explicit_defaults(self, org):
        OrgPreferences.objects.create(
            org=org,
            default_general_audience=GeneralAudience.ADMINS,
            default_general_level=GeneralLevel.VIEW,
        )
        audience, level = get_org_general_defaults(org.id)
        assert audience == GeneralAudience.ADMINS
        assert level == GeneralLevel.VIEW
