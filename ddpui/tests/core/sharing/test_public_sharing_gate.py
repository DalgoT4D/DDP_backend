"""The org-level public-sharing kill switch.

`OrgPreferences.allow_public_sharing` (Task 1) is a per-org master switch.
`org_allows_public_sharing` is the pure boolean read (no row -> default
True, matching the model field's default); `require_public_sharing_enabled`
is the "doorman" that raises HttpError(403) for the enable/re-enable path
of a share toggle.

Tests:
1. org_allows_public_sharing -- no row -> True, row True -> True, row False -> False
2. require_public_sharing_enabled -- no-op when allowed, raises 403 when off
"""

import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from ninja.errors import HttpError

from ddpui.core.sharing.public_sharing_gate import (
    org_allows_public_sharing,
    require_public_sharing_enabled,
)
from ddpui.models.org import Org
from ddpui.models.org_preferences import OrgPreferences

pytestmark = pytest.mark.django_db


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Public Sharing Gate Test Org",
        slug="pub-share-gate-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


class TestOrgAllowsPublicSharing:
    def test_no_preferences_row_defaults_true(self, org):
        assert not OrgPreferences.objects.filter(org=org).exists()
        assert org_allows_public_sharing(org.id) is True

    def test_row_with_switch_on(self, org):
        OrgPreferences.objects.create(org=org, allow_public_sharing=True)
        assert org_allows_public_sharing(org.id) is True

    def test_row_with_switch_off(self, org):
        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        assert org_allows_public_sharing(org.id) is False


class TestRequirePublicSharingEnabled:
    def test_noop_when_no_row(self, org):
        require_public_sharing_enabled(org)  # must not raise

    def test_noop_when_switch_on(self, org):
        OrgPreferences.objects.create(org=org, allow_public_sharing=True)
        require_public_sharing_enabled(org)  # must not raise

    def test_raises_403_when_switch_off(self, org):
        OrgPreferences.objects.create(org=org, allow_public_sharing=False)
        with pytest.raises(HttpError) as exc_info:
            require_public_sharing_enabled(org)
        assert exc_info.value.status_code == 403
