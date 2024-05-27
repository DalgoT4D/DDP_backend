import os
from unittest.mock import patch, Mock
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, User

from ddpui.utils.deleteorg import (
    delete_dbt_workspace,
    delete_orgusers,
    delete_airbyte_workspace_v1,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(
        name="org-name", airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug"
    )
    yield org
    print("deleting org_with_workspace")
    org.delete()


@patch.multiple("ddpui.ddpdbt.dbt_service", delete_dbt_workspace=Mock())
def test_delete_dbt_workspace(org_with_workspace):
    """check that dbt_service.delete_dbt_workspace is called"""
    delete_dbt_workspace(org_with_workspace)


def test_delete_orgusers(org_with_workspace):
    """ensure that orguser.user.delete is called"""
    email = "fake-email"
    tempuser = User.objects.create(email=email, username="fake-username")
    OrgUser.objects.create(user=tempuser, org=org_with_workspace)
    assert (
        OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 1
    )
    delete_orgusers(org_with_workspace)
    assert (
        OrgUser.objects.filter(user__email=email, org=org_with_workspace).count() == 0
    )
