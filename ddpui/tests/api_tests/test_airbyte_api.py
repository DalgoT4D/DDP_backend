import os
import django

from unittest.mock import Mock, patch


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.api.client.airbyte_api import (
    post_airbyte_workspace,
)
from ddpui.ddpairbyte.schema import AirbyteWorkspaceCreate
from ddpui import ddpprefect


def test_post_airbyte_workspace():
    """if the request passes the authentication check
    AND there are no airbyte server blocks for this org
    AND we can conenct to airbyte (or a mocked version of it)
    then post_airbyte_workspace must succeed
    """
    test_org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")

    mock_orguser = Mock()
    mock_orguser.org = test_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    testworkspacename = "Test Workspace"
    mock_payload = AirbyteWorkspaceCreate(name=testworkspacename)

    for serverblock in OrgPrefectBlock.objects.filter(
        org=test_org, block_type=ddpprefect.AIRBYTESERVER
    ):
        serverblock.delete()

    serverblock = OrgPrefectBlock.objects.filter(
        block_name="test-org-slug-airbyte-server"
    ).first()
    if serverblock:
        serverblock.delete()

    response = post_airbyte_workspace(mock_request, mock_payload)
    assert response.name == testworkspacename


if __name__ == "__main__":
    test_post_airbyte_workspace()
