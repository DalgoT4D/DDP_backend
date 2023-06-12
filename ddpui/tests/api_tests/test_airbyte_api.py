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


def test_post_airbyte_workspace():
    test_org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")

    mock_orguser = Mock()
    mock_orguser.org = test_org

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    mock_payload = AirbyteWorkspaceCreate(name="Test Workspace")

    OrgPrefectBlock.objects.filter(block_name="test-org-slug-airbyte-server").delete()

    response = post_airbyte_workspace(mock_request, mock_payload)
    assert response.status_code == 200
