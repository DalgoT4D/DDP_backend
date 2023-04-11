import os
from unittest.mock import patch
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.ddpairbyte.airbyte_service import *


class TestAbreq:

    @patch("requests.post")
    def test_abreq(self, mock_post):
        abhost = os.getenv("AIRBYTE_SERVER_HOST")
        abport = os.getenv("AIRBYTE_SERVER_PORT")
        abver = os.getenv("AIRBYTE_SERVER_APIVER")
        token = os.getenv("AIRBYTE_API_TOKEN")

        mock_post.return_value.json.return_value = {"key": "value"}
        res = abreq("source_definitions/list_for_workspace", {"test": "data"})

        assert res == {"key": "value"}
        mock_post.assert_called_with(
            f"http://{abhost}:{abport}/api/{abver}/source_definitions/list_for_workspace",
            headers={"Authorization": f"Basic {token}"},
            json={"test": "data"},
        )

    @patch("requests.post")
    def test_abreq_failure(self, mock_post):
        mock_post.side_effect = Exception("error")

        # test the abreq function for failure
        with pytest.raises(Exception) as excinfo:
            abreq("test_endpoint", {"test_key": "test_value"})
        assert str(excinfo.value) == "error"


class TestWorkspace:
    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspaces_success(self, mock_abreq):
        mock_abreq.return_value = [{"id": "workspace1"}, {"id": "workspace2"}]
        workspaces = get_workspaces()
        assert workspaces == [{"id": "workspace1"}, {"id": "workspace2"}]

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspaces_failure(self, mock_abreq):
        mock_abreq.side_effect = Exception("Failed to get workspaces")
        with pytest.raises(Exception) as excinfo:
            get_workspaces()
        assert str(excinfo.value) == "Failed to get workspaces"

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspace_success(self, mock_abreq):
        mock_abreq.return_value = {"workspace": {"id": "workspace1"}}
        workspaces = get_workspace("workspace1")
        assert workspaces == {"workspace": {"id": "workspace1"}}

    @patch("ddpui.ddpairbyte.airbyte_service.abreq")
    def test_get_workspace_failure(self, mock_abreq):
        mock_abreq.side_effect = Exception("Failed to get workspace")
        with pytest.raises(Exception) as excinfo:
            get_workspace("workspace1")
        assert str(excinfo.value) == "Failed to get workspace"

        