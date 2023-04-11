import os
from unittest.mock import patch
import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.ddpairbyte.airbyte_service import *


class TestWorkspaceIntegration:
    
    def test_get_workspaces(self):
        result = get_workspaces()
        assert isinstance(result, dict)
        assert 'workspaces' in result
        workspaces = result['workspaces']
        assert isinstance(workspaces, list)
        assert all(isinstance(workspace, dict) for workspace in workspaces)
