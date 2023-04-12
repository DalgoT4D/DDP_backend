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
        pass