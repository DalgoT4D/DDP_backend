"""Tests for task_api.py endpoints"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError
from django.contrib.auth.models import User

from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.task_api import (
    get_task,
    get_singletask,
    get_celerytask,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="task_api_testuser",
        email="task_api_testuser@test.com",
        password="testpassword",
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Task API Test Org",
        slug="task-api-test-org",
        airbyte_workspace_id="workspace-id",
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(authuser, org):
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================
# Tests for get_task
# ================================================================================


class TestGetTask:
    @patch("ddpui.api.task_api.TaskProgress")
    def test_task_found(self, MockTaskProgress, orguser, seed_db):
        MockTaskProgress.fetch.return_value = {
            "status": "running",
            "progress": 50,
        }
        request = mock_request(orguser)
        result = get_task(request, "test-task-id-123")
        assert "progress" in result
        assert result["progress"]["status"] == "running"
        MockTaskProgress.fetch.assert_called_once_with(
            task_id="test-task-id-123", hashkey="taskprogress"
        )

    @patch("ddpui.api.task_api.TaskProgress")
    def test_task_not_found(self, MockTaskProgress, orguser, seed_db):
        MockTaskProgress.fetch.return_value = None
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_task(request, "nonexistent-task-id")
        assert exc.value.status_code == 400
        assert "no such task id" in str(exc.value)

    @patch("ddpui.api.task_api.TaskProgress")
    def test_task_custom_hashkey(self, MockTaskProgress, orguser, seed_db):
        MockTaskProgress.fetch.return_value = {"status": "completed"}
        request = mock_request(orguser)
        result = get_task(request, "task-id", hashkey="custom-hash")
        MockTaskProgress.fetch.assert_called_once_with(task_id="task-id", hashkey="custom-hash")
        assert result["progress"]["status"] == "completed"


# ================================================================================
# Tests for get_singletask
# ================================================================================


class TestGetSingleTask:
    @patch("ddpui.api.task_api.SingleTaskProgress")
    def test_task_found(self, MockSingleTaskProgress, orguser, seed_db):
        MockSingleTaskProgress.fetch.return_value = {
            "status": "completed",
            "result": "success",
        }
        request = mock_request(orguser)
        result = get_singletask(request, "my-task-key")
        assert "progress" in result
        assert result["progress"]["status"] == "completed"
        MockSingleTaskProgress.fetch.assert_called_once_with(task_key="my-task-key")

    @patch("ddpui.api.task_api.SingleTaskProgress")
    def test_task_not_found(self, MockSingleTaskProgress, orguser, seed_db):
        MockSingleTaskProgress.fetch.return_value = None
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_singletask(request, "nonexistent-key")
        assert exc.value.status_code == 400
        assert "no such task id" in str(exc.value)

    @patch("ddpui.api.task_api.SingleTaskProgress")
    def test_task_returns_zero(self, MockSingleTaskProgress, orguser, seed_db):
        """Test that 0 (falsy) is a valid result"""
        MockSingleTaskProgress.fetch.return_value = 0
        request = mock_request(orguser)
        result = get_singletask(request, "zero-key")
        assert result["progress"] == 0


# ================================================================================
# Tests for get_celerytask
# ================================================================================


class TestGetCeleryTask:
    @patch("ddpui.api.task_api.AsyncResult")
    def test_task_pending(self, MockAsyncResult, orguser, seed_db):
        mock_result = MagicMock()
        mock_result.status = "PENDING"
        mock_result.result = None
        mock_result.info = None
        MockAsyncResult.return_value = mock_result

        request = mock_request(orguser)
        result = get_celerytask(request, "celery-task-123")
        assert result["id"] == "celery-task-123"
        assert result["status"] == "PENDING"
        assert result["result"] is None
        assert result["error"] is None

    @patch("ddpui.api.task_api.AsyncResult")
    def test_task_success(self, MockAsyncResult, orguser, seed_db):
        mock_result = MagicMock()
        mock_result.status = "SUCCESS"
        mock_result.result = {"data": "some result"}
        mock_result.info = None
        MockAsyncResult.return_value = mock_result

        request = mock_request(orguser)
        result = get_celerytask(request, "celery-task-456")
        assert result["status"] == "SUCCESS"
        assert result["result"] == {"data": "some result"}

    @patch("ddpui.api.task_api.AsyncResult")
    def test_task_failure(self, MockAsyncResult, orguser, seed_db):
        mock_result = MagicMock()
        mock_result.status = "FAILURE"
        mock_result.result = None
        mock_result.info = Exception("Task failed")
        MockAsyncResult.return_value = mock_result

        request = mock_request(orguser)
        result = get_celerytask(request, "celery-task-789")
        assert result["status"] == "FAILURE"
        assert "Task failed" in result["error"]


def test_seed_data(seed_db):
    """Test seed data loaded"""
    assert Role.objects.count() == 5
