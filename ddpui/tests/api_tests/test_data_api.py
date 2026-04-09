"""Tests for data_api.py endpoints"""

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
from ddpui.models.tasks import Task, TaskType
from ddpui.models.llm import UserPrompt
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.utils.constants import LIMIT_ROWS_TO_SEND_TO_LLM
from ddpui.api.data_api import (
    get_tasks,
    get_task_config,
    get_roles,
    get_user_prompts,
    get_row_limit,
)
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
# Fixtures
# ================================================================================


@pytest.fixture
def authuser():
    user = User.objects.create(
        username="data_api_testuser",
        email="data_api_testuser@test.com",
        password="testpassword",
    )
    yield user
    user.delete()


@pytest.fixture
def org():
    org = Org.objects.create(
        name="Data API Test Org",
        slug="data-api-test-org",
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


@pytest.fixture
def dbt_task():
    task = Task.objects.create(
        type=TaskType.DBT,
        slug="dbt-run",
        label="dbt run",
        command="run",
        is_system=True,
    )
    yield task
    task.delete()


@pytest.fixture
def git_task():
    task = Task.objects.create(
        type=TaskType.GIT,
        slug="git-pull",
        label="git pull",
        command="pull",
        is_system=True,
    )
    yield task
    task.delete()


# ================================================================================
# Tests for get_tasks
# ================================================================================


class TestGetTasks:
    def test_get_tasks_empty(self, orguser, seed_db):
        # Clear any existing tasks of the types we care about
        Task.objects.filter(type__in=[TaskType.DBT, TaskType.GIT, TaskType.DBTCLOUD]).delete()
        request = mock_request(orguser)
        result = get_tasks(request)
        assert result == []

    def test_get_tasks_with_data(self, orguser, dbt_task, git_task, seed_db):
        request = mock_request(orguser)
        result = get_tasks(request)
        slugs = [t["slug"] for t in result]
        assert "dbt-run" in slugs
        assert "git-pull" in slugs

    def test_get_tasks_excludes_airbyte(self, orguser, seed_db):
        # Create an airbyte task that should not appear
        airbyte_task = Task.objects.create(
            type=TaskType.AIRBYTE,
            slug="airbyte-sync",
            label="Airbyte Sync",
            command="sync",
        )
        request = mock_request(orguser)
        result = get_tasks(request)
        slugs = [t["slug"] for t in result]
        assert "airbyte-sync" not in slugs
        airbyte_task.delete()


# ================================================================================
# Tests for get_task_config
# ================================================================================


class TestGetTaskConfig:
    def test_task_not_found(self, orguser, seed_db):
        request = mock_request(orguser)
        with pytest.raises(HttpError) as exc:
            get_task_config(request, "nonexistent-slug")
        assert exc.value.status_code == 404

    @patch("ddpui.api.data_api.dbt_service.task_config_params")
    def test_task_found(self, mock_config, orguser, dbt_task, seed_db):
        mock_config.return_value = {"params": ["--full-refresh"]}
        request = mock_request(orguser)
        result = get_task_config(request, "dbt-run")
        assert result["params"] == ["--full-refresh"]
        mock_config.assert_called_once_with(dbt_task)


# ================================================================================
# Tests for get_roles
# ================================================================================


class TestGetRoles:
    def test_get_roles(self, orguser, seed_db):
        request = mock_request(orguser)
        result = get_roles(request)
        assert len(result) > 0
        for role in result:
            assert "uuid" in role
            assert "slug" in role
            assert "name" in role

    def test_get_roles_filtered_by_level(self, orguser, seed_db):
        """Roles returned should have level <= orguser's role level"""
        request = mock_request(orguser)
        result = get_roles(request)
        orguser_level = orguser.new_role.level
        for role_data in result:
            role = Role.objects.get(uuid=role_data["uuid"])
            assert role.level <= orguser_level


# ================================================================================
# Tests for get_user_prompts
# ================================================================================


class TestGetUserPrompts:
    def test_get_user_prompts_empty(self, orguser, seed_db):
        UserPrompt.objects.all().delete()
        request = mock_request(orguser)
        result = get_user_prompts(request)
        assert result == []

    def test_get_user_prompts_with_data(self, orguser, seed_db):
        prompt = UserPrompt.objects.create(
            prompt="Summarize this data",
            type="long_text_summarization",
            label="Data Summary",
        )
        request = mock_request(orguser)
        result = get_user_prompts(request)
        assert len(result) >= 1
        prompts = [p["prompt"] for p in result]
        assert "Summarize this data" in prompts
        prompt.delete()


# ================================================================================
# Tests for get_row_limit
# ================================================================================


class TestGetRowLimit:
    def test_get_row_limit(self, orguser, seed_db):
        request = mock_request(orguser)
        result = get_row_limit(request)
        assert result == LIMIT_ROWS_TO_SEND_TO_LLM
        assert result == 500


def test_seed_data(seed_db):
    """Test seed data loaded"""
    assert Role.objects.count() == 5
