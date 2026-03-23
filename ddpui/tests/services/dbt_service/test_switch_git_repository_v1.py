"""Tests for switch_git_repository_v1 function."""

import pytest
from unittest.mock import patch, Mock

from ddpui.ddpdbt.dbt_service import switch_git_repository_v1
from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.ddpprefect.schema import OrgDbtConnectGitRemote
from django.contrib.auth.models import User
from ddpui.core.git_manager import GitManagerError

pytestmark = pytest.mark.django_db


@pytest.fixture
def setup_data():
    """Setup test data"""
    # Create user and org
    user = User.objects.create_user(username="testuser", email="test@example.com")
    org = Org.objects.create(name="Test Org", slug="test-org")
    orguser = OrgUser.objects.create(user=user, org=org)

    # Create warehouse
    warehouse = OrgWarehouse.objects.create(org=org, wtype="postgres")

    yield user, org, orguser, warehouse

    # Cleanup
    user.delete()
    org.delete()


def test_switch_git_repository_v1_managed_to_external_empty_success(setup_data):
    """Test successful switch from managed to external empty repository"""
    user, org, orguser, warehouse = setup_data

    # Create managed OrgDbt
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/managed-repo",
        is_repo_managed_by_system=True,
        transform_type=TransformType.GIT,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir"
    ) as mock_get_dir, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir"
    ) as mock_get_org_dir, patch(
        "ddpui.ddpdbt.dbt_service.GitManager"
    ) as mock_git_manager_class, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ) as mock_validate, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=True,
    ) as mock_empty_check, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.save_github_pat", return_value="pat-secret-key"
    ), patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block",
        return_value=({"success": True}, None),
    ), patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
        return_value={"host": "localhost"},
    ), patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_get_dir.return_value = "/test/dbt/project/dir"
        mock_get_org_dir.return_value = "/test/org/dir"

        # Mock GitManager instance and canvas filter
        mock_git_manager = Mock()
        mock_git_manager_class.return_value = mock_git_manager
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True
        assert "Successfully switched to new git repository" in result["message"]

        # Verify repository access was validated (should happen in every scenario)
        mock_validate.assert_called_once_with("https://github.com/user/new-repo", "ghp_token123")

        # Verify empty check was performed
        mock_empty_check.assert_called_once()

        # Verify OrgDbt was updated
        orgdbt.refresh_from_db()
        assert orgdbt.gitrepo_url == "https://github.com/user/new-repo"
        assert orgdbt.is_repo_managed_by_system is False
        assert orgdbt.gitrepo_access_token_secret == "updated-pat-secret"


def test_switch_git_repository_v1_managed_to_external_nonempty_success(setup_data):
    """Test successful switch from managed to external non-empty repository"""
    user, org, orguser, warehouse = setup_data

    # Create managed OrgDbt
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/managed-repo",
        is_repo_managed_by_system=True,
        transform_type=TransformType.GIT,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/existing-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir"
    ) as mock_get_dir, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir"
    ) as mock_get_org_dir, patch(
        "ddpui.ddpdbt.dbt_service.GitManager"
    ) as mock_git_manager_class, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ) as mock_validate, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=False,
    ) as mock_empty_check, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.clone"
    ) as mock_clone, patch(
        "ddpui.ddpdbt.dbt_service.shutil.rmtree"
    ) as mock_rmtree, patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.save_github_pat", return_value="pat-secret-key"
    ), patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block",
        return_value=({"success": True}, None),
    ), patch(
        "ddpui.ddpdbt.dbt_service.secretsmanager.retrieve_warehouse_credentials",
        return_value={"host": "localhost"},
    ), patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ), patch(
        "ddpui.ddpdbt.dbt_service.Path"
    ) as mock_path:
        mock_get_dir.return_value = "/test/dbt/project/dir"
        mock_get_org_dir.return_value = "/test/org/dir"

        # Mock Path.exists()
        mock_path_instance = Mock()
        mock_path_instance.exists.return_value = True
        mock_path.return_value = mock_path_instance
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True
        assert "Successfully switched to new git repository" in result["message"]

        # Verify repository access was validated
        mock_validate.assert_called_once_with(
            "https://github.com/user/existing-repo", "ghp_token123"
        )

        # Verify empty check was performed
        mock_empty_check.assert_called_once()

        # Verify clone was called (non-empty external repo scenario)
        mock_clone.assert_called_once()

        # Verify OrgDbt was updated
        orgdbt.refresh_from_db()
        assert orgdbt.gitrepo_url == "https://github.com/user/existing-repo"
        assert orgdbt.is_repo_managed_by_system is False
        assert orgdbt.gitrepo_access_token_secret == "updated-pat-secret"
