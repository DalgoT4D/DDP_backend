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
        "ddpui.ddpdbt.dbt_service.create_or_update_dbt_profile_secret_blk",
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


def test_switch_git_repository_v1_managed_to_external_nonempty_success(setup_data, tmp_path):
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

    # Use real paths under tmp_path
    org_dir = tmp_path / "test-org"
    org_dir.mkdir()
    dbt_project_dir = org_dir / "dbtrepo"
    dbt_project_dir.mkdir()

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir"
    ) as mock_get_dir, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir"
    ) as mock_get_org_dir, patch(
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
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_repo_relative_path",
        return_value="test-org/dbtrepo",
    ), patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_get_dir.return_value = str(dbt_project_dir)
        mock_get_org_dir.return_value = str(org_dir)
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

        # Verify rmtree was called on the correct clone destination (org_dir/dbtrepo)
        mock_rmtree.assert_called_once_with(dbt_project_dir)

        # Verify clone was called with the correct org directory
        mock_clone.assert_called_once_with(
            cwd=str(org_dir),
            remote_repo_url="https://github.com/user/existing-repo",
            relative_path="dbtrepo",
            pat="ghp_token123",
        )

        # Verify OrgDbt was updated
        orgdbt.refresh_from_db()
        assert orgdbt.gitrepo_url == "https://github.com/user/existing-repo"
        assert orgdbt.is_repo_managed_by_system is False
        assert orgdbt.gitrepo_access_token_secret == "updated-pat-secret"
        assert orgdbt.project_dir == "test-org/dbtrepo"


def test_switch_git_repository_v1_clone_removes_correct_dir_when_paths_differ(
    setup_data, tmp_path
):
    """
    Regression test: when orgdbt.project_dir references a directory under a
    different org's slug (stale path), rmtree must delete the clone destination
    (org_dir/dbtrepo), not the stale dbt_project_dir.
    """
    user, org, orguser, warehouse = setup_data

    # orgdbt.project_dir points to a DIFFERENT org's directory ("other-org/dbtrepo")
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo",
        project_dir="other-org/dbtrepo",
        is_repo_managed_by_system=False,
        transform_type=TransformType.GIT,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo", gitrepoAccessToken="ghp_token123"
    )

    # Simulate the stale path under another org and the current org's directory
    old_org_dir = tmp_path / "other-org"
    old_org_dir.mkdir()
    stale_dbt_dir = old_org_dir / "dbtrepo"
    stale_dbt_dir.mkdir()

    current_org_dir = tmp_path / "test-org"
    current_org_dir.mkdir()
    current_dbt_dir = current_org_dir / "dbtrepo"
    current_dbt_dir.mkdir()  # This exists and must be removed before clone

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir"
    ) as mock_get_dir, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir"
    ) as mock_get_org_dir, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=False,
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.clone"
    ) as mock_clone, patch(
        "ddpui.ddpdbt.dbt_service.shutil.rmtree"
    ) as mock_rmtree, patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_repo_relative_path",
        return_value="test-org/dbtrepo",
    ), patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        # get_dbt_project_dir returns the STALE path (other-org/dbtrepo)
        mock_get_dir.return_value = str(stale_dbt_dir)
        # get_org_dir returns the CURRENT org's directory
        mock_get_org_dir.return_value = str(current_org_dir)
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True

        # rmtree must target the clone destination (current org), NOT the stale path
        mock_rmtree.assert_called_once_with(current_dbt_dir)

        # Clone must target the current org's directory
        mock_clone.assert_called_once_with(
            cwd=str(current_org_dir),
            remote_repo_url="https://github.com/user/new-repo",
            relative_path="dbtrepo",
            pat="ghp_token123",
        )

        # project_dir must be updated to the current org's path
        orgdbt.refresh_from_db()
        assert orgdbt.project_dir == "test-org/dbtrepo"
