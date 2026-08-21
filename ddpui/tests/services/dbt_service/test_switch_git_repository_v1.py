"""Tests for switch_git_repository_v1 function."""

import pytest
from pathlib import Path
from unittest.mock import patch, Mock

from ddpui.ddpdbt.dbt_service import switch_git_repository_v1
from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.ddpprefect.schema import OrgDbtConnectGitRemote
from django.contrib.auth.models import User

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


def test_switch_git_repository_v1_managed_to_external_empty_success(setup_data, tmp_path):
    """Test successful switch from managed to external empty repository"""
    user, org, orguser, warehouse = setup_data

    org_dir = tmp_path / org.slug
    org_dir.mkdir()
    dbt_project_dir = org_dir / "dbtrepo"
    dbt_project_dir.mkdir()

    # Create managed OrgDbt
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/managed-repo",
        project_dir=f"{org.slug}/dbtrepo",
        is_repo_managed_by_system=True,
        transform_type=TransformType.GIT,
        target_type="postgres",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir",
        return_value=str(dbt_project_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir",
        return_value=str(org_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager"
    ) as mock_git_manager_class, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ) as mock_validate, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=True,
    ) as mock_empty_check, patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_git_manager = Mock()
        mock_git_manager_class.return_value = mock_git_manager
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True
        assert "Successfully switched to new git repository" in result["message"]

        mock_validate.assert_called_once_with("https://github.com/user/new-repo", "ghp_token123")
        mock_empty_check.assert_called_once()

        orgdbt.refresh_from_db()
        assert orgdbt.gitrepo_url == "https://github.com/user/new-repo"
        assert orgdbt.is_repo_managed_by_system is False
        assert orgdbt.gitrepo_access_token_secret == "updated-pat-secret"


def test_switch_git_repository_v1_managed_to_external_nonempty_success(setup_data, tmp_path):
    """Test successful switch from managed to external non-empty repository"""
    user, org, orguser, warehouse = setup_data

    org_dir = tmp_path / org.slug
    org_dir.mkdir()
    dbt_project_dir = org_dir / "dbtrepo"
    dbt_project_dir.mkdir()

    # Create managed OrgDbt
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/managed-repo",
        project_dir=f"{org.slug}/dbtrepo",
        is_repo_managed_by_system=True,
        transform_type=TransformType.GIT,
        target_type="postgres",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/existing-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir",
        return_value=str(dbt_project_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir",
        return_value=str(org_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=False,
    ) as mock_empty_check, patch(
        "ddpui.ddpdbt.dbt_service.GitManager.clone"
    ) as mock_clone, patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True

        mock_empty_check.assert_called_once()
        mock_clone.assert_called_once()

        # dbtrepo dir was removed before clone
        assert not dbt_project_dir.exists()

        orgdbt.refresh_from_db()
        assert orgdbt.gitrepo_url == "https://github.com/user/existing-repo"
        assert orgdbt.is_repo_managed_by_system is False
        assert orgdbt.gitrepo_access_token_secret == "updated-pat-secret"


def test_switch_git_repository_v1_removes_clone_target_when_slug_changed(setup_data, tmp_path):
    """
    When the org slug changed after initial dbt setup, OrgDbt.project_dir
    references the old slug. The clone target (org_dir / 'dbtrepo') must be
    removed — not the stale dbt_project_dir — so git clone succeeds.
    """
    user, org, orguser, warehouse = setup_data

    old_slug = "old-slug"
    new_slug = org.slug  # "test-org"

    # Simulate stale directory from old slug
    old_dir = tmp_path / old_slug
    old_dir.mkdir()
    old_dbt_project_dir = old_dir / "dbtrepo"
    old_dbt_project_dir.mkdir()
    (old_dbt_project_dir / "marker.txt").write_text("stale")

    # Current org dir with existing dbtrepo (the clone target)
    current_org_dir = tmp_path / new_slug
    current_org_dir.mkdir()
    clone_target = current_org_dir / "dbtrepo"
    clone_target.mkdir()
    (clone_target / "existing_file.txt").write_text("blocks clone")

    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/old-repo",
        project_dir=f"{old_slug}/dbtrepo",  # stale path from old slug
        is_repo_managed_by_system=False,
        transform_type=TransformType.GIT,
        target_type="postgres",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir",
        return_value=str(old_dbt_project_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir",
        return_value=str(current_org_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=False,
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.clone"
    ) as mock_clone, patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="updated-pat-secret"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_canvas_filter.return_value.delete.return_value = None

        result = switch_git_repository_v1(orguser, payload, "ghp_token123")

        assert result["success"] is True

        # Clone target was removed so git clone can succeed
        assert not clone_target.exists()

        # Stale old directory was also cleaned up
        assert not old_dbt_project_dir.exists()

        # Clone was called with the correct org dir
        mock_clone.assert_called_once_with(
            cwd=str(current_org_dir),
            remote_repo_url="https://github.com/user/new-repo",
            relative_path="dbtrepo",
            pat="ghp_token123",
        )

        # project_dir updated to current slug
        orgdbt.refresh_from_db()
        assert orgdbt.project_dir == f"{new_slug}/dbtrepo"


def test_switch_git_repository_v1_updates_project_dir_to_current_slug(setup_data, tmp_path):
    """OrgDbt.project_dir is updated to the current org slug after switch."""
    user, org, orguser, warehouse = setup_data

    org_dir = tmp_path / org.slug
    org_dir.mkdir()

    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/dalgo/old-repo",
        project_dir="stale-slug/dbtrepo",
        is_repo_managed_by_system=False,
        transform_type=TransformType.GIT,
        target_type="postgres",
        default_schema="public",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/user/new-repo", gitrepoAccessToken="ghp_token123"
    )

    with patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir",
        return_value=str(tmp_path / "stale-slug" / "dbtrepo"),
    ), patch(
        "ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir",
        return_value=str(org_dir),
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.validate_repository_access"
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.check_remote_repository_empty_static",
        return_value=False,
    ), patch(
        "ddpui.ddpdbt.dbt_service.GitManager.clone"
    ), patch(
        "ddpui.ddpdbt.dbt_service.update_github_pat_storage", return_value="pat-key"
    ), patch(
        "ddpui.ddpdbt.dbt_service.CanvasNode.objects.filter"
    ) as mock_canvas_filter, patch(
        "ddpui.ddpdbt.dbt_service.sync_gitignore_contents"
    ):
        mock_canvas_filter.return_value.delete.return_value = None

        switch_git_repository_v1(orguser, payload, "ghp_token123")

        orgdbt.refresh_from_db()
        assert orgdbt.project_dir == f"{org.slug}/dbtrepo"
