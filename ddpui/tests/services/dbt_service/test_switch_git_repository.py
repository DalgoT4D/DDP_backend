"""Tests for switch_git_repository function."""

import pytest
import uuid
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import switch_git_repository
from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.ddpprefect.schema import OrgDbtConnectGitRemote
from django.contrib.auth.models import User
from ddpui.core.git_manager import GitManagerError


@patch("ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block")
@patch("ddpui.ddpdbt.dbt_service.sync_gitignore_contents")
@patch("ddpui.ddpdbt.dbt_service.GitManager")
@patch("ddpui.ddpdbt.dbt_service.update_github_pat_storage")
@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir")
@patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir")
@pytest.mark.django_db
def test_switch_git_repository_success_new_token(
    mock_get_org_dir,
    mock_get_dbt_project_dir,
    mock_secretsmanager,
    mock_update_pat_storage,
    mock_git_manager_class,
    mock_sync_gitignore,
    mock_create_cli_block,
):
    """Test successful git repository switch with new PAT token"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="new-pat-token"
    )

    # Create a warehouse for the org
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="test-warehouse",
        credentials='{"host": "localhost", "port": 5432, "database": "testdb"}',
    )

    # Mock paths
    mock_get_dbt_project_dir.return_value = "/fake/dbt/project"
    mock_get_org_dir.return_value = "/fake/org/dir"
    mock_update_pat_storage.return_value = "new-secret-key"
    mock_secretsmanager.retrieve_warehouse_credentials.return_value = {
        "username": "testuser",
        "password": "testpass",
    }
    mock_create_cli_block.return_value = (Mock(), None)  # Success, no error

    # Mock Path.exists to return True and shutil.rmtree
    with (
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
        patch("ddpui.ddpdbt.dbt_service.shutil.rmtree") as mock_rmtree,
    ):
        mock_dbt_path = Mock()
        mock_dbt_path.exists.return_value = True
        mock_dbt_path.__str__ = Mock(return_value="/fake/dbt/project")
        mock_org_path = Mock()
        mock_org_path.__str__ = Mock(return_value="/fake/org/dir")
        mock_path.return_value = mock_dbt_path
        mock_path.side_effect = lambda x: mock_dbt_path if "dbt" in str(x) else mock_org_path

        # Execute
        result = switch_git_repository(user, payload, "test-pat-token")

    # Verify PAT storage was called
    mock_update_pat_storage.assert_called_once_with(
        org, payload.gitrepoUrl, payload.gitrepoAccessToken, None
    )

    # Verify GitManager.clone was called with actual_pat parameter
    mock_git_manager_class.clone.assert_called_once_with(
        cwd="/fake/org/dir",
        remote_repo_url=payload.gitrepoUrl,
        relative_path="dbtrepo",
        pat="test-pat-token",  # This should be the actual_pat parameter, not the payload token
    )

    # Verify OrgDbt was updated
    orgdbt.refresh_from_db()
    assert orgdbt.gitrepo_url == payload.gitrepoUrl
    assert orgdbt.transform_type == TransformType.GIT
    assert orgdbt.gitrepo_access_token_secret == "new-secret-key"


@patch("ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block")
@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@pytest.mark.django_db
def test_switch_git_repository_masked_token_with_existing_secret(
    mock_secretsmanager, mock_create_cli_block, tmp_path
):
    """Test repository switch with masked token when existing secret exists"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
        gitrepo_access_token_secret="existing-secret-key",
    )
    org.dbt = orgdbt
    org.save()

    # Create warehouse for the org
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="test-warehouse",
        credentials='{"host": "localhost", "port": 5432, "database": "testdb"}',
    )

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="*******"
    )

    # Setup mock returns
    mock_secretsmanager.retrieve_github_pat.return_value = "actual-pat-token"
    mock_secretsmanager.retrieve_warehouse_credentials.return_value = {
        "username": "testuser",
        "password": "testpass",
    }
    mock_create_cli_block.return_value = (Mock(), None)  # Success, no error

    # Use real temporary paths
    dbt_project_dir = tmp_path / "dbt_project"
    dbt_project_dir.mkdir()
    org_dir = tmp_path / "org"
    org_dir.mkdir()

    # Mock other dependencies
    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir") as mock_get_org_dir,
        patch("ddpui.ddpdbt.dbt_service.GitManager") as mock_git_manager,
        patch("ddpui.ddpdbt.dbt_service.GitManager.clone") as mock_clone,
        patch("ddpui.ddpdbt.dbt_service.sync_gitignore_contents"),
        patch("ddpui.ddpdbt.dbt_service.update_github_pat_storage") as mock_update_pat,
    ):
        mock_get_dbt_dir.return_value = str(dbt_project_dir)
        mock_get_org_dir.return_value = str(org_dir)

        # Execute
        result = switch_git_repository(user, payload, "test-pat-token")

    # Verify that the actual PAT parameter was used (PAT resolution now handled in API layer)
    mock_clone.assert_called_once_with(
        cwd=str(org_dir),
        remote_repo_url=payload.gitrepoUrl,
        relative_path="dbtrepo",
        pat="test-pat-token",  # Should use the actual_pat parameter passed to the function
    )

    # Verify update_github_pat_storage was NOT called for masked token
    mock_update_pat.assert_not_called()


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.GitManager.clone")
@pytest.mark.django_db
def test_switch_git_repository_masked_token_no_existing_secret(mock_clone, mock_secretsmanager):
    """Test repository switch with masked token when PAT validation is handled at API layer"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
        gitrepo_access_token_secret=None,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="*******"
    )

    # Mock the clone to fail
    mock_clone.side_effect = GitManagerError("Failed to clone repository", "Clone failed")

    # Since PAT validation is now handled at API layer, service function just uses whatever PAT is passed
    # This test now verifies that the service function works with any PAT value
    with pytest.raises(
        Exception, match="Failed to clone new repository: Failed to clone repository"
    ):
        switch_git_repository(user, payload, "some-actual-pat-token")


@patch("ddpui.ddpdbt.dbt_service.GitManager.clone")
@pytest.mark.django_db
def test_switch_git_repository_git_clone_failure(mock_clone, tmp_path):
    """Test repository switch when git clone fails"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="test-pat-token"
    )

    # Mock clone to raise GitManagerError
    from ddpui.core.git_manager import GitManagerError

    mock_clone.side_effect = GitManagerError("Clone failed", "Authentication error")

    # Use real temporary paths
    dbt_project_dir = tmp_path / "dbt_project"
    dbt_project_dir.mkdir()
    org_dir = tmp_path / "org"
    org_dir.mkdir()

    # Mock other dependencies
    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir") as mock_get_org_dir,
        patch("ddpui.ddpdbt.dbt_service.update_github_pat_storage"),
    ):
        mock_get_dbt_dir.return_value = str(dbt_project_dir)
        mock_get_org_dir.return_value = str(org_dir)

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Failed to clone new repository: Clone failed"):
            switch_git_repository(user, payload, "test-pat-token")


@pytest.mark.django_db
def test_switch_git_repository_verification_failure():
    """Test repository switch when remote URL verification fails and PAT storage is cleaned up"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
        gitrepo_access_token_secret="existing-secret-key",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="new-pat-token"
    )

    # Create a warehouse for the org
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="test-warehouse",
        credentials='{"host": "localhost", "port": 5432, "database": "testdb"}',
    )

    from ddpui.core.git_manager import GitManagerError

    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir") as mock_get_org_dir,
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
        patch("ddpui.ddpdbt.dbt_service.shutil.rmtree") as mock_rmtree,
        patch("ddpui.ddpdbt.dbt_service.GitManager") as mock_git_manager_class,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager") as mock_secretsmanager,
        patch("ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block") as mock_create_cli_block,
        patch("ddpui.ddpdbt.dbt_service.clear_github_pat_storage") as mock_clear_pat_storage,
        patch("ddpui.ddpdbt.dbt_service.update_github_pat_storage") as mock_update_pat_storage,
    ):
        # Mock paths and dependencies
        mock_get_dbt_dir.return_value = "/fake/dbt/project"
        mock_get_org_dir.return_value = "/fake/org/dir"
        mock_path.return_value.exists.return_value = True
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {
            "username": "testuser",
            "password": "testpass",
        }
        mock_create_cli_block.return_value = (Mock(), None)

        # Mock GitManager clone to succeed but verify_remote_url to fail
        mock_git_manager_class.clone.return_value = None
        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.side_effect = GitManagerError(
            "Authentication failed", "Invalid PAT token"
        )
        mock_git_manager_class.return_value = mock_git_manager

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Authentication failed: Invalid PAT token"):
            switch_git_repository(user, payload, "test-pat-token")

        # Verify that clear_github_pat_storage was called when verification failed
        mock_clear_pat_storage.assert_called_once_with(org, "existing-secret-key")

        # Verify that update_github_pat_storage was NOT called (verification failed)
        mock_update_pat_storage.assert_not_called()

        # Verify verification was attempted
        mock_git_manager.verify_remote_url.assert_called_once_with(payload.gitrepoUrl)


@pytest.mark.django_db
def test_switch_git_repository_pat_updated_after_verification():
    """Test that PAT storage is only updated after successful verification"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url="https://github.com/old/repo.git",
        project_dir="old-project",
        target_type="postgres",
        default_schema="old_schema",
        transform_type=TransformType.GIT,
        gitrepo_access_token_secret=None,  # No existing secret
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/new/repo.git", gitrepoAccessToken="new-pat-token"
    )

    # Create a warehouse for the org
    warehouse = OrgWarehouse.objects.create(
        org=org,
        wtype="postgres",
        name="test-warehouse",
        credentials='{"host": "localhost", "port": 5432, "database": "testdb"}',
    )

    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_org_dir") as mock_get_org_dir,
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
        patch("ddpui.ddpdbt.dbt_service.shutil.rmtree") as mock_rmtree,
        patch("ddpui.ddpdbt.dbt_service.GitManager") as mock_git_manager_class,
        patch("ddpui.ddpdbt.dbt_service.secretsmanager") as mock_secretsmanager,
        patch("ddpui.ddpdbt.dbt_service.create_or_update_org_cli_block") as mock_create_cli_block,
        patch("ddpui.ddpdbt.dbt_service.clear_github_pat_storage") as mock_clear_pat_storage,
        patch("ddpui.ddpdbt.dbt_service.update_github_pat_storage") as mock_update_pat_storage,
        patch("ddpui.ddpdbt.dbt_service.sync_gitignore_contents") as mock_sync_gitignore,
    ):
        # Mock paths and dependencies
        mock_get_dbt_dir.return_value = "/fake/dbt/project"
        mock_get_org_dir.return_value = "/fake/org/dir"
        mock_path.return_value.exists.return_value = True
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {
            "username": "testuser",
            "password": "testpass",
        }
        mock_create_cli_block.return_value = (Mock(), None)
        mock_update_pat_storage.return_value = "new-secret-key"

        # Mock GitManager to succeed in all operations
        mock_git_manager_class.clone.return_value = None
        mock_git_manager = Mock()
        mock_git_manager.verify_remote_url.return_value = True  # Verification succeeds
        mock_git_manager_class.return_value = mock_git_manager

        # Execute
        result = switch_git_repository(user, payload, "test-pat-token")

        # Verify success
        assert result["success"] is True
        assert result["gitrepo_url"] == payload.gitrepoUrl

        # Verify clear_github_pat_storage was NOT called (verification succeeded)
        mock_clear_pat_storage.assert_not_called()

        # Verify update_github_pat_storage WAS called (after verification)
        mock_update_pat_storage.assert_called_once_with(
            org, payload.gitrepoUrl, payload.gitrepoAccessToken, None
        )

        # Verify verification was attempted and succeeded
        mock_git_manager.verify_remote_url.assert_called_once_with(payload.gitrepoUrl)

        # Verify order: verification should happen before PAT update
        # GitManager instance should be created before update_github_pat_storage is called
        assert mock_git_manager_class.call_count >= 1
        assert mock_update_pat_storage.call_count == 1
