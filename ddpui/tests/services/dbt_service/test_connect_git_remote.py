"""Tests for connect_git_remote function."""

import pytest
import uuid
from unittest.mock import patch, Mock
from ddpui.ddpdbt.dbt_service import connect_git_remote
from ddpui.models.org import Org, OrgDbt, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.ddpprefect.schema import OrgDbtConnectGitRemote
from django.contrib.auth.models import User


@patch("ddpui.ddpdbt.dbt_service.connect_existing_repo_to_remote")
@pytest.mark.django_db
def test_connect_git_remote_success_new_token(
    mock_connect_existing,
):
    """Test successful git remote connection with new PAT token"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,  # No existing Git repo
        project_dir="project",
        target_type="postgres",
        default_schema="schema",
        transform_type=TransformType.UI,  # Starting from UI4T
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/test/repo.git", gitrepoAccessToken="test-pat-token"
    )

    # Execute
    result = connect_git_remote(user, payload, "test-pat-token")

    # Verify connect_existing_repo_to_remote was called with correct parameters
    mock_connect_existing.assert_called_once_with(
        org=org, orgdbt=orgdbt, remote_repo_url=payload.gitrepoUrl, access_token="test-pat-token"
    )

    # Verify return value
    assert result["success"] is True
    assert result["gitrepo_url"] == payload.gitrepoUrl
    assert result["message"] == "Successfully connected to remote git repository"
    assert result["repository_switched"] is False


@patch("ddpui.ddpdbt.dbt_service.secretsmanager")
@patch("ddpui.ddpdbt.dbt_service.GitManager")
@pytest.mark.django_db
def test_connect_git_remote_masked_token_with_existing_secret(
    mock_git_manager_class, mock_secretsmanager, tmp_path
):
    """Test git remote connection with masked token raises exception"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="project",
        target_type="postgres",
        default_schema="schema",
        transform_type=TransformType.UI,
        gitrepo_access_token_secret="existing-secret-key",
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/test/repo.git", gitrepoAccessToken="*******"
    )

    # Execute and verify exception is raised
    with pytest.raises(Exception, match="Cannot connect with masked token"):
        connect_git_remote(user, payload, "test-pat-token")

    # Verify GitManager was not instantiated since masked token should fail early
    mock_git_manager_class.assert_not_called()


@pytest.mark.django_db
def test_connect_git_remote_dbt_repo_not_exists():
    """Test git remote connection when DBT repo directory doesn't exist"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="project",
        target_type="postgres",
        default_schema="schema",
        transform_type=TransformType.UI,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/test/repo.git", gitrepoAccessToken="test-pat-token"
    )

    # Mock dbt project directory doesn't exist
    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
    ):
        mock_get_dbt_dir.return_value = "/fake/dbt/project"
        mock_path.return_value.exists.return_value = False

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="DBT repo directory does not exist"):
            connect_git_remote(user, payload, "test-pat-token")


@patch("ddpui.ddpdbt.dbt_service.GitManager")
@pytest.mark.django_db
def test_connect_git_remote_git_not_initialized(mock_git_manager_class):
    """Test git remote connection when git is not initialized in DBT folder"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="project",
        target_type="postgres",
        default_schema="schema",
        transform_type=TransformType.UI,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/test/repo.git", gitrepoAccessToken="test-pat-token"
    )

    # Mock GitManager to raise error during validation
    from ddpui.core.git_manager import GitManagerError

    mock_git_manager_class.side_effect = GitManagerError("Git not initialized", "No .git directory")

    # Mock other dependencies
    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
    ):
        mock_get_dbt_dir.return_value = "/fake/dbt/project"
        mock_path.return_value.exists.return_value = True

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Git is not initialized in the DBT project folder"):
            connect_git_remote(user, payload, "test-pat-token")


@patch("ddpui.ddpdbt.dbt_service.GitManager")
@pytest.mark.django_db
def test_connect_git_remote_verify_url_failure(mock_git_manager_class):
    """Test git remote connection when remote URL verification fails"""
    # Setup
    org = Org.objects.create(name="test-org", slug="test-org")
    auth_user = User.objects.create(
        username=f"testuser-{uuid.uuid4().hex[:8]}", email="test@example.com"
    )
    user = OrgUser.objects.create(org=org, user=auth_user)
    orgdbt = OrgDbt.objects.create(
        gitrepo_url=None,
        project_dir="project",
        target_type="postgres",
        default_schema="schema",
        transform_type=TransformType.UI,
    )
    org.dbt = orgdbt
    org.save()

    payload = OrgDbtConnectGitRemote(
        gitrepoUrl="https://github.com/test/repo.git", gitrepoAccessToken="invalid-token"
    )

    # Mock GitManager instance and methods
    from ddpui.core.git_manager import GitManagerError

    mock_git_manager = Mock()
    mock_git_manager.verify_remote_url.side_effect = GitManagerError(
        "Authentication failed", "Invalid credentials"
    )
    mock_git_manager_class.return_value = mock_git_manager

    # Mock other dependencies
    with (
        patch("ddpui.ddpdbt.dbt_service.DbtProjectManager.get_dbt_project_dir") as mock_get_dbt_dir,
        patch("ddpui.ddpdbt.dbt_service.Path") as mock_path,
    ):
        mock_get_dbt_dir.return_value = "/fake/dbt/project"
        mock_path.return_value.exists.return_value = True

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Authentication failed: Invalid credentials"):
            connect_git_remote(user, payload, "test-pat-token")
