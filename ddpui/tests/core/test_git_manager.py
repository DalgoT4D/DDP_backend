import os
from unittest.mock import Mock, patch
import requests
import pytest

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.git_manager import GitManager, GitManagerError

pytestmark = pytest.mark.django_db


class TestGitManagerCheckRemoteRepositoryEmptyStatic:
    """Test cases for GitManager.check_remote_repository_empty_static"""

    def test_check_remote_repository_empty_static_dbt_project_exists(self):
        """Test when dbt_project.yml exists - repository is NOT empty"""
        remote_url = "https://github.com/user/dbt-repo.git"
        pat = "ghp_test_token"

        # Mock successful API response (file exists)
        mock_response = {"name": "dbt_project.yml", "type": "file"}

        with patch.object(GitManager, "_github_api_request", return_value=mock_response):
            result = GitManager.check_remote_repository_empty_static(remote_url, pat)

        # Should return False (not empty) when dbt_project.yml exists
        assert result is False

    def test_check_remote_repository_empty_static_dbt_project_not_found(self):
        """Test when dbt_project.yml doesn't exist - repository is empty"""
        remote_url = "https://github.com/user/empty-repo.git"
        pat = "ghp_test_token"

        # Mock 404 HTTPError (file not found)
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            result = GitManager.check_remote_repository_empty_static(remote_url, pat)

        # Should return True (empty) when dbt_project.yml doesn't exist
        assert result is True

    def test_check_remote_repository_empty_static_auth_error(self):
        """Test when authentication fails - should raise GitManagerError"""
        remote_url = "https://github.com/user/repo.git"
        pat = "invalid_token"

        # Mock 401 HTTPError (authentication failed)
        mock_response = Mock()
        mock_response.status_code = 401
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.check_remote_repository_empty_static(remote_url, pat)

            assert "Authentication failed" in str(excinfo.value)

    def test_check_remote_repository_empty_static_permission_error(self):
        """Test when access is forbidden - should raise GitManagerError"""
        remote_url = "https://github.com/private/repo.git"
        pat = "ghp_no_access_token"

        # Mock 403 HTTPError (access forbidden)
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.check_remote_repository_empty_static(remote_url, pat)

            assert "Access forbidden" in str(excinfo.value)

    def test_check_remote_repository_empty_static_server_error(self):
        """Test when GitHub API returns server error - should raise GitManagerError"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock 500 HTTPError (server error)
        mock_response = Mock()
        mock_response.status_code = 500
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.check_remote_repository_empty_static(remote_url, pat)

            assert "GitHub server error" in str(excinfo.value)
            assert "500" in str(excinfo.value)

    def test_check_remote_repository_empty_static_other_http_error(self):
        """Test other HTTP errors - should consider repository empty"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock 422 HTTPError (unprocessable entity)
        mock_response = Mock()
        mock_response.status_code = 422
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            result = GitManager.check_remote_repository_empty_static(remote_url, pat)

        # Should return True (empty) for other HTTP errors
        assert result is True

    def test_check_remote_repository_empty_static_network_error(self):
        """Test when network request fails - should raise GitManagerError"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock network error
        network_error = requests.RequestException("Connection timeout")

        with patch.object(GitManager, "_github_api_request", side_effect=network_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.check_remote_repository_empty_static(remote_url, pat)

            assert "Network error" in str(excinfo.value)
            assert "Connection timeout" in str(excinfo.value)

    def test_check_remote_repository_empty_static_no_pat(self):
        """Test when no PAT is provided - should work for public repos"""
        remote_url = "https://github.com/public/repo.git"

        # Mock successful API response (file exists in public repo)
        mock_response = {"name": "dbt_project.yml", "type": "file"}

        with patch.object(
            GitManager, "_github_api_request", return_value=mock_response
        ) as mock_api:
            result = GitManager.check_remote_repository_empty_static(remote_url, None)

        # Should return False (not empty) when dbt_project.yml exists
        assert result is False
        # Verify API was called with None PAT
        mock_api.assert_called_once_with(
            "https://api.github.com/repos/public/repo/contents/dbt_project.yml", None
        )

    def test_check_remote_repository_empty_static_invalid_github_url(self):
        """Test when invalid GitHub URL is provided"""
        remote_url = "https://gitlab.com/user/repo.git"  # Not GitHub
        pat = "ghp_test_token"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.check_remote_repository_empty_static(remote_url, pat)

        assert "Invalid GitHub URL" in str(excinfo.value)
        assert "Only GitHub URLs are supported" in str(excinfo.value)

    def test_check_remote_repository_empty_static_malformed_url(self):
        """Test when malformed GitHub URL is provided"""
        remote_url = "https://github.com/user"  # Missing repo name
        pat = "ghp_test_token"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.check_remote_repository_empty_static(remote_url, pat)

        assert "Invalid GitHub URL" in str(excinfo.value)


class TestGitManagerValidateRepositoryAccessStatic:
    """Test cases for GitManager.validate_repository_access"""

    def test_validate_repository_access_success_with_push_permission(self):
        """Test successful validation when PAT has push access"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock successful API response with push permissions
        mock_response = {"permissions": {"push": True, "pull": True, "admin": False}}

        with patch.object(GitManager, "_github_api_request", return_value=mock_response):
            result = GitManager.validate_repository_access(remote_url, pat)

        assert result is True

    def test_validate_repository_access_no_pat_provided(self):
        """Test validation fails when no PAT is provided"""
        remote_url = "https://github.com/user/repo.git"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.validate_repository_access(remote_url, None)

        assert "PAT not configured" in str(excinfo.value)
        assert "A Personal Access Token is required to verify remote URL" in str(excinfo.value)

    def test_validate_repository_access_empty_pat(self):
        """Test validation fails when empty PAT is provided"""
        remote_url = "https://github.com/user/repo.git"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.validate_repository_access(remote_url, "")

        assert "PAT not configured" in str(excinfo.value)

    def test_validate_repository_access_insufficient_permissions(self):
        """Test validation fails when PAT lacks push permissions"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_readonly_token"

        # Mock API response with only pull permissions
        mock_response = {"permissions": {"push": False, "pull": True, "admin": False}}

        with patch.object(GitManager, "_github_api_request", return_value=mock_response):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Insufficient permissions" in str(excinfo.value)
        assert "does not have write (push) access" in str(excinfo.value)

    def test_validate_repository_access_missing_permissions_key(self):
        """Test validation fails when API response lacks permissions key"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock API response without permissions key
        mock_response = {"name": "repo", "full_name": "user/repo"}

        with patch.object(GitManager, "_github_api_request", return_value=mock_response):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Insufficient permissions" in str(excinfo.value)

    def test_validate_repository_access_auth_failed_401(self):
        """Test validation fails with 401 authentication error"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_invalid_token"

        # Mock 401 HTTPError
        mock_response = Mock()
        mock_response.status_code = 401
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Authentication failed" in str(excinfo.value)
        assert "PAT token is invalid" in str(excinfo.value)

    def test_validate_repository_access_repo_not_found_404(self):
        """Test validation fails with 404 repository not found error"""
        remote_url = "https://github.com/user/nonexistent-repo.git"
        pat = "ghp_test_token"

        # Mock 404 HTTPError
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Repository not found" in str(excinfo.value)
        assert "does not exist or the PAT does not have access" in str(excinfo.value)

    def test_validate_repository_access_forbidden_403(self):
        """Test validation fails with 403 access forbidden error"""
        remote_url = "https://github.com/private/repo.git"
        pat = "ghp_insufficient_token"

        # Mock 403 HTTPError
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Access forbidden" in str(excinfo.value)
        assert "does not have sufficient permissions" in str(excinfo.value)

    def test_validate_repository_access_other_http_error(self):
        """Test validation fails with other HTTP errors"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock 422 HTTPError (unprocessable entity)
        mock_response = Mock()
        mock_response.status_code = 422
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "GitHub API error" in str(excinfo.value)
        assert "HTTP 422" in str(excinfo.value)

    def test_validate_repository_access_network_error(self):
        """Test validation fails with network/timeout error"""
        remote_url = "https://github.com/user/repo.git"
        pat = "ghp_test_token"

        # Mock network error
        network_error = requests.RequestException("Connection timeout")

        with patch.object(GitManager, "_github_api_request", side_effect=network_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.validate_repository_access(remote_url, pat)

        assert "Network error" in str(excinfo.value)
        assert "Connection timeout" in str(excinfo.value)

    def test_validate_repository_access_invalid_github_url(self):
        """Test validation fails with invalid GitHub URL"""
        remote_url = "https://gitlab.com/user/repo.git"  # Not GitHub
        pat = "ghp_test_token"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.validate_repository_access(remote_url, pat)

        assert "Invalid GitHub URL" in str(excinfo.value)
        assert "Only GitHub URLs are supported" in str(excinfo.value)


class TestGitManagerDeleteManagedRepositoryStatic:
    """Test cases for GitManager.delete_managed_repository"""

    def test_delete_managed_repository_success(self):
        """Test successful repository deletion"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"
        pat = "ghp_admin_token"

        # Mock successful deletion (DELETE returns no content)
        with patch.object(GitManager, "_github_api_request", return_value=None):
            result = GitManager.delete_managed_repository(remote_url, pat)

        assert result is True

    def test_delete_managed_repository_no_pat_provided(self):
        """Test deletion fails when no PAT is provided"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.delete_managed_repository(remote_url, None)

        assert "PAT not configured" in str(excinfo.value)
        assert "A Personal Access Token is required to delete repository" in str(excinfo.value)

    def test_delete_managed_repository_empty_pat(self):
        """Test deletion fails when empty PAT is provided"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.delete_managed_repository(remote_url, "")

        assert "PAT not configured" in str(excinfo.value)

    def test_delete_managed_repository_invalid_github_url(self):
        """Test deletion fails with non-GitHub URL"""
        remote_url = "https://gitlab.com/user/repo.git"  # Not GitHub
        pat = "ghp_admin_token"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.delete_managed_repository(remote_url, pat)

        assert "Invalid GitHub URL" in str(excinfo.value)
        assert "Only GitHub URLs are supported" in str(excinfo.value)

    def test_delete_managed_repository_malformed_url(self):
        """Test deletion fails with malformed GitHub URL"""
        remote_url = "https://github.com/dalgo"  # Missing repo name
        pat = "ghp_admin_token"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.delete_managed_repository(remote_url, pat)

        assert "Invalid GitHub URL" in str(excinfo.value)

    def test_delete_managed_repository_auth_failed_401(self):
        """Test deletion fails with 401 authentication error"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"
        pat = "ghp_invalid_token"

        # Mock 401 HTTPError
        mock_response = Mock()
        mock_response.status_code = 401
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.delete_managed_repository(remote_url, pat)

        assert "Authentication failed" in str(excinfo.value)
        assert "PAT token is invalid" in str(excinfo.value)

    def test_delete_managed_repository_not_found_404(self):
        """Test deletion fails with 404 repository not found error"""
        remote_url = "https://github.com/dalgo/dbt-nonexistent-repo.git"
        pat = "ghp_admin_token"

        # Mock 404 HTTPError
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.delete_managed_repository(remote_url, pat)

        assert "Repository not found" in str(excinfo.value)
        assert "does not exist or has already been deleted" in str(excinfo.value)

    def test_delete_managed_repository_forbidden_403(self):
        """Test deletion fails with 403 access forbidden error"""
        remote_url = "https://github.com/dalgo/dbt-protected-repo.git"
        pat = "ghp_insufficient_token"

        # Mock 403 HTTPError
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.delete_managed_repository(remote_url, pat)

        assert "Access forbidden" in str(excinfo.value)
        assert "does not have sufficient permissions to delete" in str(excinfo.value)

    def test_delete_managed_repository_other_http_error(self):
        """Test deletion fails with other HTTP errors"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"
        pat = "ghp_admin_token"

        # Mock 500 HTTPError (server error)
        mock_response = Mock()
        mock_response.status_code = 500
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.delete_managed_repository(remote_url, pat)

        assert "GitHub API error" in str(excinfo.value)
        assert "HTTP 500" in str(excinfo.value)

    def test_delete_managed_repository_network_error(self):
        """Test deletion fails with network/timeout error"""
        remote_url = "https://github.com/dalgo/dbt-test-org-dev.git"
        pat = "ghp_admin_token"

        # Mock network error
        network_error = requests.RequestException("Connection failed")

        with patch.object(GitManager, "_github_api_request", side_effect=network_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.delete_managed_repository(remote_url, pat)

        assert "Network error" in str(excinfo.value)
        assert "Connection failed" in str(excinfo.value)


class TestGitManagerCreateManagedRepositoryStatic:
    """Test cases for GitManager.create_managed_repository"""

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_success(self):
        """Test successful repository creation"""
        org_slug = "test-org"
        environment = "dev"

        # Mock successful API response
        mock_response = {
            "name": "dbt-test-org-dev",
            "full_name": "dalgo-test/dbt-test-org-dev",
            "clone_url": "https://github.com/dalgo-test/dbt-test-org-dev.git",
            "private": True,
        }

        with patch.object(GitManager, "_github_api_request", return_value=mock_response):
            result = GitManager.create_managed_repository(org_slug, environment)

        assert result["name"] == "dbt-test-org-dev"
        assert result["full_name"] == "dalgo-test/dbt-test-org-dev"
        assert result["private"] is True

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_proper_naming(self):
        """Test repository name follows correct format"""
        org_slug = "my-org"
        environment = "production"

        mock_response = {
            "name": "dbt-my-org-production",
            "full_name": "dalgo-test/dbt-my-org-production",
        }

        with patch.object(
            GitManager, "_github_api_request", return_value=mock_response
        ) as mock_api:
            GitManager.create_managed_repository(org_slug, environment)

            # Verify API was called with correct payload
            mock_api.assert_called_once()
            call_args = mock_api.call_args
            payload = call_args[1]["payload"]
            assert payload["name"] == "dbt-my-org-production"
            assert payload["description"] == "Managed dbt repository for my-org (production)"
            assert payload["private"] is True
            assert payload["auto_init"] is False

    @patch.dict(os.environ, {}, clear=True)
    def test_create_managed_repository_missing_github_org(self):
        """Test creation fails when DALGO_GITHUB_ORG is not set"""
        org_slug = "test-org"
        environment = "dev"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.create_managed_repository(org_slug, environment)

        assert "DALGO_GITHUB_ORG and DALGO_ORG_ADMIN_PAT must be set" in str(excinfo.value)

    @patch.dict(os.environ, {"DALGO_GITHUB_ORG": "dalgo-test"}, clear=True)
    def test_create_managed_repository_missing_admin_pat(self):
        """Test creation fails when DALGO_ORG_ADMIN_PAT is not set"""
        org_slug = "test-org"
        environment = "dev"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.create_managed_repository(org_slug, environment)

        assert "DALGO_GITHUB_ORG and DALGO_ORG_ADMIN_PAT must be set" in str(excinfo.value)

    @patch.dict(os.environ, {"DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}, clear=True)
    def test_create_managed_repository_missing_github_org_only(self):
        """Test creation fails when only DALGO_GITHUB_ORG is missing"""
        org_slug = "test-org"
        environment = "dev"

        with pytest.raises(GitManagerError) as excinfo:
            GitManager.create_managed_repository(org_slug, environment)

        assert "DALGO_GITHUB_ORG and DALGO_ORG_ADMIN_PAT must be set" in str(excinfo.value)

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_already_exists_422(self):
        """Test creation fails when repository already exists"""
        org_slug = "existing-org"
        environment = "dev"

        # Mock 422 HTTPError (repository already exists)
        mock_response = Mock()
        mock_response.status_code = 422
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.create_managed_repository(org_slug, environment)

        assert "Repository dbt-existing-org-dev already exists or name is invalid" in str(
            excinfo.value
        )

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_auth_failed_401(self):
        """Test creation fails with 401 authentication error"""
        org_slug = "test-org"
        environment = "dev"

        # Mock 401 HTTPError (authentication failed)
        mock_response = Mock()
        mock_response.status_code = 401
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.create_managed_repository(org_slug, environment)

        assert "Authentication failed - check PAT permissions" in str(excinfo.value)

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_forbidden_403(self):
        """Test creation fails with 403 insufficient permissions error"""
        org_slug = "test-org"
        environment = "dev"

        # Mock 403 HTTPError (insufficient permissions)
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.create_managed_repository(org_slug, environment)

        assert "Insufficient permissions to create repository in organization" in str(excinfo.value)

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_other_http_error(self):
        """Test creation fails with other HTTP errors"""
        org_slug = "test-org"
        environment = "dev"

        # Mock 500 HTTPError (server error)
        mock_response = Mock()
        mock_response.status_code = 500
        http_error = requests.HTTPError(response=mock_response)

        with patch.object(GitManager, "_github_api_request", side_effect=http_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.create_managed_repository(org_slug, environment)

        assert "GitHub API error: HTTP 500" in str(excinfo.value)

    @patch.dict(
        os.environ, {"DALGO_GITHUB_ORG": "dalgo-test", "DALGO_ORG_ADMIN_PAT": "ghp_admin_token"}
    )
    def test_create_managed_repository_network_error(self):
        """Test creation fails with network/timeout error"""
        org_slug = "test-org"
        environment = "dev"

        # Mock network error
        network_error = requests.RequestException("Connection timeout")

        with patch.object(GitManager, "_github_api_request", side_effect=network_error):
            with pytest.raises(GitManagerError) as excinfo:
                GitManager.create_managed_repository(org_slug, environment)

        assert "Network error: Failed to connect to GitHub API" in str(excinfo.value)
        assert "Connection timeout" in str(excinfo.value)
