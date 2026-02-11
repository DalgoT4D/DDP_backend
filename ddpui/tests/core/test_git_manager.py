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
