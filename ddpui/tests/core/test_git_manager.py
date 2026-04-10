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


# =========================================================
# Tests for GitManager.__init__
# =========================================================
class TestGitManagerInit:
    """Tests for GitManager constructor."""

    def test_init_nonexistent_path_raises(self, tmp_path):
        """Non-existent path raises ValueError."""
        with pytest.raises(ValueError, match="Repository path does not exist"):
            GitManager("/nonexistent/path/abc123")

    def test_init_valid_path(self, tmp_path):
        """Valid existing path succeeds."""
        gm = GitManager(str(tmp_path))
        assert gm.repo_local_path == str(tmp_path)
        assert gm.pat is None

    def test_init_with_pat(self, tmp_path):
        """PAT is stored."""
        gm = GitManager(str(tmp_path), pat="mytoken")
        assert gm.pat == "mytoken"

    def test_init_validate_git_not_a_repo(self, tmp_path):
        """validate_git=True on a non-git dir raises GitManagerError."""
        with pytest.raises(GitManagerError, match="Not a git repository"):
            GitManager(str(tmp_path), validate_git=True)


# =========================================================
# Tests for GitManager._run_command
# =========================================================
class TestGitManagerRunCommand:
    """Tests for _run_command."""

    def test_run_command_success(self, tmp_path):
        gm = GitManager(str(tmp_path))
        result = gm._run_command(["echo", "hello"])
        assert result.stdout.strip() == "hello"

    def test_run_command_failure_raises(self, tmp_path):
        gm = GitManager(str(tmp_path))
        with pytest.raises(GitManagerError):
            gm._run_command(["git", "log"], check=True)

    def test_run_command_check_false_no_raise(self, tmp_path):
        gm = GitManager(str(tmp_path))
        result = gm._run_command(["git", "log"], check=False)
        assert result.returncode != 0

    @patch("ddpui.core.git_manager.subprocess.run")
    def test_run_command_exception(self, mock_run, tmp_path):
        """Exception during subprocess.run is wrapped."""
        mock_run.side_effect = OSError("Permission denied")
        gm = GitManager(str(tmp_path))
        with pytest.raises(GitManagerError, match="Failed to execute command"):
            gm._run_command(["git", "status"])


# =========================================================
# Tests for generate_oauth_url_static
# =========================================================
class TestGenerateOauthUrlStatic:
    """Tests for GitManager.generate_oauth_url_static."""

    def test_https_url(self):
        result = GitManager.generate_oauth_url_static("https://github.com/user/repo.git", "mytoken")
        assert result == "https://oauth2:mytoken@github.com/user/repo.git"

    def test_git_at_url(self):
        result = GitManager.generate_oauth_url_static("git@github.com:user/repo.git", "mytoken")
        assert result == "https://oauth2:mytoken@github.com/user/repo.git"

    def test_already_has_oauth(self):
        url = "https://oauth2:oldtoken@github.com/user/repo.git"
        result = GitManager.generate_oauth_url_static(url, "newtoken")
        assert result == url  # returned as-is

    def test_no_pat_raises(self):
        with pytest.raises(ValueError, match="PAT"):
            GitManager.generate_oauth_url_static("https://github.com/user/repo.git", "")

    def test_unsupported_format_raises(self):
        with pytest.raises(ValueError, match="Unsupported URL format"):
            GitManager.generate_oauth_url_static("ftp://github.com/user/repo.git", "token")


# =========================================================
# Tests for init_repo
# =========================================================
class TestInitRepo:
    """Tests for GitManager.init_repo."""

    def test_init_repo(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo("main")
        assert gm.is_git_initialized()
        assert gm.get_current_branch() == "main"

    def test_init_repo_custom_branch(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo("develop")
        assert gm.get_current_branch() == "develop"


# =========================================================
# Tests for is_git_initialized
# =========================================================
class TestIsGitInitialized:
    def test_not_initialized(self, tmp_path):
        gm = GitManager(str(tmp_path))
        assert gm.is_git_initialized() is False

    def test_initialized(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        assert gm.is_git_initialized() is True


# =========================================================
# Tests for has_commits
# =========================================================
class TestHasCommits:
    def test_no_commits(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        assert gm.has_commits() is False

    def test_with_commits(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        # Create a file and commit
        (tmp_path / "file.txt").write_text("hello")
        gm.commit_changes("initial commit")
        assert gm.has_commits() is True


# =========================================================
# Tests for commit_changes
# =========================================================
class TestCommitChanges:
    def test_nothing_to_commit(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("hi")
        gm.commit_changes("first")
        result = gm.commit_changes("second empty")
        assert "Nothing to commit" in result

    def test_commit_with_changes(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("data")
        result = gm.commit_changes("add file")
        assert "add file" in result or result != ""


# =========================================================
# Tests for parse_github_url_for_owner_and_repo
# =========================================================
class TestParseGithubUrl:
    def test_standard_url(self):
        owner, repo = GitManager.parse_github_url_for_owner_and_repo(
            "https://github.com/myorg/myrepo.git"
        )
        assert owner == "myorg"
        assert repo == "myrepo"

    def test_url_without_git_suffix(self):
        owner, repo = GitManager.parse_github_url_for_owner_and_repo(
            "https://github.com/myorg/myrepo"
        )
        assert owner == "myorg"
        assert repo == "myrepo"

    def test_www_github_url(self):
        owner, repo = GitManager.parse_github_url_for_owner_and_repo(
            "https://www.github.com/myorg/myrepo.git"
        )
        assert owner == "myorg"
        assert repo == "myrepo"

    def test_non_github_url_raises(self):
        with pytest.raises(GitManagerError, match="Invalid GitHub URL"):
            GitManager.parse_github_url_for_owner_and_repo("https://gitlab.com/user/repo")

    def test_malformed_url_raises(self):
        with pytest.raises(GitManagerError, match="Invalid GitHub URL"):
            GitManager.parse_github_url_for_owner_and_repo("https://github.com/onlyowner")


# =========================================================
# Tests for verify_remote_url
# =========================================================
class TestVerifyRemoteUrl:
    def test_no_pat_raises(self, tmp_path):
        gm = GitManager(str(tmp_path), pat=None)
        with pytest.raises(GitManagerError, match="PAT not configured"):
            gm.verify_remote_url("https://github.com/user/repo.git")

    @patch.object(GitManager, "_github_api_request")
    def test_push_access_true(self, mock_api, tmp_path):
        mock_api.return_value = {"permissions": {"push": True, "pull": True}}
        gm = GitManager(str(tmp_path), pat="token")
        assert gm.verify_remote_url("https://github.com/user/repo.git") is True

    @patch.object(GitManager, "_github_api_request")
    def test_no_push_access_raises(self, mock_api, tmp_path):
        mock_api.return_value = {"permissions": {"push": False, "pull": True}}
        gm = GitManager(str(tmp_path), pat="token")
        with pytest.raises(GitManagerError, match="Insufficient permissions"):
            gm.verify_remote_url("https://github.com/user/repo.git")

    @patch.object(GitManager, "_github_api_request")
    def test_401_error(self, mock_api, tmp_path):
        resp = Mock()
        resp.status_code = 401
        mock_api.side_effect = requests.HTTPError(response=resp)
        gm = GitManager(str(tmp_path), pat="badtoken")
        with pytest.raises(GitManagerError, match="Authentication failed"):
            gm.verify_remote_url("https://github.com/user/repo.git")

    @patch.object(GitManager, "_github_api_request")
    def test_404_error(self, mock_api, tmp_path):
        resp = Mock()
        resp.status_code = 404
        mock_api.side_effect = requests.HTTPError(response=resp)
        gm = GitManager(str(tmp_path), pat="token")
        with pytest.raises(GitManagerError, match="Repository not found"):
            gm.verify_remote_url("https://github.com/user/repo.git")

    @patch.object(GitManager, "_github_api_request")
    def test_403_error(self, mock_api, tmp_path):
        resp = Mock()
        resp.status_code = 403
        mock_api.side_effect = requests.HTTPError(response=resp)
        gm = GitManager(str(tmp_path), pat="token")
        with pytest.raises(GitManagerError, match="Access forbidden"):
            gm.verify_remote_url("https://github.com/user/repo.git")

    @patch.object(GitManager, "_github_api_request")
    def test_network_error(self, mock_api, tmp_path):
        mock_api.side_effect = requests.RequestException("timeout")
        gm = GitManager(str(tmp_path), pat="token")
        with pytest.raises(GitManagerError, match="Network error"):
            gm.verify_remote_url("https://github.com/user/repo.git")


# =========================================================
# Tests for is_file_modified
# =========================================================
class TestIsFileModified:
    def test_unmodified_file(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("content")
        gm.commit_changes("init")
        assert gm.is_file_modified("f.txt") is False

    def test_modified_file(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("content")
        gm.commit_changes("init")
        (tmp_path / "f.txt").write_text("new content")
        assert gm.is_file_modified("f.txt") is True

    def test_untracked_file(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("content")
        gm.commit_changes("init")
        (tmp_path / "new.txt").write_text("new")
        assert gm.is_file_modified("new.txt") is True

    def test_not_git_repo_returns_none(self, tmp_path):
        gm = GitManager(str(tmp_path))
        result = gm.is_file_modified("anything.txt")
        assert result is None


# =========================================================
# Tests for get_raw_status and get_changes_summary
# =========================================================
class TestGetRawStatusAndSummary:
    def test_empty_repo_no_changes(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        summary = gm.get_changes_summary()
        assert summary.added == []
        assert summary.modified == []
        assert summary.deleted == []

    def test_untracked_file_shows_added(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        (tmp_path / "new.txt").write_text("new")
        summary = gm.get_changes_summary()
        assert "new.txt" in summary.added

    def test_modified_file_shows_modified(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        (tmp_path / "f.txt").write_text("changed")
        summary = gm.get_changes_summary()
        assert "f.txt" in summary.modified

    def test_deleted_file_shows_deleted(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        os.remove(tmp_path / "f.txt")
        summary = gm.get_changes_summary()
        assert "f.txt" in summary.deleted

    def test_not_git_repo_empty_status(self, tmp_path):
        gm = GitManager(str(tmp_path))
        raw = gm.get_raw_status()
        assert raw == ""


# =========================================================
# Tests for get_changed_files_list
# =========================================================
class TestGetChangedFilesList:
    def test_no_changes(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        assert gm.get_changed_files_list() == []

    def test_mixed_changes(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        gm.commit_changes("init")
        (tmp_path / "a.txt").write_text("modified")
        (tmp_path / "c.txt").write_text("new")
        os.remove(tmp_path / "b.txt")
        changed = gm.get_changed_files_list()
        filenames = {f.filename for f in changed}
        statuses = {f.filename: f.status for f in changed}
        assert "a.txt" in filenames
        assert statuses["a.txt"] == "modified"
        assert "c.txt" in filenames
        assert statuses["c.txt"] == "added"
        assert "b.txt" in filenames
        assert statuses["b.txt"] == "deleted"


# =========================================================
# Tests for get_ahead_behind
# =========================================================
class TestGetAheadBehind:
    def test_no_remote_returns_zero(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        (tmp_path / "f.txt").write_text("x")
        gm.commit_changes("init")
        ahead, behind = gm.get_ahead_behind()
        assert ahead == 0
        assert behind == 0


# =========================================================
# Tests for set_remote and get_remote_url
# =========================================================
class TestRemote:
    def test_set_and_get_remote(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        gm.set_remote("https://github.com/user/repo.git")
        url = gm.get_remote_url()
        assert url == "https://github.com/user/repo.git"

    def test_update_remote(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        gm.set_remote("https://github.com/user/repo1.git")
        gm.set_remote("https://github.com/user/repo2.git")
        url = gm.get_remote_url()
        assert url == "https://github.com/user/repo2.git"

    def test_get_remote_no_remote_raises(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        with pytest.raises(GitManagerError):
            gm.get_remote_url()


# =========================================================
# Tests for get_remote_default_branch
# =========================================================
class TestGetRemoteDefaultBranch:
    def test_no_remote_returns_none(self, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo()
        result = gm.get_remote_default_branch()
        # No remote configured, should return None
        assert result is None


# =========================================================
# Tests for sync_local_default_to_remote
# =========================================================
class TestSyncLocalDefaultToRemote:
    @patch.object(GitManager, "get_remote_default_branch", return_value=None)
    def test_remote_empty(self, mock_branch, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo("main")
        result = gm.sync_local_default_to_remote()
        assert "remote is empty" in result

    @patch.object(GitManager, "get_remote_default_branch", return_value="main")
    def test_already_matching(self, mock_branch, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo("main")
        result = gm.sync_local_default_to_remote()
        assert "already" in result

    @patch.object(GitManager, "get_remote_default_branch", return_value="develop")
    def test_rename_branch(self, mock_branch, tmp_path):
        gm = GitManager(str(tmp_path))
        gm.init_repo("main")
        result = gm.sync_local_default_to_remote()
        assert "renamed" in result
        assert gm.get_current_branch() == "develop"


# =========================================================
# Tests for GitManagerError
# =========================================================
class TestGitManagerError:
    def test_error_with_message_and_error(self):
        err = GitManagerError("Something failed", "detail here")
        assert "Something failed" in str(err)
        assert "detail here" in str(err)
        assert err.message == "Something failed"
        assert err.error == "detail here"

    def test_error_with_message_only(self):
        err = GitManagerError("Just a message")
        assert str(err) == "Just a message"
        assert err.error == ""
