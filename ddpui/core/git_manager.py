import logging
import os
import subprocess
from typing import List, Literal
from urllib.parse import urlparse

import requests
from ninja import Schema

logger = logging.getLogger(__name__)


class GitChangedFile(Schema):
    """Schema for a git changed file with its status"""

    filename: str
    status: Literal["added", "modified", "deleted"]


class GitStatusSummary(Schema):
    """Schema for git status summary"""

    added: List[str]
    modified: List[str]
    deleted: List[str]


class GitManagerError(Exception):
    """Exception raised when a git command fails"""

    def __init__(self, message: str, error: str = ""):
        self.message = message
        self.error = error
        super().__init__(f"{message}: {error}" if error else message)


class GitManager:
    def __init__(self, repo_local_path: str, pat: str = None, validate_git: bool = False):
        """
        Validate if the folder is a git repository if validate_git is True.
        :param repo_local_path: Local path to the git repository
        :param pat: Personal Access Token for authentication if needed
        :param validate_git: If True, checks if the folder is a git repository
        """
        self.repo_local_path = repo_local_path
        if not os.path.exists(repo_local_path):
            raise ValueError("Repository path does not exist")
        self.pat = pat  # Personal Access Token for authentication if needed
        if validate_git and not self.is_git_initialized():
            raise GitManagerError(
                message="Not a git repository",
                error=f"The folder {repo_local_path} is not a git repository",
            )

    def _run_command(self, cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        """
        Run a git command and return the full result.
        If check=True (default), raises GitManagerError on failure.
        If check=False, returns result even on failure.
        """
        try:
            result = subprocess.run(
                cmd,
                cwd=self.repo_local_path,
                capture_output=True,
                text=True,
            )
        except Exception as e:
            raise GitManagerError(
                message="Failed to execute command",
                error=str(e),
            )

        if check and result.returncode != 0:
            raise GitManagerError(
                message=result.stdout.strip() or "Command failed",
                error=result.stderr.strip(),
            )
        return result

    @staticmethod
    def generate_oauth_url_static(repo_url: str, pat: str) -> str:
        """
        Generate a Git URL with OAuth token embedded for authentication.

        Converts:
          https://github.com/user/repo.git
        To:
          https://oauth2:<token>@github.com/user/repo.git
        """
        if not pat:
            raise ValueError("PAT (Personal Access Token) is not set")

        # If URL already has oauth2 credentials, return as-is
        if "oauth2:" in repo_url:
            return repo_url

        # Handle both https:// and git@ formats
        if repo_url.startswith("https://"):
            # Insert oauth2:token after https://
            return repo_url.replace("https://", f"https://oauth2:{pat}@")
        elif repo_url.startswith("git@"):
            # Convert git@github.com:user/repo.git to https://oauth2:<token>@github.com/user/repo.git
            # git@github.com:user/repo.git -> github.com/user/repo.git
            url_part = repo_url.replace("git@", "").replace(":", "/")
            return f"https://oauth2:{pat}@{url_part}"
        else:
            raise ValueError(f"Unsupported URL format: {repo_url}")

    def init_repo(self, default_branch: str = "main") -> str:
        """Initialize a git repository with a specified default branch"""
        self._run_command(["git", "init"])
        result = self._run_command(["git", "branch", "-M", default_branch])
        return result.stdout.strip()

    def is_git_initialized(self) -> bool:
        """Check if the folder is a git repository"""
        result = self._run_command(["git", "rev-parse", "--git-dir"], check=False)
        return result.returncode == 0

    def get_current_branch(self) -> str:
        """Get the name of the current branch (works even with no commits)"""
        # Try symbolic-ref first (works for new repos with no commits)
        result = self._run_command(["git", "symbolic-ref", "--short", "HEAD"], check=False)
        if result.returncode == 0:
            return result.stdout.strip()

        # Fallback to rev-parse (works after first commit)
        result = self._run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        return result.stdout.strip()

    def has_commits(self) -> bool:
        """Check if the repository has any commits"""
        result = self._run_command(["git", "rev-parse", "HEAD"], check=False)
        return result.returncode == 0

    def get_ahead_behind(self, remote: str = "origin", branch: str = None) -> tuple[int, int]:
        """
        Get the number of commits ahead and behind the remote branch.
        Returns a tuple (ahead, behind).

        - ahead: commits in local that are not in remote
        - behind: commits in remote that are not in local
        """
        if branch is None:
            branch = self.get_current_branch()

        # Fetch latest from remote first (optional, but recommended for accurate count)
        # self._run_command(["git", "fetch", remote], check=False)

        result = self._run_command(
            ["git", "rev-list", "--left-right", "--count", f"{branch}...{remote}/{branch}"],
            check=False,
        )

        if result.returncode != 0:
            # Remote branch might not exist yet
            return (0, 0)

        # Output format: "ahead\tbehind"
        try:
            parts = result.stdout.strip().split()
            ahead = int(parts[0]) if len(parts) > 0 else 0
            behind = int(parts[1]) if len(parts) > 1 else 0
            return (ahead, behind)
        except (ValueError, IndexError) as e:
            raise GitManagerError(
                message="Failed to parse ahead/behind count",
                error=f"Unexpected output format: {result.stdout.strip()}",
            )

    @classmethod
    def clone(
        cls, cwd: str, remote_repo_url: str, relative_path: str, pat: str = None
    ) -> "GitManager":
        """
        Clone a repository and return a GitManager instance for it.

        :param cwd: Working directory where the clone command will be executed
        :param remote_repo_url: URL of the repository to clone
        :param pat: Personal Access Token for authentication
        :param relative_path: Relative path (from cwd) where the repo will be cloned
        :return: GitManager instance for the cloned repository
        """
        # Build authenticated URL if PAT provided
        clone_url = cls.generate_oauth_url_static(remote_repo_url, pat) if pat else remote_repo_url

        try:
            result = subprocess.run(
                ["git", "clone", clone_url, relative_path],
                cwd=cwd,
                capture_output=True,
                text=True,
            )
        except Exception as e:
            raise GitManagerError(
                message="Failed to clone repository",
                error=str(e),
            )

        if result.returncode != 0:
            raise GitManagerError(
                message="Failed to clone repository",
                error=result.stderr.strip(),
            )

        target_path = os.path.join(cwd, relative_path)
        instance = cls(repo_local_path=target_path, pat=pat)

        # Reset remote to clean URL (without credentials) if PAT was used
        if pat:
            instance.set_remote(remote_repo_url)

        return instance

    def generate_oauth_url(self, repo_url: str) -> str:
        """
        Generate a Git URL with OAuth token embedded for authentication.
        Instance method wrapper around generate_oauth_url_static.
        """
        return self.generate_oauth_url_static(repo_url, self.pat)

    def set_remote(self, remote_url: str, remote_name: str = "origin") -> str:
        """Set or update the remote repository URL"""
        # Check if remote already exists
        result = self._run_command(["git", "remote", "get-url", remote_name], check=False)

        if result.returncode == 0:
            # Remote exists, update it
            result = self._run_command(["git", "remote", "set-url", remote_name, remote_url])
            return result.stdout.strip()
        else:
            # Remote doesn't exist, add it
            result = self._run_command(["git", "remote", "add", remote_name, remote_url])
            return result.stdout.strip()

    def get_remote_url(self, remote_name: str = "origin") -> str:
        """
        Return the URL for the named remote (defaults to 'origin').
        Runs: git remote get-url <remote_name>
        """
        try:
            res = self._run_command(["git", "remote", "get-url", remote_name], check=False)
            if res.returncode != 0:
                raise GitManagerError(
                    message=f"Failed to get URL for remote '{remote_name}'",
                    error=(res.stderr or res.stdout or "").strip(),
                )
            return res.stdout.strip()
        except GitManagerError:
            raise
        except Exception as e:
            raise GitManagerError(message="Error getting remote url", error=str(e)) from e

    def set_branch_upstream(self, remote: str = "origin", branch: str = None) -> str:
        """
        Set the upstream tracking branch for the current local branch.
        If branch is not specified, uses the current branch name.
        """
        if branch is None:
            branch = self.get_current_branch()

        result = self._run_command(["git", "branch", "--set-upstream-to", f"{remote}/{branch}"])
        return result.stdout.strip()

    def commit_changes(
        self,
        message: str,
        user_name: str = "support@dalgo.org",
        user_email: str = "support@dalgo.org",
    ) -> str:
        """
        Commit changes with specified user name and email.
        Returns the commit output message.
        """
        self._run_command(["git", "add", "."])

        # Check if there are changes to commit
        result = self._run_command(["git", "diff", "--cached", "--quiet"], check=False)
        if result.returncode == 0:
            return "Nothing to commit, working tree clean"

        result = self._run_command(
            [
                "git",
                "-c",
                f"user.name={user_name}",
                "-c",
                f"user.email={user_email}",
                "commit",
                "-m",
                message,
            ]
        )
        return result.stdout.strip()

    def push_changes(
        self, remote: str = "origin", branch: str = None, set_upstream: bool = None
    ) -> str:
        """
        Push changes to the remote repository.
        If set_upstream is None, automatically sets upstream on first push (when upstream is not set).
        If set_upstream is True/False, uses that value explicitly.
        Returns the push output message.
        """
        if branch is None:
            branch = self.get_current_branch()

        # Determine if we need to set upstream
        if set_upstream is None:
            # Check if upstream is already set
            result = self._run_command(
                ["git", "rev-parse", "--abbrev-ref", f"{branch}@{{upstream}}"],
                check=False,
            )
            needs_upstream = result.returncode != 0
        else:
            needs_upstream = set_upstream

        if self.pat:
            # Get the current remote URL and convert to authenticated URL
            result = self._run_command(["git", "remote", "get-url", remote])
            remote_url = result.stdout.strip()
            auth_url = self.generate_oauth_url(remote_url)

            cmd = ["git", "push"]
            if needs_upstream:
                cmd.append("-u")
            cmd.extend([auth_url, branch])
        else:
            cmd = ["git", "push"]
            if needs_upstream:
                cmd.append("-u")
            cmd.extend([remote, branch])

        result = self._run_command(cmd)
        return result.stdout.strip()

    def pull_changes(self, remote: str = "origin", branch: str = None) -> str:
        """Pull changes from the remote repository. Returns the pull output message."""
        if branch is None:
            branch = self.get_current_branch()

        if self.pat:
            # Get the current remote URL and convert to authenticated URL
            result = self._run_command(["git", "remote", "get-url", remote])
            remote_url = result.stdout.strip()
            auth_url = self.generate_oauth_url(remote_url)
            cmd = ["git", "pull", auth_url, branch]
        else:
            cmd = ["git", "pull", remote, branch]

        result = self._run_command(cmd)
        return result.stdout.strip()

    @staticmethod
    def parse_github_url_for_owner_and_repo(remote_url: str) -> tuple[str, str]:
        """
        Parse a GitHub URL to extract owner and repo name.

        :param remote_url: GitHub URL (e.g., https://github.com/owner/repo.git)
        :return: Tuple of (owner, repo)
        :raises GitManagerError: If URL is not a valid GitHub URL
        """
        parsed = urlparse(remote_url)

        if parsed.hostname not in ("github.com", "www.github.com"):
            raise GitManagerError(
                message="Invalid GitHub URL",
                error="Only GitHub URLs are supported (github.com)",
            )

        path_parts = parsed.path.strip("/").split("/")
        if len(path_parts) < 2:
            raise GitManagerError(
                message="Invalid GitHub URL",
                error="URL must be in format: https://github.com/owner/repo",
            )

        owner = path_parts[0]
        repo = path_parts[1].removesuffix(".git")

        return owner, repo

    @staticmethod
    def _github_api_request(url: str, pat: str = None, timeout: int = 30) -> dict:
        """
        Common function to make GitHub API requests.

        :param url: The GitHub API URL to request
        :param pat: Personal Access Token for authentication (optional)
        :param timeout: Request timeout in seconds
        :return: JSON response data
        :raises requests.HTTPError: For HTTP errors
        :raises requests.RequestException: For network/timeout errors
        """
        headers = {
            "Accept": "application/vnd.github+json",
        }
        if pat:
            headers["Authorization"] = f"Bearer {pat}"

        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def verify_remote_url(self, remote_url: str) -> bool:
        """
        Verify that the PAT has push (write) access to the remote repository.
        Uses GitHub API to check permissions directly.

        :param remote_url: The remote repository URL to verify
        :return: True if the PAT has push access, raises GitManagerError otherwise
        """
        if not self.pat:
            raise GitManagerError(
                message="PAT not configured",
                error="A Personal Access Token is required to verify remote URL",
            )

        owner, repo = self.parse_github_url_for_owner_and_repo(remote_url)

        try:
            data = self._github_api_request(
                f"https://api.github.com/repos/{owner}/{repo}", self.pat
            )
        except requests.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 401:
                raise GitManagerError(
                    message="Authentication failed",
                    error="The PAT token is invalid",
                ) from e
            if status_code == 404:
                raise GitManagerError(
                    message="Repository not found",
                    error="The repository does not exist or the PAT does not have access to it",
                ) from e
            if status_code == 403:
                raise GitManagerError(
                    message="Access forbidden",
                    error="The PAT does not have sufficient permissions",
                ) from e
            raise GitManagerError(
                message="GitHub API error",
                error=f"HTTP {status_code}: {str(e)}",
            ) from e
        except requests.RequestException as e:
            raise GitManagerError(
                message="Network error",
                error=f"Failed to connect to GitHub API: {str(e)}",
            ) from e

        permissions = data.get("permissions", {})

        if not permissions.get("push", False):
            raise GitManagerError(
                message="Insufficient permissions",
                error="The PAT does not have write (push) access to this repository",
            )

        return True

    def is_file_modified(self, file_path: str) -> bool:
        """
        Check if a file has been modified, is untracked, or staged since the last commit.

        :param file_path: Path to the file relative to the git repo root
        :return: True if file has any changes, False if clean, None if cannot determine
        """
        if not self.is_git_initialized():
            return None

        try:
            result = self._run_command(["git", "status", "--porcelain", file_path], check=False)
            return len(result.stdout.strip()) > 0

        except Exception as e:
            logger.error(f"Error checking file modification status for {file_path}: {e}")
            return None

    def get_raw_status(self) -> str:
        """
        Get raw git status output directly from command.
        Returns exactly what 'git status --porcelain' outputs.

        :return: Raw git status output
        """
        if not self.is_git_initialized():
            return ""

        try:
            result = self._run_command(["git", "status", "--porcelain"], check=False)
            return result.stdout if result.returncode == 0 else ""
        except Exception as e:
            logger.error(f"Error getting raw git status: {e}")
            return ""

    def get_changes_summary(self) -> GitStatusSummary:
        """
        Parse git status and construct a nice summary message.
        Shows which files will be added, modified, or deleted.

        :return: User-friendly summary of changes
        """
        raw_status = self.get_raw_status()
        if not raw_status.strip():
            return GitStatusSummary(added=[], modified=[], deleted=[])

        added = []
        modified = []
        deleted = []

        for line in raw_status.strip().split("\n"):
            if len(line) < 3:
                continue

            status_code = line[:2]
            filepath = line[2:].lstrip()

            # Parse git status codes
            if status_code in ["A ", "??"]:  # Added or untracked
                added.append(filepath)
            elif "M" in status_code:  # Modified (staged or unstaged)
                modified.append(filepath)
            elif "D" in status_code:  # Deleted
                deleted.append(filepath)

        return GitStatusSummary(added=added, modified=modified, deleted=deleted)

    def get_changed_files_list(self) -> List[GitChangedFile]:
        """
        Get list of all changed files with their status.
        Perfect for comparing with nodes to mark them published/unpublished.

        :return: List of GitChangedFile objects with filename and status
                 status values: 'added', 'modified', 'deleted'
        """
        raw_status = self.get_raw_status()
        if not raw_status.strip():
            return []

        changed_files = []

        for line in raw_status.strip().split("\n"):
            if len(line) < 3:
                continue

            status_code = line[:2]
            filepath = line[2:].lstrip()

            # Map git status codes to our status values
            if status_code in ["A ", "??"]:  # Added or untracked
                status = "added"
            elif "M" in status_code:  # Modified (staged or unstaged)
                status = "modified"
            elif "D" in status_code:  # Deleted
                status = "deleted"
            else:
                continue  # Skip other statuses

            changed_files.append(GitChangedFile(filename=filepath, status=status))

        return changed_files

    def get_remote_default_branch(self, remote: str = "origin") -> str | None:
        """
        Determine the remote's default branch.

        1) Try `git ls-remote --symref <remote> HEAD` and parse the symbolic ref.
        2) Returns None if the remote is empty (no commits yet).

        :return: Branch name or None if remote is empty
        """
        # Try parsing via git ls-remote --symref
        result = self._run_command(["git", "ls-remote", "--symref", remote, "HEAD"], check=False)
        if result.returncode == 0 and result.stdout:
            for line in result.stdout.splitlines():
                line = line.strip()
                # Example line: "ref: refs/heads/main\tHEAD"
                if line.startswith("ref: "):
                    parts = line.split()
                    if len(parts) >= 2:
                        ref = parts[1]
                        if ref.startswith("refs/heads/"):
                            return ref.split("/")[-1]

        # Remote is empty (no commits) - return None
        return None

    def sync_local_default_to_remote(self, remote: str = "origin") -> str:
        """
        Ensure local default branch name matches remote default branch by renaming
        the current local branch to the remote default (uses `git branch -M`).

        This implementation only renames the local branch; it does not checkout,
        set upstream or push.

        If the remote is empty (no commits), keeps the local branch name as-is.
        """
        remote_default = self.get_remote_default_branch(remote)
        local_current = self.get_current_branch()

        # Remote is empty (new repo with no commits) - keep local branch name
        if remote_default is None:
            return f"remote is empty, keeping local branch '{local_current}'"

        if local_current == remote_default:
            return f"local branch already '{remote_default}'"

        # rename current branch to remote_default (force)
        self._run_command(["git", "branch", "-M", remote_default])

        return f"renamed local branch '{local_current}' -> '{remote_default}'"

    @staticmethod
    def check_remote_repository_empty_static(remote_url: str, pat: str = None) -> bool:
        """
        Check if a remote repository is "empty" from a dbt perspective -
        meaning it doesn't contain dbt_project.yml file.

        :param remote_url: The remote repository URL
        :param pat: Personal Access Token for authentication
        :return: True if no dbt_project.yml file found (empty), False if dbt project exists
        :raises GitManagerError: For auth errors, network errors, or 5xx server errors
        """
        owner, repo = GitManager.parse_github_url_for_owner_and_repo(remote_url)

        # Check for dbt_project.yml only
        try:
            GitManager._github_api_request(
                f"https://api.github.com/repos/{owner}/{repo}/contents/dbt_project.yml", pat
            )
            # If we find dbt_project.yml, repository is NOT empty
            return False
        except requests.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 404:
                # File doesn't exist, repository is considered empty
                return True
            elif status_code == 401:
                raise GitManagerError(
                    message="Authentication failed",
                    error="The PAT token is invalid",
                ) from e
            elif status_code == 403:
                raise GitManagerError(
                    message="Access forbidden",
                    error="The PAT does not have sufficient permissions to access this repository",
                ) from e
            elif status_code >= 500:
                raise GitManagerError(
                    message="GitHub server error",
                    error=f"GitHub API returned {status_code}: {str(e)}",
                ) from e
            else:
                # For other HTTP errors, consider repository empty
                return True
        except requests.RequestException as e:
            raise GitManagerError(
                message="Network error",
                error=f"Failed to connect to GitHub API: {str(e)}",
            ) from e
