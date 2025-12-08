import os
import subprocess


class GitManagerError(Exception):
    """Exception raised when a git command fails"""

    def __init__(self, message: str, error: str = ""):
        self.message = message
        self.error = error
        super().__init__(f"{message}: {error}" if error else message)


class GitManager:
    def __init__(self, repo_local_path: str, pat: str = None):
        self.repo_local_path = repo_local_path
        if not os.path.exists(repo_local_path):
            raise ValueError("Repository path does not exist")
        self.pat = pat  # Personal Access Token for authentication if needed

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

    def generate_oauth_url(self, repo_url: str) -> str:
        """
        Generate a Git URL with OAuth token embedded for authentication.

        Converts:
          https://github.com/user/repo.git
        To:
          https://<token>@github.com/user/repo.git
        """
        if not self.pat:
            raise ValueError("PAT (Personal Access Token) is not set")

        # Handle both https:// and git@ formats
        if repo_url.startswith("https://"):
            # Insert token after https://
            return repo_url.replace("https://", f"https://{self.pat}@")
        elif repo_url.startswith("git@"):
            # Convert git@github.com:user/repo.git to https://<token>@github.com/user/repo.git
            # git@github.com:user/repo.git -> github.com/user/repo.git
            url_part = repo_url.replace("git@", "").replace(":", "/")
            return f"https://{self.pat}@{url_part}"
        else:
            raise ValueError(f"Unsupported URL format: {repo_url}")

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
