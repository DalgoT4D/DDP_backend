import os
import subprocess


class GitManager:
    def __init__(self, repo_local_path: str, pat: str = None):
        self.repo_local_path = repo_local_path
        if not os.path.exists(repo_local_path):
            raise ValueError("Repository path does not exist")
        self.pat = pat  # Personal Access Token for authentication if needed

    def init_repo(self, default_branch: str = "main"):
        """Initialize a git repository with a specified default branch"""
        subprocess.run(["git", "init"], cwd=self.repo_local_path, check=True)
        # Rename the default branch if needed
        subprocess.run(
            ["git", "branch", "-M", default_branch],
            cwd=self.repo_local_path,
            check=True,
        )

    def is_git_initialized(self) -> bool:
        """Check if the folder is a git repository"""
        try:
            subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                cwd=self.repo_local_path,
                check=True,
                capture_output=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    def get_current_branch(self) -> str:
        """Get the name of the current branch (works even with no commits)"""
        # Try symbolic-ref first (works for new repos with no commits)
        result = subprocess.run(
            ["git", "symbolic-ref", "--short", "HEAD"],
            cwd=self.repo_local_path,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()

        # Fallback to rev-parse (works after first commit)
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=self.repo_local_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def has_commits(self) -> bool:
        """Check if the repository has any commits"""
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repo_local_path,
            capture_output=True,
        )
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
        # subprocess.run(["git", "fetch", remote], cwd=self.repo_local_path, check=True)

        result = subprocess.run(
            ["git", "rev-list", "--left-right", "--count", f"{branch}...{remote}/{branch}"],
            cwd=self.repo_local_path,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            # Remote branch might not exist yet
            return (0, 0)

        # Output format: "ahead\tbehind"
        parts = result.stdout.strip().split()
        ahead = int(parts[0]) if len(parts) > 0 else 0
        behind = int(parts[1]) if len(parts) > 1 else 0

        return (ahead, behind)

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

    def set_remote(self, remote_url: str, remote_name: str = "origin"):
        """Set or update the remote repository URL"""
        # Check if remote already exists
        result = subprocess.run(
            ["git", "remote", "get-url", remote_name],
            cwd=self.repo_local_path,
            capture_output=True,
        )

        if result.returncode == 0:
            # Remote exists, update it
            subprocess.run(
                ["git", "remote", "set-url", remote_name, remote_url],
                cwd=self.repo_local_path,
                check=True,
            )
        else:
            # Remote doesn't exist, add it
            subprocess.run(
                ["git", "remote", "add", remote_name, remote_url],
                cwd=self.repo_local_path,
                check=True,
            )

    def set_branch_upstream(self, remote: str = "origin", branch: str = None):
        """
        Set the upstream tracking branch for the current local branch.
        If branch is not specified, uses the current branch name.
        """
        if branch is None:
            branch = self.get_current_branch()

        subprocess.run(
            ["git", "branch", "--set-upstream-to", f"{remote}/{branch}"],
            cwd=self.repo_local_path,
            check=True,
        )

    def commit_changes(
        self,
        message: str,
        user_name: str = "support@dalgo.org",
        user_email: str = "support@dalgo.org",
    ):
        """
        Commit changes with specified user name and email.
        """
        subprocess.run(["git", "add", "."], cwd=self.repo_local_path, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                f"user.name={user_name}",
                "-c",
                f"user.email={user_email}",
                "commit",
                "-m",
                message,
            ],
            cwd=self.repo_local_path,
            check=True,
        )

    def push_changes(self, remote: str = "origin", branch: str = None, set_upstream: bool = None):
        """
        Push changes to the remote repository.
        If set_upstream is None, automatically sets upstream on first push (when upstream is not set).
        If set_upstream is True/False, uses that value explicitly.
        """
        if branch is None:
            branch = self.get_current_branch()

        # Determine if we need to set upstream
        if set_upstream is None:
            # Check if upstream is already set
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", f"{branch}@{{upstream}}"],
                cwd=self.repo_local_path,
                capture_output=True,
            )
            needs_upstream = result.returncode != 0
        else:
            needs_upstream = set_upstream

        if self.pat:
            # Get the current remote URL and convert to authenticated URL
            result = subprocess.run(
                ["git", "remote", "get-url", remote],
                cwd=self.repo_local_path,
                capture_output=True,
                text=True,
                check=True,
            )
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

        subprocess.run(cmd, cwd=self.repo_local_path, check=True)

    def pull_changes(self, remote: str = "origin", branch: str = None):
        """Pull changes from the remote repository"""
        if branch is None:
            branch = self.get_current_branch()

        if self.pat:
            # Get the current remote URL and convert to authenticated URL
            result = subprocess.run(
                ["git", "remote", "get-url", remote],
                cwd=self.repo_local_path,
                capture_output=True,
                text=True,
                check=True,
            )
            remote_url = result.stdout.strip()
            auth_url = self.generate_oauth_url(remote_url)
            cmd = ["git", "pull", auth_url, branch]
        else:
            cmd = ["git", "pull", remote, branch]

        subprocess.run(cmd, cwd=self.repo_local_path, check=True)
