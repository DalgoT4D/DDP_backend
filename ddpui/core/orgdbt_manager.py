import os
import subprocess
from pathlib import Path
from typing import Union

from ninja.errors import HttpError
from ddpui.models.org import Org, OrgDbt
from ddpui.ddpdbt.schema import DbtProjectParams


class DbtCommandError(Exception):
    """Exception raised when a dbt command fails"""

    def __init__(self, message: str, error: str = ""):
        self.message = message
        self.error = error
        super().__init__(f"{message}: {error}" if error else message)


class DbtProjectManager:
    """Helper functions to access dbt & dbt projects on disk"""

    DEFAULT_DBT_VENV_REL_PATH = "venv"  # for all orgs

    @staticmethod
    def clients_base_dir() -> Path:
        """returns the directory where all clients' dbt projects are stored"""
        return Path(os.getenv("CLIENTDBT_ROOT", ""))

    @staticmethod
    def dbt_venv_base_dir() -> Path:
        """returns the directory containing the dbt venv"""
        return Path(os.getenv("DBT_VENV", ""))

    @staticmethod
    def gather_dbt_project_params(org: Org, orgdbt: OrgDbt) -> DbtProjectParams:
        """Returns the dbt project parameters"""
        if not orgdbt:
            raise HttpError(400, "dbt workspace not setup")

        dbt_env_dir = (
            DbtProjectManager.dbt_venv_base_dir() / orgdbt.dbt_venv
        )  # /data/dbt_venv/ + venv
        if not dbt_env_dir.exists():
            raise HttpError(400, "create the dbt env first")

        dbt_binary = str(dbt_env_dir / "bin/dbt")
        venv_binary = str(dbt_env_dir / "bin")
        # /data/clients_dbt/ + {org.slug}/dbtrepo
        project_dir = DbtProjectManager.get_dbt_project_dir(orgdbt)
        target = orgdbt.default_schema
        org_dir = DbtProjectManager.get_org_dir(org)

        return DbtProjectParams(
            dbt_binary=dbt_binary,
            dbt_env_dir=str(dbt_env_dir),
            venv_binary=venv_binary,
            target=target,
            project_dir=project_dir,
            org_project_dir=org_dir,
        )

    @staticmethod
    def get_dbt_project_dir(orgdbt: OrgDbt) -> str:
        """/data/clients_dbt/ + {org.slug}/dbtrepo"""
        if not orgdbt:
            raise HttpError(400, "dbt workspace not setup")

        return str(DbtProjectManager.clients_base_dir() / orgdbt.project_dir)

    @staticmethod
    def get_org_dir(org: Org) -> str:
        """/data/clients_dbt/ + {org.slug}"""
        return str(DbtProjectManager.clients_base_dir() / org.slug)

    @staticmethod
    def get_dbt_repo_relative_path(path: Union[str, Path]) -> str:
        """returns the relative path to the dbt repo"""
        absolute_path = Path(path).resolve()
        relative_path = absolute_path.relative_to(DbtProjectManager.clients_base_dir())
        return str(relative_path)

    @staticmethod
    def get_dbt_venv_relative_path(path: Union[str, Path]) -> str:
        """returns the relative path to the dbt venv"""
        absolute_path = Path(path).resolve()
        relative_path = absolute_path.relative_to(DbtProjectManager.dbt_venv_base_dir())
        return str(relative_path)

    @classmethod
    def run_dbt_command(
        cls,
        org: Org,
        orgdbt: OrgDbt,
        command: list[str],
        check: bool = True,
        cwd: str = None,
        keyword_args: dict = None,
        flags: list = None,
    ) -> subprocess.CompletedProcess:
        """
        Run a dbt command.

        Args:
            org: The organization
            orgdbt: The OrgDbt configuration
            command: The dbt command as a list (e.g., ["run"], ["test"])
            cwd: Working directory to run the command in. Defaults to project_dir (where dbt_project.yml exists)
            keyword_args: Dict of keyword arguments (e.g., {"select": "+model_name", "profiles-dir": "profiles"})
            flags: List of boolean flags (e.g., ["full-refresh", "no-partial-parse"])

        Returns:
            subprocess.CompletedProcess with the result
        """

        try:
            params = cls.gather_dbt_project_params(org, orgdbt)

            cmd = [params.dbt_binary] + command

            # Add keyword arguments (e.g., select -> --select model_name)
            if keyword_args:
                for key, value in keyword_args.items():
                    flag = f"--{key}" if not key.startswith("-") else key
                    cmd.extend([flag, str(value)])

            # Add boolean flags (e.g., full-refresh -> --full-refresh)
            if flags:
                for flag in flags:
                    cmd.append(f"--{flag}" if not flag.startswith("-") else flag)

            # Default to project_dir where dbt_project.yml exists
            working_dir = cwd if cwd else params.project_dir

            result = subprocess.run(cmd, cwd=working_dir, capture_output=True, text=True)
        except Exception as e:
            raise DbtCommandError(
                message="Failed to execute command",
                error=str(e),
            )

        if check and result.returncode != 0:
            raise DbtCommandError(
                message=result.stdout.strip() or "Command failed",
                error=result.stderr.strip(),
            )

        return result
