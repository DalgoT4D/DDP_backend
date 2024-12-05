import os
from pathlib import Path
from typing import Union

from ninja.errors import HttpError
from ddpui.models.org import Org, OrgDbt
from ddpui.ddpdbt.schema import DbtProjectParams


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
