import os
from pathlib import Path

from ninja.errors import HttpError
from ddpui.models.org import Org, OrgDbt
from ddpui.ddpdbt.schema import DbtProjectParams


class DbtProjectManager:
    def clients_base_dir() -> Path:
        return Path(os.getenv("CLIENTDBT_ROOT", ""))

    def dbt_venv_base_dir() -> Path:
        return Path(os.getenv("DBT_VENV", ""))

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
        project_dir = str(
            DbtProjectManager.clients_base_dir() / orgdbt.project_dir
        )  # /data/clients_dbt/ + {org.slug}/dbtrepo
        target = orgdbt.default_schema
        org_dir = DbtProjectManager.clients_base_dir() / org.slug

        return DbtProjectParams(
            dbt_binary=dbt_binary,
            dbt_env_dir=str(dbt_env_dir),
            venv_binary=venv_binary,
            target=target,
            project_dir=project_dir,
            org_project_dir=org_dir,
        )

    def get_dbt_project_dir(orgdbt: OrgDbt) -> str:
        if not orgdbt:
            raise HttpError(400, "dbt workspace not setup")

        return str(
            DbtProjectManager.clients_base_dir() / orgdbt.project_dir
        )  # /data/clients_dbt/ + {org.slug}/dbtrepo

    def get_org_dir(org: Org) -> str:
        return str(DbtProjectManager.clients_base_dir() / org.slug)

    def get_dbt_repo_relative_path(path: str) -> str:
        absolute_path = Path(path).resolve()
        relative_path = absolute_path.relative_to(DbtProjectManager.clients_base_dir())
        return str(relative_path)
