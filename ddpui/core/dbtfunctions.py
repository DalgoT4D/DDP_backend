import os
from pathlib import Path
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDbt
from ddpui.ddpdbt.schema import DbtProjectParams


def gather_dbt_project_params(org: Org):
    """Returns the dbt project parameters"""
    dbt_env_dir = Path(org.dbt.dbt_venv)
    if not dbt_env_dir.exists():
        return None, "create the dbt env first"

    dbt_binary = str(dbt_env_dir / "venv/bin/dbt")
    dbtrepodir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug / "dbtrepo"
    project_dir = str(dbtrepodir)
    target = org.dbt.default_schema

    return (
        DbtProjectParams(
            dbt_binary=dbt_binary,
            dbt_env_dir=dbt_env_dir,
            dbt_repo_dir=dbtrepodir,
            target=target,
            project_dir=project_dir,
        ),
        None,
    )
