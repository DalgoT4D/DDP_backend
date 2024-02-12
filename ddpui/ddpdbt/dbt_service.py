import os
import shutil
import subprocess
from pathlib import Path

import slugify

from ddpui.ddpprefect import DBTCORE, SHELLOPERATION, prefect_service
from ddpui.models.org import OrgDbt, OrgPrefectBlock, OrgWarehouse
from ddpui.models.org_user import Org
from ddpui.models.tasks import Task
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def delete_dbt_workspace(org: Org):
    """deletes the dbt workspace on disk as well as in prefect"""
    if org.dbt:
        dbt = org.dbt
        org.dbt = None
        org.save()
        if os.path.exists(dbt.project_dir):
            shutil.rmtree(dbt.project_dir)
        dbt.delete()

    for dbtblock in OrgPrefectBlock.objects.filter(org=org, block_type=DBTCORE):
        try:
            prefect_service.delete_dbt_core_block(dbtblock.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        dbtblock.delete()

    for shellblock in OrgPrefectBlock.objects.filter(
        org=org, block_type=SHELLOPERATION
    ):
        if shellblock.block_name.find("-git-pull") > -1:
            try:
                prefect_service.delete_shell_block(shellblock.block_id)
            except Exception:  # pylint:disable=broad-exception-caught
                pass
            shellblock.delete()

    secretsmanager.delete_github_token(org)


def task_config_params(task: Task):
    """Return the config dictionary to setup parameters on this task"""

    # dbt task config parameters
    TASK_CONIF_PARAM = {
        "dbt-deps": {"flags": ["upgrade"], "options": ["add-package"]},
        "dbt-run": {"flags": ["full-refresh"], "options": ["select", "exclude"]},
        "dbt-test": {"flags": [], "options": ["select", "exclude"]},
        "dbt-seed": {"flags": [], "options": ["select"]},
    }

    return TASK_CONIF_PARAM[task.slug] if task.slug in TASK_CONIF_PARAM else None


def setup_local_dbt_workspace(org: Org, project_name: str, default_schema: str) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir: Path = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    dbtrepo_dir: Path = project_dir / project_name
    if dbtrepo_dir.exists():
        return None, f"Project {project_name} already exists"

    if not project_dir.exists():
        project_dir.mkdir()
        logger.info("created project_dir %s", project_dir)

    try:
        subprocess.check_call(
            [
                Path(os.getenv("DBT_VENV")) / "venv/bin/dbt",
                "init",
                project_name,
                "--skip-profile-setup",
            ],
            cwd=project_dir,
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt init failed with {e.returncode}")
        return None, "Something went wrong while setting up workspace"

    logger.info(f"starting to setup local dbt workspace at {project_dir}")

    dbt = OrgDbt(
        project_dir=str(project_dir),
        dbt_venv=os.getenv("DBT_VENV"),
        target_type=warehouse.wtype,
        default_schema=default_schema,
    )
    dbt.save()
    logger.info("created orgdbt for org %s", org.name)
    org.dbt = dbt
    org.save()
    logger.info("set org.dbt for org %s", org.name)

    logger.info("set dbt workspace completed for org %s", org.name)

    return None, None
