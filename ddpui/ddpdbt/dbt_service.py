import re

import glob
import os
import shutil
import subprocess
from pathlib import Path
import requests
from django.utils.text import slugify
from dbt_automation import assets
from ddpui.ddpprefect import (
    prefect_service,
    DBTCLIPROFILE,
    SECRET,
)
from ddpui.models.org import OrgDbt, OrgPrefectBlockv1, OrgWarehouse, TransformType
from ddpui.models.org_user import Org
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask
from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.utils import secretsmanager
from ddpui.utils.constants import (
    TASK_DOCSGENERATE,
    TASK_DBTTEST,
    TASK_DBTRUN,
    TASK_DBTSEED,
    TASK_DBTDEPS,
    TASK_DBTCLOUD_JOB,
)
from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


def delete_dbt_workspace(org: Org):
    """deletes the dbt workspace on disk as well as in prefect"""

    # remove transform tasks
    org_tasks_delete = []
    for org_task in OrgTask.objects.filter(org=org, task__type__in=["dbt", "git"]).all():
        if (
            DataflowOrgTask.objects.filter(
                orgtask=org_task, dataflow__dataflow_type="orchestrate"
            ).count()
            > 0
        ):
            raise Exception(f"{str(org_task)} is being used in a deployment")
        org_tasks_delete.append(org_task)

    logger.info("deleting orgtasks")
    for org_task in org_tasks_delete:
        for dataflow_orgtask in DataflowOrgTask.objects.filter(orgtask=org_task).all():
            dataflow_orgtask.dataflow.delete()
        org_task.delete()

    logger.info("deleting dbt cli profile")
    for dbt_cli_block in OrgPrefectBlockv1.objects.filter(org=org, block_type=DBTCLIPROFILE).all():
        try:
            prefect_service.delete_dbt_cli_profile_block(dbt_cli_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        dbt_cli_block.delete()

    logger.info("deleting git secret block")
    # remove git token uri block
    for secret_block in OrgPrefectBlockv1.objects.filter(org=org, block_type=SECRET).all():
        try:
            prefect_service.delete_secret_block(secret_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        secret_block.delete()

    secretsmanager.delete_github_token(org)

    logger.info("deleting orgdbt, orgdbtmodel and reference to org")
    if org.dbt:
        dbt = org.dbt

        # remove org dbt models
        OrgDbtModel.objects.filter(orgdbt=dbt).delete()

        # remove dbt reference from the org
        org.dbt = None
        org.save()

        # remove dbt project dir and OrgDbt model
        if os.path.exists(dbt.project_dir):
            shutil.rmtree(dbt.project_dir)
        dbt.delete()


def task_config_params(task: Task):
    """Return the config dictionary to setup parameters on this task"""

    # dbt task config parameters
    TASK_CONIF_PARAM = {
        TASK_DBTDEPS: {"flags": ["upgrade"], "options": ["add-package"]},
        TASK_DBTRUN: {"flags": ["full-refresh"], "options": ["select", "exclude"]},
        TASK_DBTTEST: {"flags": [], "options": ["select", "exclude"]},
        TASK_DBTSEED: {"flags": [], "options": ["select"]},
        TASK_DOCSGENERATE: {"flags": [], "options": []},
        TASK_DBTCLOUD_JOB: {"flags": [], "options": ["job_id"]},
    }

    return TASK_CONIF_PARAM[task.slug] if task.slug in TASK_CONIF_PARAM else None


def setup_local_dbt_workspace(org: Org, project_name: str, default_schema: str) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""
    warehouse = OrgWarehouse.objects.filter(org=org).first()

    if not warehouse:
        return None, "Please set up your warehouse first"

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir: Path = Path(DbtProjectManager.get_org_dir(org))
    dbtrepo_dir: Path = project_dir / project_name

    if dbtrepo_dir.exists():
        return None, f"Project {project_name} already exists"

    if not project_dir.exists():
        project_dir.mkdir()
        logger.info("created project_dir %s", project_dir)

    logger.info(f"starting to setup local dbt workspace at {project_dir}")

    # dbt init
    try:
        subprocess.check_call(
            [
                DbtProjectManager.dbt_venv_base_dir()
                / f"{DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH}/bin/dbt",
                "init",
                project_name,
                "--skip-profile-setup",
            ],
            cwd=project_dir,
        )

        # Delete example models
        example_models_dir = dbtrepo_dir / "models" / "example"
        if example_models_dir.exists():
            shutil.rmtree(example_models_dir)

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt init failed with {e.returncode}")
        return None, "Something went wrong while setting up workspace"

    # copy packages.yml
    logger.info("copying packages.yml from assets")
    target_packages_yml = Path(dbtrepo_dir) / "packages.yml"
    source_packages_yml = os.path.abspath(
        os.path.join(os.path.abspath(assets.__file__), "..", "packages.yml")
    )
    shutil.copy(source_packages_yml, target_packages_yml)

    # copy all macros with .sql extension from assets
    assets_dir = assets.__path__[0]

    for sql_file_path in glob.glob(os.path.join(assets_dir, "*.sql")):
        # Get the target path in the project_dir/macros directory
        target_path = Path(dbtrepo_dir) / "macros" / Path(sql_file_path).name

        # Copy the .sql file to the target path
        shutil.copy(sql_file_path, target_path)

        # Log the creation of the file
        logger.info("created %s", target_path)

    dbt = OrgDbt(
        project_dir=DbtProjectManager.get_dbt_repo_relative_path(dbtrepo_dir),
        dbt_venv=DbtProjectManager.DEFAULT_DBT_VENV_REL_PATH,
        target_type=warehouse.wtype,
        default_schema=default_schema,
        transform_type=TransformType.UI,
    )
    dbt.save()
    logger.info("created orgdbt for org %s", org.name)
    org.dbt = dbt
    org.save()
    logger.info("set org.dbt for org %s", org.name)

    logger.info("set dbt workspace completed for org %s", org.name)

    return None, None


def convert_github_url(url: str) -> str:
    """convert Github repo url to api url"""
    pattern = r"https://github.com/([^/]+)/([^/]+)\.git"
    replacement = r"https://api.github.com/repos/\1/\2"
    new_url = re.sub(pattern, replacement, url)
    return new_url


def check_repo_exists(gitrepo_url: str, gitrepo_access_token: str | None) -> bool:
    """Check if a GitHub repo exists."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
    }
    if gitrepo_access_token:
        headers["Authorization"] = f"token {gitrepo_access_token}"

    url = convert_github_url(gitrepo_url)

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.info(f"Error checking repo existence: {e}")
        return False

    return response.status_code == 200
