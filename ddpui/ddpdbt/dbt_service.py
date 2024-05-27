import glob
import os
import shutil
import subprocess
from pathlib import Path
import requests
import re
from uuid import uuid4
import boto3
import boto3.exceptions

from django.utils.text import slugify
from dbt_automation import assets
from ddpui.ddpprefect import (
    prefect_service,
    DBTCLIPROFILE,
    SECRET,
)
from ddpui.models.org import OrgDbt, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.org_user import Org
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask
from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.redis_client import RedisClient
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.celeryworkers.tasks import create_elementary_report

logger = CustomLogger("ddpui")


def delete_dbt_workspace(org: Org):
    """deletes the dbt workspace on disk as well as in prefect"""

    # remove transform tasks
    org_tasks_delete = []
    for org_task in OrgTask.objects.filter(
        org=org, task__type__in=["dbt", "git"]
    ).all():
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
    for dbt_cli_block in OrgPrefectBlockv1.objects.filter(
        org=org, block_type=DBTCLIPROFILE
    ).all():
        try:
            prefect_service.delete_dbt_cli_profile_block(dbt_cli_block.block_id)
        except Exception:  # pylint:disable=broad-exception-caught
            pass
        dbt_cli_block.delete()

    logger.info("deleting git secret block")
    # remove git token uri block
    for secret_block in OrgPrefectBlockv1.objects.filter(
        org=org, block_type=SECRET
    ).all():
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
        "dbt-deps": {"flags": ["upgrade"], "options": ["add-package"]},
        "dbt-run": {"flags": ["full-refresh"], "options": ["select", "exclude"]},
        "dbt-test": {"flags": [], "options": ["select", "exclude"]},
        "dbt-seed": {"flags": [], "options": ["select"]},
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
    project_dir: Path = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
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
                Path(os.getenv("DBT_VENV")) / "venv/bin/dbt",
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
        project_dir=str(project_dir),
        dbt_venv=os.getenv("DBT_VENV"),
        target_type=warehouse.wtype,
        default_schema=default_schema,
        transform_type="ui",
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


def make_elementary_report(org: Org):
    """generate elementary report"""
    if org.dbt is None:
        return "dbt is not configured for this client", None

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if not os.path.exists(project_dir):
        return "create the dbt env first", None

    repo_dir = project_dir / "dbtrepo"
    if not os.path.exists(repo_dir / "elementary_profiles"):
        return "set up elementary profile first", None

    s3 = boto3.client(
        "s3",
        "ap-south-1",
        aws_access_key_id=os.getenv("ELEMENTARY_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("ELEMENTARY_AWS_SECRET_ACCESS_KEY"),
    )
    try:
        s3response = s3.get_object(
            Bucket=os.getenv("ELEMENTARY_S3_BUCKET"),
            Key=f"reports/{org.slug}.html",
        )
        logger.info("fetched s3response")
    except boto3.exceptions.botocore.exceptions.ClientError:
        # the report doesn't exist, trigger a celery task to generate it
        # see if there is an existing TaskProgress for this org
        running_task = TaskProgress.get_running_tasks(
            f"{TaskProgressHashPrefix.RUNELEMENTARY}-{org.slug}"
        )
        logger.info("running_task %s", str(running_task))
        if running_task and len(running_task) > 0:
            logger.info("edr already running for org %s", org.slug)
            task_id = running_task[0].decode("utf8")
            return None, {"task_id": task_id}

        logger.info("creating new elementary report")
        task = create_elementary_report.delay(org.id)
        logger.info(task.id)
        return None, {"task_id": task.id}
    except Exception:
        return "error fetching elementary report", None

    report_html = s3response["Body"].read().decode("utf-8")
    htmlfilename = str(repo_dir / "elementary-report.html")
    with open(htmlfilename, "w", encoding="utf-8") as indexfile:
        indexfile.write(report_html)
        indexfile.close()
    logger.info("wrote elementary report to %s", htmlfilename)

    redis = RedisClient.get_instance()
    token = uuid4()
    redis_key = f"elementary-report-{token.hex}"
    redis.set(redis_key, htmlfilename.encode("utf-8"))
    redis.expire(redis_key, 3600 * 24)
    logger.info("created redis key %s", redis_key)

    return None, {"token": token.hex}
