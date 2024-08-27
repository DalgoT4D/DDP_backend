import re
from uuid import uuid4
import glob
import os
import shutil
from datetime import datetime
import subprocess
from pathlib import Path
import requests
import boto3
import boto3.exceptions
from ninja.errors import HttpError
from django.utils.text import slugify
from dbt_automation import assets
from ddpui.ddpprefect import (
    prefect_service,
    DBTCLIPROFILE,
    SECRET,
)
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import OrgDataFlowv1
from ddpui.models.org import OrgDbt, OrgPrefectBlockv1, OrgWarehouse, TransformType
from ddpui.models.org_user import Org
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask
from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.utils import secretsmanager
from ddpui.utils.timezone import as_ist
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.singletaskprogress import SingleTaskProgress
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


def make_edr_report_s3_path(org: Org):
    """make s3 path for elementary report"""
    todays_date = datetime.today().strftime("%Y-%m-%d")
    bucket_file_path = f"reports/{org.slug}.{todays_date}.html"
    return bucket_file_path


def refresh_elementary_report(org: Org):
    """refreshes the elementary report for the current date"""
    if org.dbt is None:
        return "dbt is not configured for this client", None

    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if not os.path.exists(project_dir):
        return "create the dbt env first", None

    repo_dir = project_dir / "dbtrepo"
    if not os.path.exists(repo_dir / "elementary_profiles"):
        return "set up elementary profile first", None

    task_str = f"{TaskProgressHashPrefix.RUNELEMENTARY}-{org.slug}"
    if SingleTaskProgress.fetch(task_str) is None:
        bucket_file_path = make_edr_report_s3_path(org)
        logger.info("creating new elementary report at %s", bucket_file_path)
        create_elementary_report.delay(task_str, org.id, bucket_file_path)
        ttl = int(os.getenv("EDR_TTL", "180"))
    else:
        logger.info("edr already running for org %s", org.slug)
        ttl = SingleTaskProgress.get_ttl(task_str)
    return None, {"task_id": task_str, "ttl": ttl}


def fetch_elementary_report(org: Org):
    """fetch previously generated elementary report"""
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
    bucket_file_path = make_edr_report_s3_path(org)
    try:
        s3response = s3.get_object(
            Bucket=os.getenv("ELEMENTARY_S3_BUCKET"),
            Key=bucket_file_path,
        )
        logger.info("fetched s3response")
    except boto3.exceptions.botocore.exceptions.ClientError:
        return "report has not been generated", None
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
    redis.set(redis_key, htmlfilename.encode("utf-8"), 600)
    logger.info("created redis key %s", redis_key)

    return None, {
        "token": token.hex,
        "created_on_utc": s3response[
            "LastModified"
        ].isoformat(),  # e.g. 2024-06-07T00:44:08+00:00
        "created_on_ist": as_ist(
            s3response["LastModified"]
        ).isoformat(),  # e.g. 2024-06-07T06:14:08+05:30
    }


def refresh_elementary_report_via_prefect(orguser: OrgUser) -> dict:
    """refreshes the elementary report for the current date using the prefect deployment"""
    org: Org = orguser.org
    odf = OrgDataFlowv1.objects.filter(
        org=org, name__startswith=f"pipeline-{org.slug}-generate-edr"
    ).first()

    if odf is None:
        return {"error": "pipeline not found"}

    locks = prefect_service.lock_tasks_for_deployment(odf.deployment_id, orguser)

    try:
        res = prefect_service.create_deployment_flow_run(odf.deployment_id)
        for tasklock in locks:
            tasklock.flow_run_id = res["flow_run_id"]
            tasklock.save()

    except Exception as error:
        for task_lock in locks:
            logger.info("deleting TaskLock %s", task_lock.orgtask.task.slug)
            task_lock.delete()
        logger.exception(error)
        raise HttpError(400, "failed to start a run") from error

    return res
