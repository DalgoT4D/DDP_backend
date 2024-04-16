import os
import shutil
from pathlib import Path

from datetime import datetime, timedelta
from ddpui.utils.timezone import UTC
from django.utils.text import slugify
from ddpui.celery import app
from ddpui.utils.custom_logger import CustomLogger
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.models.orgjobs import BlockLock
from ddpui.models.tasks import TaskLock
from ddpui.utils.helpers import runcmd
from ddpui.utils import secretsmanager
from ddpui.utils.taskprogress import TaskProgress
from ddpui.ddpprefect.prefect_service import update_dbt_core_block_schema

logger = CustomLogger("ddpui")


@app.task(bind=True)
def clone_github_repo(
    self,
    gitrepo_url: str,
    gitrepo_access_token: str | None,
    project_dir: str,
    taskprogress: TaskProgress | None,
) -> bool:
    """clones an org's github repo"""
    if taskprogress is None:
        child = False
        taskprogress = TaskProgress(self.request.id)
    else:
        child = True

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    # if we have an access token with the "contents" and "metadata" permissions then
    #   git clone https://oauth2:[TOKEN]@github.com/[REPO-OWNER]/[REPO-NAME]
    if gitrepo_access_token is not None:
        gitrepo_url = gitrepo_url.replace(
            "github.com", "oauth2:" + gitrepo_access_token + "@github.com"
        )

    project_dir: Path = Path(project_dir)
    dbtrepo_dir = project_dir / "dbtrepo"
    if not project_dir.exists():
        project_dir.mkdir()
        taskprogress.add(
            {
                "message": "created project_dir",
                "status": "running",
            }
        )
        logger.info("created project_dir %s", project_dir)

    elif dbtrepo_dir.exists():
        shutil.rmtree(str(dbtrepo_dir))

    cmd = f"git clone {gitrepo_url} dbtrepo"

    try:
        runcmd(cmd, project_dir)
    except Exception as error:
        taskprogress.add(
            {
                "message": "git clone failed",
                "error": str(error),
                "status": "failed",
            }
        )
        logger.exception(error)
        return False

    taskprogress.add(
        {
            "message": "cloned git repo",
            "status": "running" if child else "completed",
        }
    )
    return True


@app.task(bind=True)
def setup_dbtworkspace(self, org_id: int, payload: dict) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""
    taskprogress = TaskProgress(self.request.id)

    taskprogress.add(
        {
            "message": "started",
            "status": "running",
        }
    )
    org = Org.objects.filter(id=org_id).first()
    logger.info("found org %s", org.name)

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        taskprogress.add(
            {
                "message": "need to set up a warehouse first",
                "status": "failed",
            }
        )
        logger.error("need to set up a warehouse first for org %s", org.name)
        return

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug

    # four parameters here is correct despite vscode thinking otherwise
    if not clone_github_repo(
        payload["gitrepoUrl"],
        payload["gitrepoAccessToken"],
        str(project_dir),
        taskprogress,
    ):
        return

    logger.info("git clone succeeded for org %s", org.name)

    dbt = OrgDbt(
        gitrepo_url=payload["gitrepoUrl"],
        project_dir=str(project_dir),
        dbt_venv=os.getenv("DBT_VENV"),
        target_type=warehouse.wtype,
        default_schema=payload["profile"]["target_configs_schema"],
        transform_type="github",
    )
    dbt.save()
    logger.info("created orgdbt for org %s", org.name)
    org.dbt = dbt
    org.save()
    logger.info("set org.dbt for org %s", org.name)

    if payload["gitrepoAccessToken"] is not None:
        secretsmanager.delete_github_token(org)
        secretsmanager.save_github_token(org, payload["gitrepoAccessToken"])

    taskprogress.add(
        {
            "message": "wrote OrgDbt entry",
            "status": "completed",
        }
    )
    logger.info("set dbt workspace completed for org %s", org.name)


@app.task(bind=False)
def update_dbt_core_block_schema_task(block_name, default_schema):
    """single http PUT request to the prefect-proxy"""
    logger.info("updating default_schema of %s to %s", block_name, default_schema)
    update_dbt_core_block_schema(block_name, default_schema)


@app.task()
def delete_old_blocklocks():
    """delete blocklocks which were created over an hour ago"""
    onehourago = UTC.localize(datetime.utcnow() - timedelta(seconds=3600))
    BlockLock.objects.filter(locked_at__lt=onehourago).delete()


@app.task()
def delete_old_tasklocks():
    """delete task locks which were created over an hour ago"""
    onehourago = UTC.localize(datetime.utcnow() - timedelta(seconds=3600))
    TaskLock.objects.filter(locked_at__lt=onehourago).delete()


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    """check for old blocks every minute"""
    sender.add_periodic_task(
        60 * 1.0, delete_old_blocklocks.s(), name="remove old blocklocks"
    )
    sender.add_periodic_task(
        60 * 1.0, delete_old_tasklocks.s(), name="remove old tasklocks"
    )
