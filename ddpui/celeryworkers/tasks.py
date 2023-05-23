import os
import shutil
from pathlib import Path
from subprocess import CalledProcessError

from django.utils.text import slugify
from ddpui.celery import app
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.utils.helpers import runcmd
from ddpui.utils import secretsmanager
from ddpui.utils.taskprogress import TaskProgress


@app.task(bind=True)
def setup_dbtworkspace(self, org_id: int, payload: dict) -> str:
    """sets up an org's dbt workspace, recreating it if it already exists"""

    taskprogress = TaskProgress(self.request.id)

    taskprogress.add(
        {
            "stepnum": 1,
            "numsteps": 8,
            "message": "started",
            "status": "running",
        }
    )
    org = Org.objects.filter(id=org_id).first()

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        taskprogress.add(
            {
                "stepnum": 2,
                "numsteps": 8,
                "message": "need to set up a warehouse first",
                "status": "failed",
            }
        )
        return

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if project_dir.exists():
        shutil.rmtree(str(project_dir))
    project_dir.mkdir()

    taskprogress.add(
        {
            "stepnum": 2,
            "numsteps": 8,
            "message": "created project_dir",
            "status": "running",
        }
    )

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    # if we have an access token with the "contents" and "metadata" permissions then
    #   git clone https://oauth2:[TOKEN]@github.com/[REPO-OWNER]/[REPO-NAME]
    if payload["gitrepoAccessToken"] is not None:
        gitrepo_url = payload["gitrepoUrl"].replace(
            "github.com", "oauth2:" + payload["gitrepoAccessToken"] + "@github.com"
        )
        cmd = f"git clone {gitrepo_url} dbtrepo"
    else:
        cmd = f"git clone {payload['gitrepoUrl']} dbtrepo"

    try:
        runcmd(cmd, project_dir)
    except Exception as error:
        taskprogress.add(
            {
                "stepnum": 3,
                "numsteps": 8,
                "message": "git clone failed",
                "error": str(error),
                "status": "failed",
            }
        )
        return

    taskprogress.add(
        {
            "stepnum": 3,
            "numsteps": 8,
            "message": "cloned git repo",
            "status": "running",
        }
    )

    # install a dbt venv
    try:
        runcmd("python3 -m venv venv", project_dir)
    except Exception as error:
        taskprogress.add(
            {
                "stepnum": 4,
                "numsteps": 8,
                "message": "make venv failed",
                "error": str(error),
                "status": "failed",
            }
        )
        return

    taskprogress.add(
        {
            "stepnum": 4,
            "numsteps": 8,
            "message": "created venv",
            "status": "running",
        }
    )

    # upgrade pip
    pip = project_dir / "venv/bin/pip"
    try:
        runcmd(f"{pip} install --upgrade pip", project_dir)
    except CalledProcessError as error:
        if error.returncode != 120:
            taskprogress.add(
                {
                    "stepnum": 5,
                    "numsteps": 8,
                    "message": f"{pip} --upgrade failed",
                    "error": str(error),
                    "status": "failed",
                }
            )
            return
    except Exception as error:
        taskprogress.add(
            {
                "stepnum": 5,
                "numsteps": 8,
                "message": f"{pip} --upgrade failed",
                "error": str(error),
                "status": "failed",
            }
        )
        return

    taskprogress.add(
        {
            "stepnum": 5,
            "numsteps": 8,
            "message": "upgraded pip",
            "status": "running",
        }
    )

    # install dbt in the new env
    try:
        runcmd(f"{pip} install dbt-core=={payload['dbtVersion']}", project_dir)
    except Exception as error:
        taskprogress.add(
            {
                "stepnum": 6,
                "numsteps": 8,
                "message": "pip install dbt-core=={payload['dbtVersion']} failed",
                "error": str(error),
                "status": "failed",
            }
        )
        return

    taskprogress.add(
        {
            "stepnum": 6,
            "numsteps": 8,
            "message": "installed dbt-core",
            "status": "running",
        }
    )

    if warehouse.wtype == "postgres":
        try:
            runcmd(f"{pip} install dbt-postgres==1.4.5", project_dir)
        except Exception as error:
            taskprogress.add(
                {
                    "stepnum": 7,
                    "numsteps": 8,
                    "message": "pip install dbt-postgres==1.4.5 failed",
                    "error": str(error),
                    "status": "failed",
                }
            )
            return
        taskprogress.add(
            {
                "stepnum": 7,
                "numsteps": 8,
                "message": "installed dbt-postgres",
                "status": "running",
            }
        )

    elif warehouse.wtype == "bigquery":
        try:
            runcmd(f"{pip} install dbt-bigquery==1.4.3", project_dir)
        except Exception as error:
            taskprogress.add(
                {
                    "stepnum": 7,
                    "numsteps": 8,
                    "message": "pip install dbt-bigquery==1.4.3 failed",
                    "error": str(error),
                    "status": "failed",
                }
            )
            return
        taskprogress.add(
            {
                "stepnum": 7,
                "numsteps": 8,
                "message": "installed dbt-bigquery",
                "status": "running",
            }
        )

    else:
        taskprogress.add(
            {
                "stepnum": 7,
                "numsteps": 8,
                "message": "what warehouse is this",
                "status": "failed",
            }
        )
        return

    dbt = OrgDbt(
        gitrepo_url=payload["gitrepoUrl"],
        project_dir=str(project_dir),
        dbt_version=payload["dbtVersion"],
        target_type=warehouse.wtype,
        default_schema=payload["profile"]["target_configs_schema"],
    )
    dbt.save()
    org.dbt = dbt
    org.save()

    if payload["gitrepoAccessToken"] is not None:
        secretsmanager.delete_github_token(org)
        secretsmanager.save_github_token(org, payload["gitrepoAccessToken"])

    taskprogress.add(
        {
            "stepnum": 8,
            "numsteps": 8,
            "message": "wrote OrgDbt entry",
            "status": "completed",
        }
    )
