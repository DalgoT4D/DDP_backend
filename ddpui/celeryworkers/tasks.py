import os
import shutil
from pathlib import Path

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

    taskprogress.add({
        "stepnum": 1,
        "numsteps": 8,
        "message": 'started',
        "status": 'running',
    })
    org = Org.objects.filter(id=org_id).first()

    warehouse = OrgWarehouse.objects.filter(org=org).first()
    if warehouse is None:
        taskprogress.add({
            "stepnum": 2,
            "numsteps": 8,
            "message": 'need to set up a warehouse first',
            "status": "failed",
        })
        return

    if org.slug is None:
        org.slug = slugify(org.name)
        org.save()

    # this client'a dbt setup happens here
    project_dir = Path(os.getenv("CLIENTDBT_ROOT")) / org.slug
    if project_dir.exists():
        shutil.rmtree(str(project_dir))
    project_dir.mkdir()

    taskprogress.add({
        "stepnum": 2,
        "numsteps": 8,
        "message": 'created project_dir',
        "status": 'running',
    })

    # clone the client's dbt repo into "dbtrepo/" under the project_dir
    # if we have an access token with the "contents" and "metadata" permissions then
    #   git clone https://oauth2:[TOKEN]@github.com/[REPO-OWNER]/[REPO-NAME]
    if payload['gitrepoAccessToken'] is not None:
        gitrepo_url = payload['gitrepoUrl'].replace(
            "github.com", "oauth2:" + payload['gitrepoAccessToken'] + "@github.com"
        )
        process = runcmd(f"git clone {gitrepo_url} dbtrepo", project_dir)
    else:
        process = runcmd(f"git clone {payload['gitrepoUrl']} dbtrepo", project_dir)
        
    if process.wait() != 0:
        taskprogress.add({
            "stepnum": 3,
            "numsteps": 8,
            "message": 'git clone failed',
            "status": "failed",
        })
        return

    taskprogress.add({
        "stepnum": 3,
        "numsteps": 8,
        "message": 'cloned git repo',
        "status": 'running',
    })

    # install a dbt venv
    process = runcmd("python -m venv venv", project_dir)
    if process.wait() != 0:
        taskprogress.add({
            "stepnum": 4,
            "numsteps": 8,
            "message": 'make venv failed',
            "status": "failed",
        })
        return

    taskprogress.add({
        "stepnum": 4,
        "numsteps": 8,
        "message": 'created venv',
        "status": 'running',
    })

    # upgrade pip
    pip = project_dir / "venv/bin/pip"
    process = runcmd(f"{pip} install --upgrade pip", project_dir)
    if process.wait() != 0:
        taskprogress.add({
            "stepnum": 5,
            "numsteps": 8,
            "message": 'pip --upgrade failed',
            "status": "failed",
        })
        return

    taskprogress.add({
        "stepnum": 5,
        "numsteps": 8,
        "message": 'upgraded pip',
        "status": 'running',
    })

    # install dbt in the new env
    process = runcmd(f"{pip} install dbt-core=={payload['dbtVersion']}", project_dir)
    if process.wait() != 0:
        taskprogress.add({
            "stepnum": 6,
            "numsteps": 8,
            "message": "pip install dbt-core=={payload['dbtVersion']} failed",
            "status": "failed",
        })
        return

    taskprogress.add({
        "stepnum": 6,
        "numsteps": 8,
        "message": 'installed dbt-core',
        "status": 'running',
    })

    if warehouse.wtype == "postgres":
        process = runcmd(f"{pip} install dbt-postgres==1.4.5", project_dir)
        if process.wait() != 0:
            taskprogress.add({
                "stepnum": 7,
                "numsteps": 8,
                "message": "pip install dbt-postgres==1.4.5 failed",
                "status": 'failed',
            })
            return
        taskprogress.add({
            "stepnum": 7,
            "numsteps": 8,
            "message": 'installed dbt-postgres',
            "status": 'running',
        })

    elif warehouse.wtype == "bigquery":
        process = runcmd(f"{pip} install dbt-bigquery==1.4.3", project_dir)
        if process.wait() != 0:
            taskprogress.add({
                "stepnum": 7,
                "numsteps": 8,
                "message": "pip install dbt-bigquery==1.4.3 failed",
                "status": 'failed',
            })
            return
        taskprogress.add({
            "stepnum": 7,
            "numsteps": 8,
            "message": 'installed dbt-bigquery',
            "status": 'running',
        })

    else:
        taskprogress.add({
            "stepnum": 7,
            "numsteps": 8,
            "message": "what warehouse is this",
            "status": 'failed',
        })
        return

    dbt = OrgDbt(
        gitrepo_url=payload['gitrepoUrl'],
        project_dir=str(project_dir),
        dbt_version=payload['dbtVersion'],
        target_name=payload['profile']['target'],
        target_type=warehouse.wtype,
        target_schema=payload['profile']['target_configs_schema'],
    )
    dbt.save()
    org.dbt = dbt
    org.save()

    if payload['gitrepoAccessToken'] is not None:
        secretsmanager.delete_github_token(org)
        secretsmanager.save_github_token(org, payload['gitrepoAccessToken'])

    taskprogress.add({
        "stepnum": 8,
        "numsteps": 8,
        "message": 'wrote OrgDbt entry',
        "status": 'completed',
    })
