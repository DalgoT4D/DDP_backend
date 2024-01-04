import os
from unittest.mock import Mock, patch

import django
import pytest
import yaml

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.pipeline_api import post_run_prefect_org_deployment_task
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task, TaskLock
from ddpui.utils.constants import TASK_DBTRUN

pytestmark = pytest.mark.django_db


def seed_tasks():
    # seed data
    for task in [
        {"type": "git", "slug": "git-pull", "label": "GIT pull", "command": "git pull"},
        {"type": "dbt", "slug": "dbt-clean", "label": "DBT clean", "command": "clean"},
        {"type": "dbt", "slug": "dbt-deps", "label": "DBT deps", "command": "deps"},
        {"type": "dbt", "slug": "dbt-run", "label": "DBT run", "command": "run"},
        {"type": "dbt", "slug": "dbt-test", "label": "DBT test", "command": "test"},
        {
            "type": "airbyte",
            "slug": "airbyte-sync",
            "label": "AIRBYTE sync",
            "command": None,
        },
    ]:
        Task.objects.create(**task)


# ================================================================================
@pytest.fixture()
def org_without_dbt_workspace():
    """org without dbt workspace"""
    print("creating org")
    org_slug = "test-org-slug"

    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )
    yield org
    print("deleting org")
    org.delete()


@pytest.fixture()
def org_with_dbt_workspace(tmpdir_factory):
    """a pytest fixture which creates an Org having an dbt workspace"""
    print("creating org_with_dbt_workspace")
    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(
        str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8"
    ) as output:
        yaml.safe_dump(yml_obj, output)

    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        dbt=dbt,
        name=org_slug,
    )
    yield org
    print("deleting org_with_dbt_workspace")
    org.delete()


@pytest.fixture()
def org_with_transformation_tasks(tmpdir_factory):
    """org having the transformation tasks and dbt workspace"""
    print("creating org with tasks")

    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(
        str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8"
    ) as output:
        yaml.safe_dump(yml_obj, output)

    dbt = OrgDbt.objects.create(
        gitrepo_url="dummy-git-url.github.com",
        project_dir="tmp/",
        dbt_venv=tmpdir_factory.mktemp("venv"),
        target_type="postgres",
        default_schema="prod",
    )
    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        dbt=dbt,
        name=org_slug,
    )
    user = User.objects.create(
        is_superuser=True,
        username="test@gmail.com",
        password="password",
        is_active=True,
        is_staff=False,
    )
    OrgUser.objects.create(org=org, user=user, role=3, email_verified=True)

    # create secret block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=SECRET,
        block_id="secret-blk-id",
        block_name="secret-blk-name",
    )

    # create cli block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_id="cliprofile-blk-id",
        block_name="cliprofile-blk-name",
    )

    # seed data
    seed_tasks()

    for task in Task.objects.filter(type__in=["dbt", "git"]).all():
        org_task = OrgTask.objects.create(org=org, task=task)

        if task.slug == "dbt-run":
            new_dataflow = OrgDataFlowv1.objects.create(
                org=org,
                name="test-dbtrun-deployment",
                deployment_name="test-dbtrun-deployment",
                deployment_id="test-dbtrun-deployment-id",
                dataflow_type="manual",
            )

            DataflowOrgTask.objects.create(
                dataflow=new_dataflow,
                orgtask=org_task,
            )

    yield org

    org.delete()


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_deployment_flow_run=Mock(return_value=True),
    lock_tasks_for_deployment=Mock(return_value=[]),
)
def test_post_run_prefect_org_deployment_task_success(org_with_transformation_tasks):
    """tests POST /v1/flows/{deployment_id}/flow_run/ success"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    dataflow_orgtask = None
    org_task = OrgTask.objects.filter(
        org=mock_orguser.org, task__slug=TASK_DBTRUN
    ).first()
    if org_task:
        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

    if dataflow_orgtask is None:
        raise Exception("Deployment not found")

    post_run_prefect_org_deployment_task(
        mock_orguser, dataflow_orgtask.dataflow.deployment_id
    )

    assert TaskLock.objects.filter(orgtask=org_task).count() == 0
