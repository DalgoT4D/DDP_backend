import os
from unittest.mock import Mock, patch

import django
import pytest
import yaml
import json
from pathlib import Path
from django.apps import apps
from ninja.errors import HttpError


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User

from ddpui.api.pipeline_api import (
    post_run_prefect_org_deployment_task,
    post_prefect_dataflow_v1,
)
from ddpui.ddpprefect import DBTCLIPROFILE, SECRET, AIRBYTESERVER
from ddpui.ddpprefect.schema import (
    PrefectDataFlowCreateSchema4,
    PrefectFlowAirbyteConnection2,
)
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task, TaskLock
from ddpui.utils.constants import TASK_DBTRUN

pytestmark = pytest.mark.django_db


# ================================================================================
@pytest.fixture
def seed_master_tasks():
    app_dir = os.path.join(Path(apps.get_app_config("ddpui").path), "..")
    seed_dir = os.path.abspath(os.path.join(app_dir, "seed"))
    f = open(os.path.join(seed_dir, "tasks.json"))
    tasks = json.load(f)
    for task in tasks:
        Task.objects.create(**task["fields"])


@pytest.fixture()
def org_without_dbt_workspace(seed_master_tasks):
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
def org_with_dbt_workspace(tmpdir_factory, seed_master_tasks):
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
def org_with_transformation_tasks(tmpdir_factory, seed_master_tasks):
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

    # server block
    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=AIRBYTESERVER,
        block_id="server-blk-id",
        block_name="server-blk-name",
    )

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


# ================================================================================


def test_post_prefect_dataflow_v1_failure1():
    """tests the failure due to missing organization"""
    connections = [PrefectFlowAirbyteConnection2(id="test-conn-id", seq=1)]
    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow", connections=connections, dbtTransform="yes", cron=""
    )
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_prefect_dataflow_v1(mock_request, payload)

    assert str(excinfo.value) == "register an organization first"


def test_post_prefect_dataflow_v1_failure2(org_with_transformation_tasks):
    """tests the failure due to missing name of the dataflow in the payload"""
    connections = [PrefectFlowAirbyteConnection2(id="test-conn-id", seq=1)]
    payload = PrefectDataFlowCreateSchema4(
        name="", connections=connections, dbtTransform="yes", cron=""
    )
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        post_prefect_dataflow_v1(mock_request, payload)

    assert str(excinfo.value) == "must provide a name for the flow"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
    ),
)
def test_post_prefect_dataflow_v1_success(org_with_transformation_tasks):
    """tests the success of creating dataflow with only connections and no transform"""
    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=1),
    ]
    # create org tasks for these connections
    for conn in connections:
        OrgTask.objects.create(
            org=org_with_transformation_tasks,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )
    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        dbtTransform="no",
        cron="test-cron",
    )
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    deployment = post_prefect_dataflow_v1(mock_request, payload)

    assert deployment["deploymentId"] == "test-deploy-id"
    assert deployment["name"] == payload.name
    assert deployment["cron"] == payload.cron


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
    ),
)
def test_post_prefect_dataflow_v1_success2(org_with_transformation_tasks):
    """tests the success of creating dataflow with connections and dbt transform yes"""
    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=1),
    ]
    # create org tasks for these connections
    for conn in connections:
        OrgTask.objects.create(
            org=org_with_transformation_tasks,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )
    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        dbtTransform="yes",
        cron="test-cron",
    )
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    deployment = post_prefect_dataflow_v1(mock_request, payload)

    assert deployment["deploymentId"] == "test-deploy-id"
    assert deployment["name"] == payload.name
    assert deployment["cron"] == payload.cron


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
