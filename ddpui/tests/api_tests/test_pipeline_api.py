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
    get_prefect_dataflows_v1,
    get_prefect_dataflow_v1,
    delete_prefect_dataflow_v1,
)
from ddpui.ddpprefect import (
    DBTCLIPROFILE,
    SECRET,
    AIRBYTESERVER,
    AIRBYTECONNECTION,
    DBTCORE,
    SHELLOPERATION,
)
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


def test_get_prefect_dataflows_v1_failure():
    """tests failure in get dataflows due to missing org"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflows_v1(mock_request)

    assert str(excinfo.value) == "register an organization first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_filtered_deployments=Mock(return_value=[]),
)
def test_get_prefect_dataflows_v1_success(org_with_transformation_tasks):
    """tests success with 0 dataflows for the org"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    dataflows = get_prefect_dataflows_v1(mock_request)

    assert len(dataflows) == 0


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_filtered_deployments=Mock(
        return_value=[
            {"deploymentId": "test-dep-id-1", "isScheduleActive": True},
            {"deploymentId": "test-dep-id-2", "isScheduleActive": False},
        ]
    ),
    get_last_flow_run_by_deployment_id=Mock(
        return_value="some-last-run-prefect-object"
    ),
)
def test_get_prefect_dataflows_v1_success2(org_with_transformation_tasks):
    """tests success with atleast 1 dataflow/pipeline for the org"""
    # setup two pipelines to fetch for the org
    flow0 = OrgDataFlowv1.objects.create(
        org=org_with_transformation_tasks,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )
    flow1 = OrgDataFlowv1.objects.create(
        org=org_with_transformation_tasks,
        name="flow-2",
        deployment_name="prefect-flow-2",
        deployment_id="test-dep-id-2",
        dataflow_type="orchestrate",
    )

    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    dataflows = get_prefect_dataflows_v1(mock_request)

    assert len(dataflows) == 2
    assert dataflows[0]["deploymentId"] == flow0.deployment_id
    assert dataflows[0]["name"] == flow0.name
    assert dataflows[0]["cron"] == flow0.cron
    assert dataflows[0]["deploymentName"] == flow0.deployment_name
    assert dataflows[0]["lastRun"] == "some-last-run-prefect-object"
    assert dataflows[0]["status"] is True
    assert dataflows[0]["lock"] is None

    assert dataflows[1]["status"] is False
    assert dataflows[1]["deploymentId"] == flow1.deployment_id


def test_get_prefect_dataflow_v1_failure():
    """tests failure in fetching a dataflow due to missing org"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(mock_request, "deployment-id")

    assert str(excinfo.value) == "register an organization first"


def test_get_prefect_dataflow_v1_failure2(org_with_transformation_tasks):
    """tests failure in fetching a dataflow that does not exist"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(mock_request, "deployment-id")

    assert str(excinfo.value) == "pipeline does not exist"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_deployment=Mock(side_effect=Exception("not found")),
)
def test_get_prefect_dataflow_v1_failure3(org_with_transformation_tasks):
    """tests failure in fetching a dataflow which prefect failed to fetch"""
    # create the dataflow to be fetched
    OrgDataFlowv1.objects.create(
        org=org_with_transformation_tasks,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(mock_request, "test-dep-id-1")

    assert str(excinfo.value) == "failed to get deploymenet from prefect-proxy"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_deployment=Mock(
        return_value={
            "name": "deployment-name",
            "cron": "cron-time",
            "isScheduleActive": True,
            "parameters": {
                "config": {
                    "tasks": [
                        {"type": AIRBYTECONNECTION, "seq": 1, "connection_id": "id-1"},
                        {"type": AIRBYTECONNECTION, "seq": 2, "connection_id": "id-2"},
                        {"type": SHELLOPERATION, "seq": 3},
                        {"type": DBTCORE, "seq": 4},
                    ]
                }
            },
        }
    ),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(
        return_value={
            "name": "fake-conn",
            "sourceId": "fake-source-id-1",
            "connectionId": "fake-connection-id-1",
            "destinationId": "fake-destination-id-1",
            "catalogId": "fake-source-catalog-id-1",
            "syncCatalog": "sync-catalog",
            "status": "conn-status",
            "source": {"id": "fake-source-id-1", "name": "fake-source-name-1"},
            "destination": {
                "id": "fake-destination-id-1",
                "name": "fake-destination-name-1",
            },
        }
    ),
)
def test_get_prefect_dataflow_v1_success(org_with_transformation_tasks):
    """tests success in fetching a dataflow with both transformation tasks and airbyte syncs"""
    # create the dataflow to be fetched
    OrgDataFlowv1.objects.create(
        org=org_with_transformation_tasks,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    dataflow = get_prefect_dataflow_v1(mock_request, "test-dep-id-1")

    assert dataflow["name"] == "flow-1"
    assert dataflow["deploymentName"] == "deployment-name"
    assert dataflow["cron"] == dataflow["cron"]
    assert len(dataflow["connections"]) == 2
    assert dataflow["dbtTransform"] == "yes"
    assert dataflow["isScheduleActive"] is True


def test_delete_prefect_dataflow_v1_failure():
    """tests failure in deleting a dataflow due to missing org"""
    mock_orguser = Mock()
    mock_orguser.org = None

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_prefect_dataflow_v1(mock_request, "deployment-id")

    assert str(excinfo.value) == "register an organization first"


def test_delete_prefect_dataflow_v1_failure2(org_with_transformation_tasks):
    """tests failure in deleting a dataflow due to not found error"""
    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    with pytest.raises(HttpError) as excinfo:
        delete_prefect_dataflow_v1(mock_request, "deployment-id")

    assert str(excinfo.value) == "pipeline not found"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_deployment_by_id=Mock(return_value=True),
)
def test_delete_prefect_dataflow_v1_success(org_with_transformation_tasks):
    """tests success in deleting a dataflow"""
    # create dataflow to delete
    OrgDataFlowv1.objects.create(
        org=org_with_transformation_tasks,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    mock_orguser = Mock()
    mock_orguser.org = org_with_transformation_tasks

    mock_request = Mock()
    mock_request.orguser = mock_orguser

    delete_prefect_dataflow_v1(mock_request, "test-dep-id-1")

    assert (
        OrgDataFlowv1.objects.filter(
            org=org_with_transformation_tasks, deployment_id="test-dep-id-1"
        ).count()
        == 0
    )


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
