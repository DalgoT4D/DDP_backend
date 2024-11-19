import os, uuid
from unittest.mock import Mock, patch

import django
import pytest
import yaml
import json
from pathlib import Path
from django.apps import apps
from django.core.management import call_command
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
    put_prefect_dataflow_v1,
    post_deployment_set_schedule,
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
    PrefectDataFlowUpdateSchema3,
    PrefectDataFlowOrgTasks,
)
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task, TaskLock
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.utils.constants import TASK_DBTRUN
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


@pytest.fixture(scope="session")
def seed_master_tasks_db(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        # Run the loaddata command to load the fixture
        call_command("loaddata", "tasks.json")


# ================================================================================


@pytest.fixture()
def org_without_dbt_workspace():
    """org without dbt workspace"""
    org_slug = "test-org-slug"

    org = Org.objects.create(
        airbyte_workspace_id="FAKE-WORKSPACE-ID-1",
        slug=org_slug,
        name=org_slug,
    )
    yield org
    org.delete()


@pytest.fixture
def orguser(org_without_dbt_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_without_dbt_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


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
    with open(str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8") as output:
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


@pytest.fixture
def orguser_dbt_workspace(org_with_dbt_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_with_dbt_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture()
def org_with_transformation_tasks(tmpdir_factory, seed_master_tasks_db):
    """org having the transformation tasks and dbt workspace"""
    org_slug = "test-org-slug"
    client_dir = tmpdir_factory.mktemp("clients")
    org_dir = client_dir.mkdir(org_slug)
    org_dir.mkdir("dbtrepo")

    os.environ["CLIENTDBT_ROOT"] = str(client_dir)

    # create dbt_project.yml file
    yml_obj = {"profile": "dummy"}
    with open(str(org_dir / "dbtrepo" / "dbt_project.yml"), "w", encoding="utf-8") as output:
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
        org_task = OrgTask.objects.create(org=org, task=task, uuid=uuid.uuid4())

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


@pytest.fixture
def orguser_transform_tasks(org_with_transformation_tasks):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=User.objects.create(
            username="tempusername", email="tempuseremail", password="tempuserpassword"
        ),
        org=org_with_transformation_tasks,
        role=OrgUserRole.ACCOUNT_MANAGER,
        email_verified=True,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


def test_seed_master_tasks(seed_master_tasks_db):
    """a test to seed the database"""
    assert Task.objects.count() == 11


# ================================================================================


def test_post_prefect_dataflow_v1_failure1(orguser):
    """tests the failure due to missing organization"""
    connections = [PrefectFlowAirbyteConnection2(id="test-conn-id", seq=1)]
    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        cron="",
        transformTasks=[],
    )
    orguser.org = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_prefect_dataflow_v1(request, payload)

    assert str(excinfo.value) == "register an organization first"


def test_post_prefect_dataflow_v1_failure2(orguser_transform_tasks):
    """tests the failure due to missing name of the dataflow in the payload"""
    connections = [PrefectFlowAirbyteConnection2(id="test-conn-id", seq=1)]
    payload = PrefectDataFlowCreateSchema4(
        name="", connections=connections, cron="", transformTasks=[]
    )
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        post_prefect_dataflow_v1(request, payload)

    assert str(excinfo.value) == "must provide a name for the flow"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
    ),
)
def test_post_prefect_dataflow_v1_success(orguser_transform_tasks):
    """tests the success of creating dataflow with only connections and no transform"""
    request = mock_request(orguser_transform_tasks)

    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=1),
    ]
    # create org tasks for these connections
    for conn in connections:
        OrgTask.objects.create(
            org=request.orguser.org,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )
    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        cron="test-cron",
        transformTasks=[],
    )

    deployment = post_prefect_dataflow_v1(request, payload)

    assert deployment["deploymentId"] == "test-deploy-id"
    assert deployment["name"] == payload.name
    assert deployment["cron"] == payload.cron

    # cleanup
    OrgTask.objects.filter(
        org=request.orguser.org,
        connection_id__in=["test-conn-id-1", "test-conn-id-1"],
    ).delete()


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
    ),
)
def test_post_prefect_dataflow_v1_success2(orguser_transform_tasks):
    """tests the success of creating dataflow/pipelin with connections and with system default tasks"""
    request = mock_request(orguser_transform_tasks)

    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=2),
    ]
    # create org tasks for these connections
    for conn in connections:
        OrgTask.objects.create(
            uuid=uuid.uuid4(),
            org=request.orguser.org,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )

    transform_tasks = OrgTask.objects.filter(
        org=request.orguser.org,
        generated_by="system",
        task__type__in=["dbt", "git"],
    ).all()

    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        cron="test-cron",
        transformTasks=[
            PrefectDataFlowOrgTasks(uuid=str(org_task.uuid), seq=idx)
            for idx, org_task in enumerate(transform_tasks)
        ],
    )

    deployment = post_prefect_dataflow_v1(request, payload)

    assert deployment["deploymentId"] == "test-deploy-id"
    assert deployment["name"] == payload.name
    assert deployment["cron"] == payload.cron

    # check sequence of tasks
    dataflow = OrgDataFlowv1.objects.filter(
        org=request.orguser.org, deployment_id="test-deploy-id"
    ).first()

    assert dataflow is not None

    # test the sequencing of connections
    for i, conn in enumerate(connections):
        dataflow_task = DataflowOrgTask.objects.filter(
            dataflow=dataflow, orgtask__connection_id=conn.id
        ).first()
        assert dataflow_task is not None
        assert dataflow_task.seq == i

    seq = len(connections)
    for i, org_task in enumerate(transform_tasks):
        dataflow_task = DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask=org_task).first()
        assert dataflow_task is not None
        assert dataflow_task.seq == seq + i

    # cleanup
    OrgTask.objects.filter(
        org=request.orguser.org,
        connection_id__in=["test-conn-id-1", "test-conn-id-1"],
    ).delete()


def test_get_prefect_dataflows_v1_failure(orguser):
    """tests failure in get dataflows due to missing org"""
    orguser.org = None

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflows_v1(request)

    assert str(excinfo.value) == "register an organization first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_filtered_deployments=Mock(return_value=[]),
)
def test_get_prefect_dataflows_v1_success(orguser_transform_tasks):
    """tests success with 0 dataflows for the org"""
    request = mock_request(orguser_transform_tasks)

    dataflows = get_prefect_dataflows_v1(request)

    assert len(dataflows) == 0


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_filtered_deployments=Mock(
        return_value=[
            {"deploymentId": "test-dep-id-1", "isScheduleActive": True},
            {"deploymentId": "test-dep-id-2", "isScheduleActive": False},
        ]
    ),
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
    ),
)
def test_get_prefect_dataflows_v1_success2(orguser_transform_tasks):
    """tests success with atleast 1 dataflow/pipeline for the org"""
    # setup two pipelines to fetch for the org
    request = mock_request(orguser_transform_tasks)

    flow0 = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )
    # last run of this sync deployment/dataflow
    PrefectFlowRun.objects.create(
        deployment_id="test-dep-id-1",
        flow_run_id="some-fake-run-id",
        name="pipeline-run",
        start_time="2022-01-01",
        expected_start_time="2022-01-01",
        total_run_time=12,
        status="COMPLETED",
        state_name="Completed",
    )

    flow1 = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-2",
        deployment_name="prefect-flow-2",
        deployment_id="test-dep-id-2",
        dataflow_type="orchestrate",
    )

    dataflows = get_prefect_dataflows_v1(request)

    assert len(dataflows) == 2
    assert dataflows[0]["deploymentId"] == flow0.deployment_id
    assert dataflows[0]["name"] == flow0.name
    assert dataflows[0]["cron"] == flow0.cron
    assert dataflows[0]["deploymentName"] == flow0.deployment_name
    assert dataflows[0]["lastRun"]["id"] == "some-fake-run-id"
    assert dataflows[0]["status"] is True
    assert dataflows[0]["lock"] is None

    assert dataflows[1]["status"] is False
    assert dataflows[1]["deploymentId"] == flow1.deployment_id

    # cleanup
    OrgDataFlowv1.objects.filter(
        org=request.orguser.org,
        deployment_id__in=["test-dep-id-1", "test-dep-id-2"],
    ).delete()


def test_get_prefect_dataflow_v1_failure(orguser):
    """tests failure in fetching a dataflow due to missing org"""
    orguser.org = None

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(request, "deployment-id")

    assert str(excinfo.value) == "register an organization first"


def test_get_prefect_dataflow_v1_failure2(orguser_transform_tasks):
    """tests failure in fetching a dataflow that does not exist"""
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(request, "deployment-id")

    assert str(excinfo.value) == "pipeline does not exist"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_deployment=Mock(side_effect=Exception("not found")),
)
def test_get_prefect_dataflow_v1_failure3(orguser_transform_tasks):
    """tests failure in fetching a dataflow which prefect failed to fetch"""
    # create the dataflow to be fetched
    request = mock_request(orguser_transform_tasks)

    OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    with pytest.raises(HttpError) as excinfo:
        get_prefect_dataflow_v1(request, "test-dep-id-1")

    assert str(excinfo.value) == "failed to get deploymenet from prefect-proxy"

    # cleanup
    OrgDataFlowv1.objects.filter(org=request.orguser.org, deployment_id="test-dep-id-1").delete()


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    get_deployment=Mock(
        return_value={
            "name": "test-deploy",
            "cron": "test-cron",
            "isScheduleActive": True,
            "parameters": {
                "config": {
                    "tasks": [
                        {
                            "type": AIRBYTECONNECTION,
                            "seq": 1,
                            "connection_id": "test-conn-id-1",
                        },
                        {
                            "type": AIRBYTECONNECTION,
                            "seq": 2,
                            "connection_id": "test-conn-id-2",
                        },
                        {"type": SHELLOPERATION, "seq": 3},
                        {"type": DBTCORE, "seq": 4},
                    ]
                }
            },
        }
    ),
    create_dataflow_v1=Mock(
        return_value={"deployment": {"name": "test-deploy", "id": "test-deploy-id"}}
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
def test_get_prefect_dataflow_v1_success(orguser_transform_tasks):
    """tests success in fetching a dataflow with both default system transformation tasks and airbyte syncs"""
    request = mock_request(orguser_transform_tasks)

    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=2),
    ]
    # create org tasks for these connections
    for conn in connections:
        OrgTask.objects.create(
            uuid=uuid.uuid4(),
            org=request.orguser.org,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )

    transform_tasks = OrgTask.objects.filter(
        org=request.orguser.org,
        generated_by="system",
        task__type__in=["dbt", "git"],
    ).all()

    payload = PrefectDataFlowCreateSchema4(
        name="test-dataflow",
        connections=connections,
        cron="test-cron",
        transformTasks=[
            PrefectDataFlowOrgTasks(uuid=str(org_task.uuid), seq=idx)
            for idx, org_task in enumerate(transform_tasks)
        ],
    )

    deployment = post_prefect_dataflow_v1(request, payload)

    assert deployment["deploymentId"] == "test-deploy-id"
    assert deployment["name"] == payload.name
    assert deployment["cron"] == payload.cron

    created_dataflow = OrgDataFlowv1.objects.filter(
        org=request.orguser.org, deployment_id="test-deploy-id"
    ).first()

    assert created_dataflow is not None

    dataflow = get_prefect_dataflow_v1(request, "test-deploy-id")

    assert dataflow["name"] == created_dataflow.name
    assert dataflow["deploymentName"] == created_dataflow.deployment_name
    assert dataflow["cron"] == created_dataflow.cron
    assert len(dataflow["connections"]) == 2
    assert dataflow["dbtTransform"] == "yes"
    assert dataflow["isScheduleActive"] is True
    assert set([conn["id"] for conn in dataflow["connections"]]) == set(
        [conn.id for conn in connections]
    )

    # cleanup
    OrgDataFlowv1.objects.filter(org=request.orguser.org, deployment_id="test-dep-id-1").delete()


def test_delete_prefect_dataflow_v1_failure(orguser):
    """tests failure in deleting a dataflow due to missing org"""
    orguser.org = None

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_prefect_dataflow_v1(request, "deployment-id")

    assert str(excinfo.value) == "register an organization first"


def test_delete_prefect_dataflow_v1_failure2(orguser_transform_tasks):
    """tests failure in deleting a dataflow due to not found error"""
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        delete_prefect_dataflow_v1(request, "deployment-id")

    assert str(excinfo.value) == "pipeline not found"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_deployment_by_id=Mock(return_value=True),
)
def test_delete_prefect_dataflow_v1_success(orguser_transform_tasks):
    """tests success in deleting a dataflow"""
    # create dataflow to delete
    request = mock_request(orguser_transform_tasks)

    OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    delete_prefect_dataflow_v1(request, "test-dep-id-1")

    assert (
        OrgDataFlowv1.objects.filter(org=request.orguser.org, deployment_id="test-dep-id-1").count()
        == 0
    )


def test_put_prefect_dataflow_v1_failure(orguser):
    """tests failure in update dataflow due to missing org"""
    orguser.org = None

    request = mock_request(orguser)

    payload = PrefectDataFlowUpdateSchema3(
        name="put-dataflow",
        connections=[],
        transformTasks=[],
        cron="",
    )

    with pytest.raises(HttpError) as excinfo:
        put_prefect_dataflow_v1(request, "deployment-id", payload)

    assert str(excinfo.value) == "register an organization first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    update_dataflow_v1=Mock(return_value=[]),
)
def test_put_prefect_dataflow_v1_success(orguser_transform_tasks):
    """tests success in update dataflow; remove all connection syncs & keep the transform on"""
    # create pipeline with airbyte syncs + dbt transform
    request = mock_request(orguser_transform_tasks)

    dataflow = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=2),
    ]

    # create org tasks for these connections
    for i, conn in enumerate(connections):
        org_task = OrgTask.objects.create(
            uuid=uuid.uuid4(),
            org=request.orguser.org,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )
        DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task, seq=i)

    seq = len(connections)

    transform_tasks = OrgTask.objects.filter(
        org=request.orguser.org,
        generated_by="system",
        task__type__in=["dbt", "git"],
    ).all()
    for i, transform_task in enumerate(transform_tasks):
        DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=transform_task, seq=seq + i)

    payload = PrefectDataFlowUpdateSchema3(
        name="put-dataflow",
        connections=[],
        transformTasks=[
            PrefectDataFlowOrgTasks(uuid=str(org_task.uuid), seq=idx)
            for idx, org_task in enumerate(transform_tasks)
        ],
        cron="",
    )

    put_prefect_dataflow_v1(request, "test-dep-id-1", payload)

    assert (
        DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="airbyte").count()
        == 0
    )
    assert DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="git").count() == 1
    assert DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="dbt").count() >= 1


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    update_dataflow_v1=Mock(return_value=[]),
)
def test_put_prefect_dataflow_v1_success2(orguser_transform_tasks):
    """tests success in update dataflow; add connection syncs & keep the transform on"""
    # create pipeline with dbt transform default system tasks
    request = mock_request(orguser_transform_tasks)

    dataflow = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="flow-1",
        deployment_name="prefect-flow-1",
        deployment_id="test-dep-id-1",
        dataflow_type="orchestrate",
    )

    connections = [
        PrefectFlowAirbyteConnection2(id="test-conn-id-1", seq=1),
        PrefectFlowAirbyteConnection2(id="test-conn-id-2", seq=2),
    ]

    # create org tasks for these connections
    for i, conn in enumerate(connections):
        OrgTask.objects.create(
            uuid=uuid.uuid4(),
            org=request.orguser.org,
            task=Task.objects.filter(type__in=["airbyte"]).first(),
            connection_id=conn.id,
        )

    transform_tasks = OrgTask.objects.filter(
        org=request.orguser.org,
        generated_by="system",
        task__type__in=["dbt", "git"],
    ).all()
    for i, transform_task in enumerate(transform_tasks):
        DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=transform_task, seq=i)

    payload = PrefectDataFlowUpdateSchema3(
        name="put-dataflow",
        connections=[
            PrefectFlowAirbyteConnection2(id=conn.id, seq=i) for i, conn in enumerate(connections)
        ],
        transformTasks=[
            PrefectDataFlowOrgTasks(uuid=str(org_task.uuid), seq=idx)
            for idx, org_task in enumerate(transform_tasks)
        ],
        cron="",
    )

    put_prefect_dataflow_v1(request, "test-dep-id-1", payload)

    assert (
        DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="airbyte").count()
        == 2
    )

    assert DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="git").count() == 1
    assert DataflowOrgTask.objects.filter(dataflow=dataflow, orgtask__task__type="dbt").count() >= 1


def test_post_deployment_set_schedule_failure(orguser):
    """tests failure in setting schedule for dataflow due to missing org"""
    orguser.org = None

    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_deployment_set_schedule(request, "deployment-id", "active")

    assert str(excinfo.value) == "register an organization first"


def test_post_deployment_set_schedule_failure2(orguser_transform_tasks):
    """tests failure in setting schedule for dataflow due to incorrect status value"""
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        post_deployment_set_schedule(request, "deployment-id", "some-fake-status-value")

    assert str(excinfo.value) == "incorrect status value"

    with pytest.raises(HttpError) as excinfo:
        post_deployment_set_schedule(request, "deployment-id", None)

    assert str(excinfo.value) == "incorrect status value"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    set_deployment_schedule=Mock(side_effect=Exception("error")),
)
def test_post_deployment_set_schedule_failure3(orguser_transform_tasks):
    """tests failure in setting schedule for dataflow due to error from prefect"""
    request = mock_request(orguser_transform_tasks)

    with pytest.raises(HttpError) as excinfo:
        post_deployment_set_schedule(request, "deployment-id", "active")

    assert str(excinfo.value) == "failed to change flow state"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    set_deployment_schedule=Mock(return_value=True),
)
def test_post_deployment_set_schedule_success(orguser_transform_tasks):
    """tests success in setting schedule for dataflow"""
    request = mock_request(orguser_transform_tasks)

    res = post_deployment_set_schedule(request, "deployment-id", "active")

    assert res["success"] == 1


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_deployment_flow_run=Mock(return_value={"flow_run_id": "fake-flow-run-id"}),
    lock_tasks_for_deployment=Mock(return_value=[]),
)
def test_post_run_prefect_org_deployment_task_success(orguser_transform_tasks):
    """tests POST /v1/flows/{deployment_id}/flow_run/ success"""
    request = mock_request(orguser_transform_tasks)

    dataflow_orgtask = None
    org_task = OrgTask.objects.filter(org=request.orguser.org, task__slug=TASK_DBTRUN).first()
    if org_task:
        dataflow_orgtask = DataflowOrgTask.objects.filter(orgtask=org_task).first()

    if dataflow_orgtask is None:
        raise Exception("Deployment not found")

    post_run_prefect_org_deployment_task(request, dataflow_orgtask.dataflow.deployment_id)

    assert TaskLock.objects.filter(orgtask=org_task).count() == 1
