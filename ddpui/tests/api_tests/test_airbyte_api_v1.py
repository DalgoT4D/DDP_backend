import os
from unittest.mock import MagicMock, Mock, patch
from django.core.management import call_command

import django
from flags.state import enable_flag
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from ddpui import ddpprefect
from ddpui.api.airbyte_api import (
    delete_airbyte_connection_v1,
    delete_airbyte_source_v1,
    get_airbyte_connection_v1,
    get_airbyte_connections_v1,
    get_connection_catalog_v1,
    get_latest_job_for_connection,
    post_airbyte_connection_reset_v1,
    post_airbyte_connection_v1,
    post_airbyte_workspace_v1,
    put_airbyte_connection_v1,
    put_airbyte_destination_v1,
    update_connection_schema,
    get_sync_history_for_connection,
    schedule_update_connection_schema,
)
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.ddpairbyte.schema import (
    AirbyteConnectionCreate,
    AirbyteConnectionSchemaUpdate,
    AirbyteConnectionUpdate,
    AirbyteDestinationUpdate,
    AirbyteWorkspace,
    AirbyteWorkspaceCreate,
)
from ddpui.auth import ACCOUNT_MANAGER_ROLE, SUPER_ADMIN_ROLE
from ddpui.models.org_user import OrgUser, OrgUserRole
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgWarehouse
from ddpui.models.tasks import DataflowOrgTask, OrgDataFlowv1, OrgTask, Task
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db


# ================================================================================
@pytest.fixture
def authuser():
    """a django User object"""
    user = User.objects.create(
        username="tempusername", email="tempuseremail", password="tempuserpassword"
    )
    yield user
    user.delete()


@pytest.fixture
def org_without_workspace():
    """a pytest fixture which creates an Org without an airbyte workspace"""
    print("creating org_without_workspace")
    org = Org.objects.create(airbyte_workspace_id=None, slug="test-org-slug")
    yield org
    print("deleting org_without_workspace")
    org.delete()


@pytest.fixture
def org_with_workspace():
    """a pytest fixture which creates an Org having an airbyte workspace"""
    print("creating org_with_workspace")
    org = Org.objects.create(airbyte_workspace_id="FAKE-WORKSPACE-ID", slug="test-org-slug")
    yield org
    print("deleting org_with_workspace")
    org.delete()


@pytest.fixture
def orguser(authuser, org_without_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_without_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield orguser
    orguser.delete()


@pytest.fixture
def orguser_workspace(authuser, org_with_workspace):
    """a pytest fixture representing an OrgUser having the account-manager role"""
    orguser = OrgUser.objects.create(
        user=authuser,
        org=org_with_workspace,
        role=OrgUserRole.ACCOUNT_MANAGER,
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


# ================================================================================
def test_get_airbyte_connection_v1_without_workspace(orguser):
    """tests GET /v1/connections/{connection_id} failure with no workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connection_v1(request, "fake-conn-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


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
            "namespaceDefinition": "namespace-definition",
            "status": "conn-status",
            "source": {"id": "fake-source-id-1", "name": "fake-source-name-1"},
            "destination": {
                "id": "fake-destination-id-1",
                "name": "fake-destination-name-1",
            },
        }
    ),
)
def test_get_airbyte_connection_v1_success(orguser_workspace):
    """tests GET /v1/connections/{connection_id} success"""
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=request.orguser.org, connection_id="fake-conn-id"
    )

    dataflow = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
    )

    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    result = get_airbyte_connection_v1(request, "fake-conn-id")

    assert result["name"] == "fake-conn"
    assert result["source"]["id"] == "fake-source-id-1"
    assert result["source"]["name"] == "fake-source-name-1"
    assert result["destination"]["id"] == "fake-destination-id-1"
    assert result["destination"]["name"] == "fake-destination-name-1"
    assert result["catalogId"] == "fake-source-catalog-id-1"
    assert result["syncCatalog"] == "sync-catalog"
    assert result["status"] == "conn-status"
    assert result["deploymentId"] == "fake-deployment-id"


# ================================================================================
def test_get_airbyte_connections_v1_without_workspace(orguser):
    """tests GET /v1/connections failure with no workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        get_airbyte_connections_v1(request)

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_webbackend_connections=Mock(
        return_value=[
            {
                "name": "fake-conn",
                "sourceId": "fake-source-id-1",
                "connectionId": "fake-connection-id-1",
                "destinationId": "fake-destination-id-1",
                "status": "conn-status",
                "source": {"id": "fake-source-id-1", "name": "fake-source-name-1"},
                "destination": {
                    "id": "fake-destination-id-1",
                    "name": "fake-destination-name-1",
                },
            }
        ]
    ),
)
def test_get_airbyte_connections_success(orguser_workspace):
    """tests GET /v1/connections success"""
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=request.orguser.org, connection_id="fake-connection-id-1"
    )

    # each connection will also have this
    reset_dataflow = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="test-reset-deployment",
        deployment_id="fake-reset-conn-deployment-id",
        deployment_name="test-reset-deployment",
        cron=None,
        dataflow_type="manual",
    )

    dataflow = OrgDataFlowv1.objects.create(
        org=request.orguser.org,
        name="test-deployment",
        deployment_id="fake-deployment-id",
        deployment_name="test-deployment",
        cron=None,
        dataflow_type="manual",
        reset_conn_dataflow=reset_dataflow,
    )

    # last run of this sync deployment/dataflow
    PrefectFlowRun.objects.create(
        deployment_id="fake-deployment-id",
        flow_run_id="some-fake-run-id",
        name="airbyte-sync-run",
        start_time="2022-01-01",
        expected_start_time="2022-01-01",
        total_run_time=12,
        status="COMPLETED",
        state_name="Completed",
    )

    DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=org_task)

    OrgWarehouse.objects.create(org=request.orguser.org, name="fake-warehouse-name")

    result = get_airbyte_connections_v1(request)
    assert len(result) == 1

    assert result[0]["name"] == "fake-conn"
    assert result[0]["source"]["id"] == "fake-source-id-1"
    assert result[0]["source"]["name"] == "fake-source-name-1"
    assert result[0]["destination"]["id"] == "fake-destination-id-1"
    assert result[0]["destination"]["name"] == "fake-warehouse-name"
    assert result[0]["status"] == "conn-status"
    assert result[0]["deploymentId"] == "fake-deployment-id"
    assert result[0]["lastRun"]["id"] == "some-fake-run-id"
    assert result[0]["lastRun"]["deployment_id"] == "fake-deployment-id"
    assert result[0]["lastRun"]["name"] == "airbyte-sync-run"
    assert result[0]["lastRun"]["state_name"] == "Completed"
    assert result[0]["lastRun"]["status"] == "COMPLETED"
    assert result[0]["lock"] is None


# ================================================================================
def test_post_airbyte_connection_v1_without_workspace(orguser):
    """tests POST /v1/connections/ failure with no workspace"""
    request = mock_request(orguser)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream-1", "stream-2"],
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(request, payload)

    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_v1_without_stream_names(orguser_workspace):
    """tests POST /v1/connections/ failure with no streams passed in payload"""
    request = mock_request(orguser_workspace)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=[],
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(request, payload)

    assert str(excinfo.value) == "must specify stream names"


def test_post_airbyte_connection_v1_without_warehouse(orguser_workspace):
    """tests POST /v1/connections/ failure without warehouse"""
    request = mock_request(orguser_workspace)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(request, payload)

    assert str(excinfo.value) == "need to set up a warehouse first"


@pytest.fixture
def warehouse_without_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(org=org_with_workspace)
    yield warehouse
    warehouse.delete()


@pytest.fixture
def warehouse_with_destination(org_with_workspace):
    warehouse = OrgWarehouse.objects.create(
        org=org_with_workspace, airbyte_destination_id="destination-id"
    )
    yield warehouse
    warehouse.delete()


def test_post_airbyte_connection_v1_without_destination_id(
    orguser_workspace, warehouse_without_destination
):
    """tests POST /v1/connections/ failure without warehouse"""
    request = mock_request(orguser_workspace)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
    )
    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_v1(request, payload)

    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


def test_post_airbyte_connection_v1_without_server_block(
    orguser_workspace, warehouse_with_destination
):
    """tests POST /v1/connections/ failure without server block"""
    request = mock_request(orguser_workspace)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
    )
    with pytest.raises(Exception) as excinfo:
        post_airbyte_connection_v1(request, payload)
    assert str(excinfo.value) == "test-org-slug has no Airbyte Server block in OrgPrefectBlock"


@pytest.fixture
def airbyte_server_block(org_with_workspace):
    block = OrgPrefectBlockv1.objects.create(
        org=org_with_workspace,
        block_type=ddpprefect.AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake ab server block",
    )
    yield block
    block.delete()


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
)
def test_post_airbyte_connection_v1_task_not_supported(
    orguser_workspace, warehouse_with_destination, airbyte_server_block
):
    """tests POST /v1/connections/ success"""
    request = mock_request(orguser_workspace)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
    )
    with pytest.raises(Exception) as excinfo:
        post_airbyte_connection_v1(request, payload)
    assert str(excinfo.value) == "sync task not supported"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    create_connection=Mock(
        return_value={
            "sourceId": "fake-source-id",
            "destinationId": "fake-destination-id",
            "connectionId": "fake-connection-id",
            "sourceCatalogId": "fake-source-catalog-id",
            "syncCatalog": "sync-catalog",
            "status": "running",
        }
    ),
    delete_connection=Mock(),
)
@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    create_dataflow_v1=Mock(
        side_effect=[
            {
                "deployment": {
                    "id": "fake-deployment-id",
                    "name": "fake-deployment-name",
                },
            },
            {
                "deployment": {
                    "id": "fake-reset-conn-deployment-id",
                    "name": "fake-deployment-name",
                },
            },
        ]
    ),
)
def test_post_airbyte_connection_v1_success(
    orguser_workspace, warehouse_with_destination, airbyte_server_block
):
    """tests POST /v1/connections/ success"""
    request = mock_request(orguser_workspace)

    call_command("loaddata", "seed/tasks.json")
    for task in Task.objects.all():
        OrgTask.objects.create(org=request.orguser.org, task=task)

    payload = AirbyteConnectionCreate(
        name="conn-name",
        sourceId="source-id",
        destinationId="dest-id",
        destinationSchema="dest-schema",
        streams=["stream_1", "stream_2"],
    )

    # create the master task
    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    Task.objects.create(**airbyte_task_config)
    airbyte_reset_task_config = {
        "type": "airbyte",
        "slug": "airbyte-reset",
        "label": "AIRBYTE reset",
        "command": None,
    }
    Task.objects.create(**airbyte_reset_task_config)

    response = post_airbyte_connection_v1(request, payload)

    assert response["name"] == "conn-name"
    assert response["connectionId"] == "fake-connection-id"
    assert response["source"]["id"] == "fake-source-id"
    assert response["destination"]["id"] == "fake-destination-id"
    assert response["catalogId"] == "fake-source-catalog-id"
    assert response["status"] == "running"
    assert response["deploymentId"] == "fake-deployment-id"


# ================================================================================
def test_post_airbyte_connection_reset_v1_no_workspace(orguser):
    """tests POST /v1/connections/{connection_id}/reset failure with no workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset_v1(request, "connection_id")
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_post_airbyte_connection_reset_v1_no_connection_task(
    orguser_workspace, airbyte_server_block
):
    """tests POST /v1/connections/{connection_id}/reset failure with connection task created"""
    request = mock_request(orguser_workspace)

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset_v1(request, "connection_id")
    assert (
        str(excinfo.value)
        == "Reset job is disabled. Please contact the dalgo support team at support@dalgo.in"
    )


def test_post_airbyte_connection_reset_v1_success(orguser_workspace, airbyte_server_block):
    """tests POST /v1/connections/{connection_id}/reset success"""
    request = mock_request(orguser_workspace)

    connection_id = "connection_id"

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(task=task, org=request.orguser.org, connection_id=connection_id)

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_connection_reset_v1(request, connection_id)
    assert (
        str(excinfo.value)
        == "Reset job is disabled. Please contact the dalgo support team at support@dalgo.in"
    )

    enable_flag("AIRBYTE_RESET_JOB")
    with patch("ddpui.ddpairbyte.airbytehelpers.reset_connection") as reset_helper_mock:
        reset_helper_mock.return_value = (None, None)
        post_airbyte_connection_reset_v1(request, connection_id)

        reset_helper_mock.assert_called_once()


# ================================================================================
def test_put_airbyte_connection_v1_no_workspace(orguser):
    """tests PUT /v1/connections/{connection_id}/update failure with no workspace"""
    payload = AirbyteConnectionUpdate(name="connection-name-1", streams=[])
    connection_id = "connection_id"
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(request, connection_id, payload)
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_put_airbyte_connection_v1_no_warehouse_destination(orguser_workspace):
    """tests PUT /v1/connections/{connection_id}/update failure with warehouse but no destination id"""
    payload = AirbyteConnectionUpdate(name="connection-block-name", streams=[1])
    connection_id = "connection_id"
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(task=task, org=request.orguser.org, connection_id=connection_id)

    OrgWarehouse.objects.create(org=request.orguser.org)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(request, connection_id, payload)
    assert str(excinfo.value) == "warehouse has no airbyte_destination_id"


def test_put_airbyte_connection_v1_no_warehouse(orguser_workspace):
    """tests PUT /v1/connections/{connection_id}/update failure with no warehouse"""
    payload = AirbyteConnectionUpdate(name="connection-name", streams=[1])
    connection_id = "connection_id"
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(task=task, org=request.orguser.org, connection_id=connection_id)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(request, connection_id, payload)
    assert str(excinfo.value) == "need to set up a warehouse first"


def test_put_airbyte_connection_v1_no_streams(orguser_workspace, warehouse_with_destination):
    """tests PUT /v1/connections/{connection_id}/update failure with no streams"""
    payload = AirbyteConnectionUpdate(name="connection-name-1", streams=[])
    connection_id = "connection_id"
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(task=task, org=request.orguser.org, connection_id=connection_id)

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_connection_v1(request, connection_id, payload)
    assert str(excinfo.value) == "must specify stream names"


@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    get_connection=Mock(return_value={"conn-key": "conn-val"}),
)
def test_put_airbyte_connection_v1(orguser_workspace):
    """tests PUT /v1/connections/{connection_id}/update success"""
    payload = AirbyteConnectionUpdate(name="connection-name", streams=[1])
    connection_id = "connection_id"
    request = mock_request(orguser_workspace)

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    OrgTask.objects.create(task=task, org=request.orguser.org, connection_id=connection_id)

    warehouse = OrgWarehouse.objects.create(
        org=request.orguser.org, airbyte_destination_id="airbyte_destination_id"
    )

    update_connection_mock = MagicMock()
    with patch("ddpui.ddpairbyte.airbyte_service.update_connection") as update_connection_mock:
        put_airbyte_connection_v1(request, connection_id, payload)

    connection = {
        "conn-key": "conn-val",
        "operationIds": [],
        "name": payload.name,
        "skipReset": False,
    }
    payload.destinationId = warehouse.airbyte_destination_id
    update_connection_mock.assert_called_with(
        request.orguser.org.airbyte_workspace_id, payload, connection
    )


# ================================================================================
def test_delete_airbyte_connection_v1_without_org(orguser):
    """tests DELETE /v1/connections/{connection_id} failure without org"""
    orguser.org = None
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection_v1(request, "conn-block-id")

    assert str(excinfo.value) == "create an organization first"


def test_delete_airbyte_connection_v1_without_workspace(orguser):
    """tests DELETE /v1/connections/{connection_id} failure without workspace"""
    request = mock_request(orguser)

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_connection_v1(request, "conn-id")

    assert str(excinfo.value) == "create an airbyte workspace first"


@patch.multiple(
    "ddpui.ddpprefect.prefect_service",
    delete_deployment_by_id=Mock(),
)
@patch.multiple(
    "ddpui.ddpairbyte.airbyte_service",
    delete_connection=Mock(),
)
def test_delete_airbyte_connection_success(orguser_workspace):
    """tests DELETE /v1/connections/{connection_id} success"""
    request = mock_request(orguser_workspace)

    connection_id = "conn-1"

    airbyte_task_config = {
        "type": "airbyte",
        "slug": "airbyte-sync",
        "label": "AIRBYTE sync",
        "command": None,
    }
    task = Task.objects.create(**airbyte_task_config)

    org_task = OrgTask.objects.create(
        task=task, org=request.orguser.org, connection_id=connection_id
    )

    dataflow = OrgDataFlowv1.objects.create(
        name="conn-deployment",
        deployment_name="conn-deployment",
        deployment_id="conn-deployment-id",
        cron=None,
        dataflow_type="manual",
        org=request.orguser.org,
    )

    DataflowOrgTask.objects.create(orgtask=org_task, dataflow=dataflow)

    assert (
        OrgTask.objects.filter(
            org=request.orguser.org, task=task, connection_id=connection_id
        ).count()
        == 1
    )
    response = delete_airbyte_connection_v1(request, connection_id)
    assert response["success"] == 1
    assert OrgTask.objects.filter(org=request.orguser.org).count() == 0
    assert OrgDataFlowv1.objects.filter(org=request.orguser.org).count() == 0
    assert DataflowOrgTask.objects.filter(orgtask__org=request.orguser.org).count() == 0


def test_post_airbyte_workspace_with_existing_workspace(orguser_workspace):
    """Tests post_airbyte_workspace_v1 when organization already has a workspace"""
    # only super admins can create new orgs
    orguser_workspace.new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
    request = mock_request(orguser_workspace)

    payload = AirbyteWorkspaceCreate(name="New Workspace")

    with pytest.raises(HttpError) as excinfo:
        post_airbyte_workspace_v1(request, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "org already has a workspace"


def test_post_airbyte_workspace_success(orguser):
    """Tests post_airbyte_workspace_v1"""
    # only super admins can create new orgs
    orguser.new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
    request = mock_request(orguser)

    payload = AirbyteWorkspaceCreate(name="New Workspace")

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.setup_airbyte_workspace_v1"
    ) as setup_workspace_mock:
        setup_workspace_mock.return_value = AirbyteWorkspace(
            workspaceId=1, name="New Workspace", initialSetupComplete=True
        )

        workspace = post_airbyte_workspace_v1(request, payload)

    setup_workspace_mock.assert_called_once_with(payload.name, request.orguser.org)

    assert isinstance(workspace, AirbyteWorkspace)
    assert workspace.name == "New Workspace"


def test_get_latest_job_for_connection_with_no_workspace(orguser):
    """Tests get_latest_job_for_connection when organization has no workspace"""
    request = mock_request(orguser)

    connection_id = "connection_123"

    with pytest.raises(HttpError) as excinfo:
        get_latest_job_for_connection(request, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_get_latest_job_for_connection_with_workspace(orguser):
    """Tests get_latest_job_for_connection when organization has a workspace"""
    orguser.org.airbyte_workspace_id = "workspace_123"

    request = mock_request(orguser)

    connection_id = "connection_123"

    with patch("ddpui.ddpairbyte.airbytehelpers.get_job_info_for_connection") as get_job_info_mock:
        job_info = {"status": "success", "details": "Job completed successfully"}
        get_job_info_mock.return_value = (job_info, None)

        returned_job_info = get_latest_job_for_connection(request, connection_id)

    get_job_info_mock.assert_called_once_with(request.orguser.org, connection_id)

    assert returned_job_info == job_info


def test_get_latest_job_for_connection_with_error(orguser):
    """Tests get_latest_job_for_connection when job information retrieval returns an error"""
    orguser.org.airbyte_workspace_id = "workspace_123"

    request = mock_request(orguser)

    connection_id = "connection_123"

    with patch("ddpui.ddpairbyte.airbytehelpers.get_job_info_for_connection") as get_job_info_mock:
        error_message = "Failed to retrieve job information"
        get_job_info_mock.return_value = (None, error_message)

        with pytest.raises(HttpError) as excinfo:
            get_latest_job_for_connection(request, connection_id)

    get_job_info_mock.assert_called_once_with(request.orguser.org, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == error_message


def test_get_sync_history_for_connection_with_no_workspace(orguser):
    """Tests get_latest_job_for_connection when organization has no workspace"""
    request = mock_request(orguser)

    connection_id = "connection_123"

    with pytest.raises(HttpError) as excinfo:
        get_sync_history_for_connection(request, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_get_sync_history_for_connection_with_workspace(orguser):
    """Tests get_latest_job_for_connection when organization has a workspace"""
    orguser.org.airbyte_workspace_id = "workspace_123"

    request = mock_request(orguser)

    connection_id = "connection_123"

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.get_sync_job_history_for_connection"
    ) as get_job_info_mock:
        job_info = {"status": "success", "details": "Job completed successfully"}
        get_job_info_mock.return_value = (job_info, None)

        returned_job_info = get_sync_history_for_connection(
            request, connection_id, limit=1, offset=0
        )

    get_job_info_mock.assert_called_once_with(request.orguser.org, connection_id, limit=1, offset=0)

    assert returned_job_info == job_info


def test_get_sync_history_for_connection_with_error(orguser):
    """Tests get_latest_job_for_connection when job information retrieval returns an error"""
    orguser.org.airbyte_workspace_id = "workspace_123"

    request = mock_request(orguser)

    connection_id = "connection_123"

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.get_sync_job_history_for_connection"
    ) as get_job_info_mock:
        error_message = "Failed to retrieve job information"
        get_job_info_mock.return_value = (None, error_message)

        with pytest.raises(HttpError) as excinfo:
            get_sync_history_for_connection(request, connection_id, limit=1, offset=0)

    get_job_info_mock.assert_called_once_with(request.orguser.org, connection_id, limit=1, offset=0)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == error_message


def test_put_airbyte_destination_with_no_organization(orguser):
    """Tests put_airbyte_destination_v1 when organization is missing"""
    orguser.org = None

    request = mock_request(orguser)

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_destination_v1(request, destination_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an organization first"


def test_put_airbyte_destination_with_no_workspace(orguser):
    """Tests put_airbyte_destination_v1 when organization has no workspace"""
    request = mock_request(orguser)

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with pytest.raises(HttpError) as excinfo:
        put_airbyte_destination_v1(request, destination_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_put_airbyte_destination_success(orguser_workspace):
    """Tests put_airbyte_destination_v1 when updating destination is successful"""
    request = mock_request(orguser_workspace)

    destination_id = "destination_123"
    payload = AirbyteDestinationUpdate(
        name="Updated Destination", destinationDefId="def_123", config={}
    )

    with patch("ddpui.ddpairbyte.airbytehelpers.update_destination") as update_destination_mock:
        updated_destination = {
            "destinationId": destination_id,
            "name": "Updated Destination",
        }
        update_destination_mock.return_value = (updated_destination, None)

        response = put_airbyte_destination_v1(request, destination_id, payload)

    update_destination_mock.assert_called_once_with(request.orguser.org, destination_id, payload)

    assert response == {"destinationId": destination_id}


def test_delete_airbyte_source_with_no_workspace(orguser):
    """Tests delete_airbyte_source_v1 when organization has no workspace"""
    request = mock_request(orguser)

    source_id = "source_123"

    with pytest.raises(HttpError) as excinfo:
        delete_airbyte_source_v1(request, source_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_delete_airbyte_source_success(orguser_workspace):
    """Tests delete_airbyte_source_v1 when deleting source is successful"""
    request = mock_request(orguser_workspace)

    source_id = "source_123"

    with patch("ddpui.ddpairbyte.airbytehelpers.delete_source") as delete_source_mock:
        delete_source_mock.return_value = (None, None)

        response = delete_airbyte_source_v1(request, source_id)

    delete_source_mock.assert_called_once_with(request.orguser.org, source_id)

    assert response == {"success": 1}


def test_get_connection_catalog_v1_no_workspace(orguser):
    """Tests get_connection_catalog_v1 when organization has no workspace"""
    request = mock_request(orguser)

    connection_id = "connection_123"

    with pytest.raises(HttpError) as excinfo:
        get_connection_catalog_v1(request, connection_id)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_update_schema_changes_connection_with_no_workspace(orguser):
    """Tests update_schema_changes_connection_v1 when organization has no workspace"""
    request = mock_request(orguser)

    connection_id = "connection_123"
    payload = {"schemaChange": "true"}

    with pytest.raises(HttpError) as excinfo:
        update_connection_schema(request, connection_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


# write success test case for update_connection_schema


def test_update_schema_changes_connection_success(orguser_workspace):
    """Tests update_connection_schema when updating schema changes is successful"""
    request = mock_request(orguser_workspace)

    OrgPrefectBlockv1.objects.create(
        org=request.orguser.org,
        block_type=ddpprefect.AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake ab server block",
    )

    connection_id = "connection_123"
    payload = AirbyteConnectionSchemaUpdate(
        name="Updated Connection",
        connectionId="connection_123",
        syncCatalog={},
        sourceCatalogId="source_catalog_id",
    )

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.update_connection_schema"
    ) as update_schema_changes_connection_mock:
        updated_connection = {
            "connectionId": connection_id,
            "name": "Updated Connection",
            "syncCatalog": {},
            "catalogId": "source_catalog_id",
            "schemaChange": "no_change",
        }
        update_schema_changes_connection_mock.return_value = (updated_connection, None)

        response = update_connection_schema(request, connection_id, payload)

    update_schema_changes_connection_mock.assert_called_once_with(
        request.orguser.org, connection_id, payload
    )

    assert "connectionId" in response
    assert "syncCatalog" in response
    assert "catalogId" in response
    assert "schemaChange" in response


def test_schedule_update_connection_schema_workspace_not_found(orguser):
    """Tests schedule_update_connection_schema when organization has no workspace"""
    request = mock_request(orguser)

    connection_id = "connection_123"
    payload = {"schemaChange": "true"}

    with pytest.raises(HttpError) as excinfo:
        schedule_update_connection_schema(request, connection_id, payload)

    assert excinfo.value.status_code == 400
    assert str(excinfo.value) == "create an airbyte workspace first"


def test_schedule_update_connection_schema_workspace_success(orguser):
    """Tests schedule_update_connection_schema success"""

    orguser.org.airbyte_workspace_id = "workspace_123"
    request = mock_request(orguser)

    connection_id = "connection_123"
    payload = {"schemaChange": "true"}

    with patch(
        "ddpui.ddpairbyte.airbytehelpers.schedule_update_connection_schema"
    ) as schedule_update_connection_schema_mock:
        schedule_update_connection_schema_mock.return_value = (None, None)

        response = schedule_update_connection_schema(request, connection_id, payload)

    schedule_update_connection_schema_mock.assert_called_once_with(
        request.orguser, connection_id, payload
    )

    assert response == {"success": 1}
