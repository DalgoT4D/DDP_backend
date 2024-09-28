from unittest.mock import patch, Mock
import pytest
from ddpui.models.org import Org, OrgPrefectBlockv1, OrgDataFlowv1
from ddpui.models.tasks import Task, DataflowOrgTask, OrgTask
from ddpui.utils.deploymentblocks import trigger_reset_and_sync_workflow
from ddpui.utils.constants import TASK_AIRBYTERESET, TASK_AIRBYTESYNC
from ddpui.ddpprefect import AIRBYTESERVER

pytestmark = pytest.mark.django_db


@pytest.fixture
def synctask():
    """airbyte sync task"""
    return Task.objects.create(slug=TASK_AIRBYTESYNC, label="sync", type="airbyte")


@pytest.fixture
def resettask():
    """airbyte reset task"""
    return Task.objects.create(slug=TASK_AIRBYTERESET, label="reset", type="airbyte")


def test_trigger_reset_and_sync_workflow_0():
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    result, error = trigger_reset_and_sync_workflow(org, connection_id)
    assert result is None
    assert error == "sync OrgTask not found"


def test_trigger_reset_and_sync_workflow_1(synctask):
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    OrgTask.objects.create(org=org, connection_id=connection_id, task=synctask)
    result, error = trigger_reset_and_sync_workflow(org, connection_id)
    assert result is None
    assert error == "sync dataflow not found"


def test_trigger_reset_and_sync_workflow_2(synctask):
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    sync_orgtask = OrgTask.objects.create(org=org, connection_id=connection_id, task=synctask)
    dataflow = OrgDataFlowv1.objects.create(org=org, name="dataflow-name", dataflow_type="manual")
    DataflowOrgTask.objects.create(
        orgtask=sync_orgtask,
        dataflow=dataflow,
    )
    result, error = trigger_reset_and_sync_workflow(org, connection_id)
    assert result is None
    assert error == "reset OrgTask not found"


def test_trigger_reset_and_sync_workflow_3(synctask, resettask):
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    sync_orgtask = OrgTask.objects.create(org=org, connection_id=connection_id, task=synctask)
    syncdataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-sync",
        dataflow_type="manual",
        deployment_id="sync-deployment-id",
    )
    DataflowOrgTask.objects.create(
        orgtask=sync_orgtask,
        dataflow=syncdataflow,
    )
    OrgTask.objects.create(org=org, connection_id=connection_id, task=resettask)
    reset_conn_dataflow = OrgDataFlowv1.objects.create(
        org=org, name="dataflow-reset", dataflow_type="manual", deployment_id=None
    )
    syncdataflow.reset_conn_dataflow = reset_conn_dataflow
    syncdataflow.save()

    result, error = trigger_reset_and_sync_workflow(org, connection_id)
    assert result is None
    assert error == "airbyte server block not found"


def test_trigger_reset_and_sync_workflow_4(synctask, resettask):
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    sync_orgtask = OrgTask.objects.create(org=org, connection_id=connection_id, task=synctask)
    syncdataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-sync",
        dataflow_type="manual",
        deployment_id="sync-deployment-id",
    )
    DataflowOrgTask.objects.create(
        orgtask=sync_orgtask,
        dataflow=syncdataflow,
    )
    OrgTask.objects.create(org=org, connection_id=connection_id, task=resettask)
    reset_conn_dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-reset",
        dataflow_type="manual",
        deployment_id="reset-deployment-id",
    )
    syncdataflow.reset_conn_dataflow = reset_conn_dataflow
    syncdataflow.save()

    result, error = trigger_reset_and_sync_workflow(org, connection_id)
    assert result is None
    assert error == "airbyte server block not found"


def test_trigger_reset_and_sync_workflow_5(synctask, resettask):
    """tests trigger_reset_and_sync_workflow"""
    org = Org.objects.create(name="name", slug="slug")
    connection_id = "fake-connection-id"
    sync_orgtask = OrgTask.objects.create(org=org, connection_id=connection_id, task=synctask)
    syncdataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-sync",
        dataflow_type="manual",
        deployment_id="sync-deployment-id",
    )
    DataflowOrgTask.objects.create(
        orgtask=sync_orgtask,
        dataflow=syncdataflow,
    )
    OrgTask.objects.create(org=org, connection_id=connection_id, task=resettask)
    reset_conn_dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="dataflow-reset",
        dataflow_type="manual",
        deployment_id="reset-deployment-id",
    )
    syncdataflow.reset_conn_dataflow = reset_conn_dataflow
    syncdataflow.save()

    OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=AIRBYTESERVER,
        block_id="fake-serverblock-id",
        block_name="fake-serverblock-name",
    )

    with patch(
        "ddpui.ddpprefect.prefect_service.create_deployment_flow_run"
    ) as mock_create_deployment_flow_run:
        mock_create_deployment_flow_run.side_effect = 1
        result, error = trigger_reset_and_sync_workflow(org, connection_id)
        assert result is None
        assert error == "failed to trigger Prefect flow run for reset"
