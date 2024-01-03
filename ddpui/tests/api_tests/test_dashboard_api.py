import os
import django

from unittest.mock import Mock, patch
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.models.org_user import User, OrgUser
from ddpui.models.orgjobs import OrgDataFlow, DataflowBlock, BlockLock
from ddpui.models.tasks import Task, DataflowOrgTask, TaskLock, OrgTask, OrgDataFlowv1
from ddpui.api.dashboard_api import get_dashboard, get_dashboard_v1

pytestmark = pytest.mark.django_db


def test_get_dashboard():
    user = User.objects.create(email="email", username="username")
    org = Org.objects.create(name="org", slug="org")
    orguser = OrgUser.objects.create(user=user, org=org)
    request = Mock()
    request.orguser = orguser

    opb = OrgPrefectBlock.objects.create(
        org=org, block_type="block-type", block_id="block-id", block_name="block-name"
    )
    odf = OrgDataFlow.objects.create(
        org=org,
        cron="1",
        name="flow-name",
        deployment_id="deployment-id",
        deployment_name="deployment-name",
    )
    DataflowBlock.objects.create(dataflow=odf, opb=opb)
    BlockLock.objects.create(opb=opb, locked_by=orguser)

    with patch(
        "ddpui.api.dashboard_api.prefect_service.get_flow_runs_by_deployment_id"
    ) as mock_get_flow_runs_by_deployment_id:
        mock_get_flow_runs_by_deployment_id.return_value = []
        result = get_dashboard(request)

    assert result[0]["name"] == "flow-name"
    assert result[0]["deploymentId"] == "deployment-id"
    assert result[0]["cron"] == "1"
    assert result[0]["deploymentName"] == "deployment-name"
    assert result[0]["runs"] == []
    assert result[0]["lock"]["lockedBy"] == "email"


def test_get_dashboard_v1():
    user = User.objects.create(email="email", username="username")
    org = Org.objects.create(name="org", slug="org")
    orguser = OrgUser.objects.create(user=user, org=org)
    request = Mock()
    request.orguser = orguser

    task = Task.objects.create(
        type="dbt", slug="dbt-clean", label="DBT clean", command="clean"
    )
    orgtask = OrgTask.objects.create(org=org, task=task)
    odf = OrgDataFlowv1.objects.create(
        org=org,
        cron="1",
        name="flow-name",
        deployment_id="deployment-id",
        deployment_name="deployment-name",
        dataflow_type="orchestrate",
    )
    DataflowOrgTask.objects.create(dataflow=odf, orgtask=orgtask)
    TaskLock.objects.create(orgtask=orgtask, locked_by=orguser)

    with patch(
        "ddpui.api.dashboard_api.prefect_service.get_flow_runs_by_deployment_id"
    ) as mock_get_flow_runs_by_deployment_id:
        mock_get_flow_runs_by_deployment_id.return_value = []
        result = get_dashboard_v1(request)

    assert result[0]["name"] == "flow-name"
    assert result[0]["deploymentId"] == "deployment-id"
    assert result[0]["cron"] == "1"
    assert result[0]["deploymentName"] == "deployment-name"
    assert result[0]["runs"] == []
    assert result[0]["lock"]["lockedBy"] == "email"
