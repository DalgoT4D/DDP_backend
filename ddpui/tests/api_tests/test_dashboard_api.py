import os
import django

from unittest.mock import patch
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.models.org import Org
from ddpui.models.org_user import User, OrgUser
from ddpui.models.tasks import Task, DataflowOrgTask, TaskLock, OrgTask, OrgDataFlowv1, TaskType
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.api.dashboard_api import get_dashboard_v1
from ddpui.tests.api_tests.test_user_org_api import seed_db, mock_request

pytestmark = pytest.mark.django_db

# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


# ================================================================================
def test_get_dashboard_v1():
    """test the dashboard_v1 endpoint"""
    user = User.objects.create(email="email", username="username")
    org = Org.objects.create(name="org", slug="org")
    orguser = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    request = mock_request(orguser)

    task = Task.objects.create(
        type=TaskType.DBT, slug="dbt-clean", label="DBT clean", command="clean"
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
        "ddpui.ddpprefect.prefect_service.get_flow_runs_by_deployment_id"
    ) as mock_get_flow_runs_by_deployment_id:
        mock_get_flow_runs_by_deployment_id.return_value = []
        result = get_dashboard_v1(request)

    assert result[0]["name"] == "flow-name"
    assert result[0]["deploymentId"] == "deployment-id"
    assert result[0]["cron"] == "1"
    assert result[0]["deploymentName"] == "deployment-name"
    assert result[0]["runs"] == []
    assert result[0]["lock"]["lockedBy"] == "email"
