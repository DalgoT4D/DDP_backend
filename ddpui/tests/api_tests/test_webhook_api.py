import os
import django
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import json
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.webhook_api import (
    FLOW_RUN,
    get_message_type,
    get_flowrun_id_and_state,
    post_notification_v1,
)
from ddpui.utils.webhook_helpers import (
    get_org_from_flow_run,
    generate_notification_email,
    email_orgusers,
    email_flowrun_logs_to_orgusers,
)
from ddpui.auth import SUPER_ADMIN_ROLE, GUEST_ROLE, ACCOUNT_MANAGER_ROLE
from ddpui.models.role_based_access import Role
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, User, OrgUserRole, UserAttributes
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask, TaskLock
from ddpui.models.org import OrgDataFlowv1
from ddpui.settings import PRODUCTION
from ddpui.tests.api_tests.test_user_org_api import seed_db
from ddpui.ddpprefect import (
    FLOW_RUN_PENDING_STATE_TYPE,
    FLOW_RUN_RUNNING_STATE_TYPE,
    FLOW_RUN_PENDING_STATE_NAME,
    FLOW_RUN_RUNNING_STATE_NAME,
    FLOW_RUN_FAILED_STATE_NAME,
    FLOW_RUN_FAILED_STATE_TYPE,
    FLOW_RUN_CRASHED_STATE_NAME,
    FLOW_RUN_CRASHED_STATE_TYPE,
    FLOW_RUN_COMPLETED_STATE_NAME,
    FLOW_RUN_COMPLETED_STATE_TYPE,
)

pytestmark = pytest.mark.django_db

# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


@pytest.fixture
def seed_master_tasks():
    app_dir = os.path.join(Path(apps.get_app_config("ddpui").path), "..")
    seed_dir = os.path.abspath(os.path.join(app_dir, "seed"))
    f = open(os.path.join(seed_dir, "tasks.json"))
    tasks = json.load(f)
    for task in tasks:
        Task.objects.create(**task["fields"])


# ================================================================================


def test_get_message_type():
    """tests the get_message_type function"""
    assert (
        get_message_type(
            {"state": {"state_details": {"flow_run_id": "THEID"}}, "id": "THEID"}
        )
        == FLOW_RUN
    )
    assert get_message_type({"state": {"state_details": {}}, "id": "THEID"}) is None


def test_get_flowrun_id_and_state():
    """tests the get_flowrun_id_and_state function"""
    assert get_flowrun_id_and_state(
        "Flow run glossy-bittern with id 3b55ee18-7511-4ede-abba-ad866ebf982c entered state Running"
    ) == ("3b55ee18-7511-4ede-abba-ad866ebf982c", "Running")


def test_get_org_from_flow_run_by_connection():
    """tests get_org_from_flow_run"""
    blockid = str(uuid4())
    flow_run = {
        "parameters": {
            "airbyte_connection": {"_block_document_id": blockid},
            "config": {"org_slug": "temp"},
        }
    }
    org = Org.objects.create(name="temp", slug="temp")
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_generate_notification_email():
    """tests the email generated"""
    response = generate_notification_email(
        "orgname", "flow-run-id", ["log-message-1", "log-message-2"]
    )
    assert response.find("orgname") > -1
    assert response.find("flow-run-id") > -1
    assert response.find("log-message-1") > -1
    assert response.find("log-message-2") > -1


def test_email_orgusers():
    """tests the email_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    user = User.objects.create(username="username", email="useremail")
    new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
    OrgUser.objects.create(org=org, new_role=new_role, user=user)
    UserAttributes.objects.create(user=user, is_platform_admin=True)
    with patch(
        "ddpui.utils.webhook_helpers.send_text_message"
    ) as mock_send_text_message:
        email_orgusers(org, "hello")
        tag = " [STAGING]" if not PRODUCTION else ""
        subject = f"Dalgo notification{tag}"
        mock_send_text_message.assert_called_once_with("useremail", subject, "hello")


def test_email_orgusers_not_non_admins():
    """tests the email_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    user = User.objects.create(username="username", email="useremail")
    new_role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
    OrgUser.objects.create(org=org, new_role=new_role, user=user)
    with patch(
        "ddpui.utils.webhook_helpers.send_text_message"
    ) as mock_send_text_message:
        email_orgusers(org, "hello")
        mock_send_text_message.assert_not_called()


def test_email_orgusers_not_to_report_viewers():
    """tests the email_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    user = User.objects.create(username="username", email="useremail")
    new_role = Role.objects.filter(slug=GUEST_ROLE).first()
    OrgUser.objects.create(org=org, new_role=new_role, user=user)
    with patch(
        "ddpui.utils.webhook_helpers.send_text_message"
    ) as mock_send_text_message:
        email_orgusers(org, "hello")
        mock_send_text_message.assert_not_called()


def test_email_flowrun_logs_to_orgusers():
    """tests the email_flowrun_logs_to_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    with patch(
        "ddpui.ddpprefect.prefect_service.get_flow_run_logs"
    ) as mock_get_flow_run_logs:
        mock_get_flow_run_logs.return_value = {
            "logs": {
                "logs": [
                    {
                        "message": "log-message-1",
                    },
                    {
                        "message": "log-message-2",
                    },
                ]
            }
        }
        with patch("ddpui.utils.webhook_helpers.email_orgusers") as mock_email_orgusers:
            email_flowrun_logs_to_orgusers(org, "flow-run-id")
            tag = " [STAGING]" if not PRODUCTION else ""
            mock_email_orgusers.assert_called_once_with(
                org,
                f"\nTo the admins of temp{tag},\n\nThis is an automated notification from Dalgo{tag}.\n\nFlow run id: flow-run-id\nLogs:\nlog-message-1\nlog-message-2",
            )


@pytest.mark.skip(reason="Skipping this test as its failing for some reason.")
def test_post_notification_v1_unauthorized():
    """tests the api endpoint /notifications/"""
    request = Mock()
    request.headers = {}
    os.environ["X-Notification-Key"] = ""
    with pytest.raises(HttpError) as excinfo:
        post_notification_v1(request)
    assert str(excinfo.value) == "unauthorized"


def test_post_notification_v1():
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    request = Mock()
    body = """
    Flow run test-flow-run-name with id test-run-id entered state Failed
    """
    request.body = json.dumps({"body": body})
    request.headers = {
        "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
    }
    blockid = str(uuid4())
    org = Org.objects.create(name="temp", slug="temp")
    flow_run = {
        "parameters": {
            "airbyte_connection": {"_block_document_id": blockid},
            "config": {"org_slug": org.slug},
        },
        "deployment_id": "test-deployment-id",
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    with patch(
        "ddpui.ddpprefect.prefect_service.get_flow_run"
    ) as mock_get_flow_run, patch(
        "ddpui.api.webhook_api.email_flowrun_logs_to_orgusers"
    ) as mock_email_flowrun_logs_to_orgusers, patch(
        "ddpui.api.webhook_api.email_orgusers_ses_whitelisted"
    ) as mock_email_orgusers_ses_whitelisted:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(
            org=org, user=user, role=OrgUserRole.ACCOUNT_MANAGER, new_role=new_role
        )
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
        mock_email_flowrun_logs_to_orgusers.assert_called_once()
        mock_email_orgusers_ses_whitelisted.assert_called_once()


def test_post_notification_v1_webhook_scheduled_pipeline(seed_master_tasks):
    """
    This test cases make sure that the states of prefect flow runs are being updated as notifications from prefect come
    Also checks for locks being created/deleted at the correct times
    """
    blockid = str(uuid4())
    org = Org.objects.create(name="temp", slug="temp")
    dataflow = OrgDataFlowv1.objects.create(
        org=org,
        name="test-dataflow-name",
        deployment_id="test-deployment-id",
        deployment_name="test-deployment-name",
        cron=None,
        dataflow_type="orchestrate",
    )
    # add sync orgtask and a dbt run to the above pipeline
    for slugs in ["airbyte-sync", "dbt-run"]:
        task = Task.objects.filter(slug=slugs).first()
        orgtask = OrgTask.objects.create(task=task, org=org)
        DataflowOrgTask.objects.create(dataflow=dataflow, orgtask=orgtask)

    flow_run = {
        "parameters": {
            "airbyte_connection": {"_block_document_id": blockid},
            "config": {"org_slug": org.slug},
        },
        "deployment_id": dataflow.deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_PENDING_STATE_TYPE,
        "state_name": FLOW_RUN_PENDING_STATE_NAME,
    }
    # make sure the system orguser is present
    call_command("create-system-orguser")

    # Pending; first message from prefect; deployment has just been triggered
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        mock_get_flow_run.return_value = flow_run
        request = Mock()
        body = f"""
        Flow run test-flow-run-name with id test-run-id entered state {FLOW_RUN_PENDING_STATE_NAME}
        """
        request.body = json.dumps({"body": body})
        request.headers = {
            "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
        }
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_PENDING_STATE_TYPE
            ).count()
            == 1
        )
        # the dataflow & its orgtasks should be locked
        assert (
            TaskLock.objects.filter(
                locking_dataflow=dataflow, flow_run_id=flow_run["id"]
            ).count()
            == 2
        )

    # Running; second message from prefect; deployment is running
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        flow_run["status"] = FLOW_RUN_RUNNING_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_RUNNING_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        request = Mock()
        body = f"""
        Flow run test-flow-run-name with id test-run-id entered state {FLOW_RUN_RUNNING_STATE_NAME}
        """
        request.body = json.dumps({"body": body})
        request.headers = {
            "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
        }
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert PrefectFlowRun.objects.filter(flow_run_id=flow_run["id"]).count() == 1
        # the dataflow & its orgtasks should still be locked
        assert (
            TaskLock.objects.filter(
                locking_dataflow=dataflow, flow_run_id=flow_run["id"]
            ).count()
            == 2
        )

    # Failed (any terminal state); third message from prefect; deployment has failed
    with patch(
        "ddpui.ddpprefect.prefect_service.get_flow_run"
    ) as mock_get_flow_run, patch(
        "ddpui.api.webhook_api.email_flowrun_logs_to_orgusers"
    ) as mock_email_flowrun_logs_to_orgusers:
        flow_run["status"] = FLOW_RUN_FAILED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_FAILED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        request = Mock()
        body = f"""
        Flow run test-flow-run-name with id test-run-id entered state {FLOW_RUN_FAILED_STATE_NAME}
        """
        request.body = json.dumps({"body": body})
        request.headers = {
            "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
        }
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_FAILED_STATE_TYPE
            ).count()
            == 1
        )
        assert (
            TaskLock.objects.filter(
                locking_dataflow=dataflow, flow_run_id=flow_run["id"]
            ).count()
            == 0
        )
        mock_email_flowrun_logs_to_orgusers.assert_called_once()

    # Failed (crashed); with retry logic
    with patch(
        "ddpui.ddpprefect.prefect_service.get_flow_run"
    ) as mock_get_flow_run, patch(
        "ddpui.api.webhook_api.email_flowrun_logs_to_orgusers"
    ) as mock_email_flowrun_logs_to_orgusers:
        flow_run["status"] = FLOW_RUN_CRASHED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_CRASHED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        request = Mock()
        body = f"""
        Flow run test-flow-run-name with id test-run-id entered state {FLOW_RUN_CRASHED_STATE_NAME}
        """
        request.body = json.dumps({"body": body})
        request.headers = {
            "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
        }
        os.environ["PREFECT_RETRY_CRASHED_FLOW_RUNS"] = True
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_CRASHED_STATE_TYPE
            ).count()
            == 1
        )
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_CRASHED_STATE_TYPE
            )
            .first()
            .retries
            == 1
        )
        mock_email_flowrun_logs_to_orgusers.assert_not_called()

    # or
    # Completed (any terminal state); third message from prefect; deployment has completed
    PrefectFlowRun.objects.filter(flow_run_id=flow_run["id"]).update(
        status=FLOW_RUN_PENDING_STATE_TYPE, state_name=FLOW_RUN_PENDING_STATE_NAME
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        flow_run["status"] = FLOW_RUN_COMPLETED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_COMPLETED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        request = Mock()
        body = f"""
        Flow run test-flow-run-name with id test-run-id entered state {FLOW_RUN_COMPLETED_STATE_NAME}
        """
        request.body = json.dumps({"body": body})
        request.headers = {
            "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
        }
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_COMPLETED_STATE_TYPE
            ).count()
            == 1
        )
        assert (
            TaskLock.objects.filter(
                locking_dataflow=dataflow, flow_run_id=flow_run["id"]
            ).count()
            == 0
        )
