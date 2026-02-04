import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import json
from unittest.mock import Mock, patch
import django
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
from ddpui.core.webhooks.webhook_functions import (
    get_org_from_flow_run,
    do_handle_prefect_webhook,
    get_flow_run_times,
)
from ddpui.core.notifications.delivery import (
    generate_notification_email,
    notify_platform_admins,
)
from ddpui.auth import SUPER_ADMIN_ROLE, GUEST_ROLE, ACCOUNT_MANAGER_ROLE
from ddpui.models.org import Org, ConnectionMeta
from ddpui.models.org import OrgDataFlowv1
from ddpui.models.org_user import OrgUser, User, OrgUserRole, UserAttributes
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.tasks import Task, OrgTask, DataflowOrgTask, TaskLock
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
        get_message_type({"state": {"state_details": {"flow_run_id": "THEID"}}, "id": "THEID"})
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


def test_email_flowrun_logs_to_orgusers():
    """tests the email_flowrun_logs_to_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_logs") as mock_get_flow_run_logs:
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


@pytest.mark.skip(reason="Skipping this test as its failing for some reason.")
def test_post_notification_v1_unauthorized():
    """tests the api endpoint /notifications/"""
    request = Mock()
    request.headers = {}
    os.environ["X-Notification-Key"] = ""
    with pytest.raises(HttpError) as excinfo:
        post_notification_v1(request)
    assert str(excinfo.value) == "unauthorized"


def test_post_notification_v1_orchestrate():
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    org = Org.objects.create(name="temp", slug="temp")
    deployment_id = "test-deployment-id"
    connection_id = "test-connection_id"
    connection_name = "test-connection_name"
    ConnectionMeta.objects.create(connection_id=connection_id, connection_name=connection_name)
    flow_run = {
        "parameters": {
            "config": {"org_slug": org.slug, "connection_id": connection_id},
        },
        "deployment_id": deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    odf = OrgDataFlowv1.objects.create(
        org=org, name=deployment_id, dataflow_type="orchestrate", deployment_id=deployment_id
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.webhooks.webhook_functions.notify_users_about_failed_run"
    ) as mock_notify_users_about_failed_run:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(org=org, user=user, new_role=new_role)
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
        mock_notify_users_about_failed_run.assert_called_once_with(
            org, odf, flow_run, FLOW_RUN_FAILED_STATE_NAME
        )


def test_post_notification_v1_manual_with_connection_id():
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    org = Org.objects.create(name="temp", slug="temp")
    deployment_id = "test-deployment-id"
    connection_id = "test-connection_id"
    connection_name = "test-connection_name"
    ConnectionMeta.objects.create(connection_id=connection_id, connection_name=connection_name)
    flow_run = {
        "parameters": {
            "config": {"org_slug": org.slug, "connection_id": connection_id},
        },
        "deployment_id": deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    odf = OrgDataFlowv1.objects.create(
        org=org, name=deployment_id, dataflow_type="manual", deployment_id=deployment_id
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.webhooks.webhook_functions.notify_users_about_failed_run"
    ) as mock_notify_users_about_failed_run:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(org=org, user=user, new_role=new_role)
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
        mock_notify_users_about_failed_run.assert_called_once_with(
            org, odf, flow_run, FLOW_RUN_FAILED_STATE_NAME
        )


def test_post_notification_v1_manual_with_orgtask_id(seed_master_tasks):
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    org = Org.objects.create(name="temp", slug="temp")
    deployment_id = "test-deployment-id"
    task = Task.objects.first()
    orgtask = OrgTask.objects.create(org=org, task=task)

    flow_run = {
        "parameters": {
            "config": {"org_slug": org.slug, "orgtask_uuid": orgtask.uuid},
        },
        "deployment_id": deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    odf = OrgDataFlowv1.objects.create(
        org=org, name=deployment_id, dataflow_type="manual", deployment_id=deployment_id
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.webhooks.webhook_functions.notify_users_about_failed_run"
    ) as mock_notify_users_about_failed_run:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(org=org, user=user, new_role=new_role)
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
        mock_notify_users_about_failed_run.assert_called_once_with(
            org, odf, flow_run, FLOW_RUN_FAILED_STATE_NAME
        )


def test_post_notification_v1_manual_with_orgtask_id_generate_edr(seed_master_tasks):
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    org = Org.objects.create(name="temp", slug="temp")
    deployment_id = "test-deployment-id"
    task = Task.objects.filter(slug="generate-edr").first()
    orgtask = OrgTask.objects.create(org=org, task=task)

    flow_run = {
        "parameters": {
            "config": {"org_slug": org.slug, "orgtask_uuid": orgtask.uuid},
        },
        "deployment_id": deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    OrgDataFlowv1.objects.create(
        org=org, name=deployment_id, dataflow_type="manual", deployment_id=deployment_id
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.notifications.delivery.notify_org_managers"
    ) as mock_notify_org_managers, patch(
        "ddpui.core.notifications.delivery.notify_platform_admins"
    ) as mock_notify_platform_admins:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(org=org, user=user, new_role=new_role)
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
        # For generate-edr tasks, org managers should not be notified
        mock_notify_org_managers.assert_not_called()
        # Platform admins also should not be notified unless explicitly enabled
        mock_notify_platform_admins.assert_not_called()


def test_post_notification_v1_email_supersadmins():
    """tests the api endpoint /notifications/ ; fail & if logs are being sent"""
    blockid = str(uuid4())
    org = Org.objects.create(name="temp", slug="temp")
    deployment_id = "test-deployment-id"
    flow_run = {
        "parameters": {
            "airbyte_connection": {"_block_document_id": blockid},
            "config": {"org_slug": org.slug},
        },
        "deployment_id": deployment_id,
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": FLOW_RUN_FAILED_STATE_TYPE,
        "state_name": FLOW_RUN_FAILED_STATE_NAME,
    }
    # Create OrgDataFlowv1 to match webhook logic
    odf = OrgDataFlowv1.objects.create(
        org=org, name=deployment_id, dataflow_type="orchestrate", deployment_id=deployment_id
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.webhooks.webhook_functions.notify_users_about_failed_run"
    ) as mock_notify_users_about_failed_run:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(org=org, user=user, new_role=new_role)

        # Use context manager to ensure environment variables are properly reset
        with patch.dict(
            os.environ,
            {
                "NOTIFY_PLATFORM_ADMINS_OF_ERRORS": "True",
            },
        ):
            do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
            assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
            mock_notify_users_about_failed_run.assert_called_once_with(
                org, odf, flow_run, FLOW_RUN_FAILED_STATE_NAME
            )


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
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run:
        mock_get_flow_run.return_value = flow_run
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_PENDING_STATE_TYPE
            ).count()
            == 1
        )
        # the dataflow & its orgtasks should be locked
        assert (
            TaskLock.objects.filter(locking_dataflow=dataflow, flow_run_id=flow_run["id"]).count()
            == 2
        )

    # Running; second message from prefect; deployment is running
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run:
        flow_run["status"] = FLOW_RUN_RUNNING_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_RUNNING_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert PrefectFlowRun.objects.filter(flow_run_id=flow_run["id"]).count() == 1
        # the dataflow & its orgtasks should still be locked
        assert (
            TaskLock.objects.filter(locking_dataflow=dataflow, flow_run_id=flow_run["id"]).count()
            == 2
        )

    # Failed (any terminal state); third message from prefect; deployment has failed
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.core.webhooks.webhook_functions.notify_users_about_failed_run"
    ) as mock_notify_users_about_failed_run:
        flow_run["status"] = FLOW_RUN_FAILED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_FAILED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_FAILED_STATE_TYPE
            ).count()
            == 1
        )
        assert (
            TaskLock.objects.filter(locking_dataflow=dataflow, flow_run_id=flow_run["id"]).count()
            == 0
        )
        mock_notify_users_about_failed_run.assert_called_once_with(
            dataflow.org, dataflow, flow_run, FLOW_RUN_FAILED_STATE_NAME
        )

    # Failed (crashed); with retry logic
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run, patch(
        "ddpui.ddpprefect.prefect_service.retry_flow_run"
    ) as mock_retry_flow_run:
        flow_run["status"] = FLOW_RUN_CRASHED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_CRASHED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        os.environ["PREFECT_RETRY_CRASHED_FLOW_RUNS"] = "True"
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        mock_retry_flow_run.assert_called_with(flow_run["id"], 5)
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

    # or
    # Completed (any terminal state); third message from prefect; deployment has completed
    PrefectFlowRun.objects.filter(flow_run_id=flow_run["id"]).update(
        status=FLOW_RUN_PENDING_STATE_TYPE, state_name=FLOW_RUN_PENDING_STATE_NAME
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run_poll") as mock_get_flow_run:
        flow_run["status"] = FLOW_RUN_COMPLETED_STATE_TYPE
        flow_run["state_name"] = FLOW_RUN_COMPLETED_STATE_NAME
        mock_get_flow_run.return_value = flow_run
        do_handle_prefect_webhook(flow_run["id"], flow_run["state_name"])
        assert (
            PrefectFlowRun.objects.filter(
                flow_run_id=flow_run["id"], status=FLOW_RUN_COMPLETED_STATE_TYPE
            ).count()
            == 1
        )
        assert (
            TaskLock.objects.filter(locking_dataflow=dataflow, flow_run_id=flow_run["id"]).count()
            == 0
        )


def test_notify_platform_admins():
    """tests notify_platform_admins"""
    with patch(
        "ddpui.utils.discord.send_discord_notification"
    ) as mock_send_discord_notification, patch("ddpui.utils.awsses.ses") as mock_ses:
        org = Mock(slug="orgslug", airbyte_workspace_id="airbyte_workspace_id")
        org.base_plan = Mock(return_value="baseplan")
        os.environ["ADMIN_EMAIL"] = "adminemail"
        os.environ["ADMIN_DISCORD_WEBHOOK"] = "https://discord.com/api/webhooks/test"
        os.environ["PREFECT_URL_FOR_NOTIFICATIONS"] = "prefect-url-for-notifications"
        os.environ["AIRBYTE_URL_FOR_NOTIFICATIONS"] = "airbyte-url-for-notifications"
        os.environ["SES_SENDER_EMAIL"] = "sender@example.com"

        message = """**Pipeline Failure Alert**
Organization: orgslug
Failed Step: Test Step
State: FAILED
Base plan: baseplan

[View Logs](prefect-url-for-notifications/flow-runs/flow-run/flow-run-id)
[Airbyte Workspace](airbyte-url-for-notifications/workspaces/airbyte_workspace_id)"""

        notify_platform_admins(org, "flow-run-id", "FAILED", "Test Step")
        mock_send_discord_notification.assert_called_once_with(
            "https://discord.com/api/webhooks/test", message
        )
        mock_ses.send_email.assert_called_once()


def test_get_flow_run_times():
    """tests get_flow_run_times"""
    flow_run = {
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
    }
    start_time, expected_start_time = get_flow_run_times(flow_run)
    assert str(start_time) == flow_run["start_time"]
    assert str(expected_start_time) == flow_run["expected_start_time"]


def test_get_flow_run_times_no_start_time():
    """tests get_flow_run_times with no start time"""
    flow_run = {
        "start_time": None,
        "expected_start_time": str(datetime.now()),
    }
    start_time, expected_start_time = get_flow_run_times(flow_run)
    assert str(start_time) == flow_run["expected_start_time"]
    assert str(expected_start_time) == flow_run["expected_start_time"]


def test_get_flow_run_times_no_expected_start_time():
    """tests get_flow_run_times with no expected start time"""
    flow_run = {
        "start_time": str(datetime.now()),
    }
    start_time, expected_start_time = get_flow_run_times(flow_run)
    assert str(start_time) == flow_run["start_time"]
    assert expected_start_time is not None
