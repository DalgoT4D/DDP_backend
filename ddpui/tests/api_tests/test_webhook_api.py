import os
import django
from datetime import datetime
from uuid import uuid4
import json
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

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
from ddpui.settings import PRODUCTION
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db

# ================================================================================


def test_seed_data(seed_db):
    """a test to seed the database"""
    assert Role.objects.count() == 5
    assert RolePermission.objects.count() > 5
    assert Permission.objects.count() > 5


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
        subject = f"Prefect notification{tag}"
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
    flow_run = {
        "parameters": {
            "airbyte_connection": {"_block_document_id": blockid},
        },
        "deployment_id": "test-deployment-id",
        "id": "test-run-id",
        "name": "test-flow-run-name",
        "start_time": str(datetime.now()),
        "expected_start_time": str(datetime.now()),
        "total_run_time": 12,
        "status": "Failed",
        "state_name": "FAILED",
    }
    org = Org.objects.create(name="temp", slug="temp")
    with patch(
        "ddpui.ddpprefect.prefect_service.get_flow_run"
    ) as mock_get_flow_run, patch(
        "ddpui.utils.webhook_helpers.send_text_message"
    ) as mock_send_text_message:
        mock_get_flow_run.return_value = flow_run
        user = User.objects.create(email="email", username="username")
        new_role = Role.objects.filter(slug=SUPER_ADMIN_ROLE).first()
        OrgUser.objects.create(
            org=org, user=user, role=OrgUserRole.ACCOUNT_MANAGER, new_role=new_role
        )
        response = post_notification_v1(request)
        assert response["status"] == "ok"
        assert PrefectFlowRun.objects.filter(flow_run_id="test-run-id").count() == 1
