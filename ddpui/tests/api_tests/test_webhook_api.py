import os
import django
from uuid import uuid4
import json
from unittest.mock import Mock, patch
import pytest
from ninja.errors import HttpError

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.api.client.webhook_api import (
    FLOW_RUN,
    get_message_type,
    get_flowrun_id_and_state,
    post_notification,
)
from ddpui.utils.webhook_helpers import (
    get_org_from_flow_run,
    generate_notification_email,
    email_orgusers,
    email_flowrun_logs_to_orgusers,
)
from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.models.org_user import OrgUser, User, OrgUserRole

pytestmark = pytest.mark.django_db


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


def test_get_org_from_flow_run_by_blockname():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"block_name": blockname}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="12345"
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_connection():
    """tests get_org_from_flow_run"""
    blockid = str(uuid4())
    flow_run = {"parameters": {"airbyte_connection": {"_block_document_id": blockid}}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name="tempblockname", block_type="fake-type", block_id=blockid
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_airbyte_blocks():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"airbyte_blocks": [{"blockName": blockname}]}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="blockid"
    )
    response = get_org_from_flow_run(flow_run)
    assert response == org


def test_get_org_from_flow_run_by_dbt_blocks():
    """tests get_org_from_flow_run"""
    blockname = str(uuid4())
    flow_run = {"parameters": {"dbt_blocks": [{"blockName": blockname}]}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name=blockname, block_type="fake-type", block_id="blockid"
    )
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
    OrgUser.objects.create(org=org, role=OrgUserRole.ACCOUNT_MANAGER, user=user)
    with patch(
        "ddpui.utils.webhook_helpers.send_text_message"
    ) as mock_send_text_message:
        email_orgusers(org, "hello")
        mock_send_text_message.assert_called_once_with(
            "useremail", "Prefect notification", "hello"
        )


def test_email_orgusers_not_to_report_viewers():
    """tests the email_orgusers function"""
    org = Org.objects.create(name="temp", slug="temp")
    user = User.objects.create(username="username", email="useremail")
    OrgUser.objects.create(org=org, role=OrgUserRole.REPORT_VIEWER, user=user)
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
            mock_email_orgusers.assert_called_once_with(
                org,
                "\nTo the admins of temp,\n\nThis is an automated notification from Dalgo.\n\nFlow run id: flow-run-id\nLogs:\nlog-message-1\nlog-message-2",
            )


def test_post_notification_unauthorized():
    """tests the api endpoint /notifications/"""
    request = Mock()
    request.headers = {}
    os.environ["X-Notification-Key"] = ""
    with pytest.raises(HttpError) as excinfo:
        post_notification(request)
    assert str(excinfo.value) == "unauthorized"


def test_post_notification():
    """tests the api endpoint /notifications/"""
    request = Mock()
    body = """this is a message
    and another
    Flow run ID: test-flow-run-id
    more text
    """
    request.body = json.dumps({"body": body})
    request.headers = {
        "X-Notification-Key": os.getenv("PREFECT_NOTIFICATIONS_WEBHOOK_KEY")
    }
    blockid = str(uuid4())
    flow_run = {"parameters": {"airbyte_connection": {"_block_document_id": blockid}}}
    org = Org.objects.create(name="temp", slug="temp")
    OrgPrefectBlock.objects.create(
        org=org, block_name="tempblockname", block_type="fake-type", block_id=blockid
    )
    with patch("ddpui.ddpprefect.prefect_service.get_flow_run") as mock_get_flow_run:
        mock_get_flow_run.return_value = flow_run
        with patch(
            "ddpui.ddpprefect.prefect_service.get_flow_run_logs"
        ) as mock_get_flow_run_logs:
            mock_get_flow_run_logs.return_value = {"logs": []}
            user = User.objects.create(email="email", username="username")
            OrgUser.objects.create(org=org, user=user, role=OrgUserRole.ACCOUNT_MANAGER)
            response = post_notification(request)
            assert response["status"] == "ok"
