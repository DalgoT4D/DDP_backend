import pytest
from ddpui.models.notifications import Notification

pytestmark = pytest.mark.django_db


def test_notification_defaults_preserve_existing_behavior():
    """target_org_ids/send_in_app/send_email are additive -- a notification created
    without them (the management command, the existing broken HTTP route) keeps
    today's behavior: audience unknown (legacy), both channels on."""
    notification = Notification.objects.create(
        author="test_author",
        message="test_message",
        email_subject="test_subject",
    )
    assert notification.target_org_ids is None
    assert notification.send_in_app is True
    assert notification.send_email is True
