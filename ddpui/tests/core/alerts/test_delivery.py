"""Tests for ddpui.core.alerts.delivery — SES + Slack delivery shape + summarize."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest
from django.contrib.auth.models import User
from types import SimpleNamespace

from ddpui.core.alerts import delivery
from ddpui.core.notifications.triggers import alert as alert_trigger
from ddpui.models.org import Org
from ddpui.models.org_user import OrgUser, OrgUserGroup, OrgUserGroupMember
from ddpui.models.role_based_access import Role
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


# ── _deliver_email (moved to triggers/alert.py) ────────────────────────────


def test_deliver_email_success_calls_send_html_message(monkeypatch):
    """_deliver_email must go through the HTML sender so alerts get the shared
    Dalgo shell (v1.1). Plain-text and HTML bodies are passed through separately.
    """
    called: dict = {}

    def fake_send(to, subject, plain, html_):  # noqa: ANN001
        called["to"] = to
        called["subject"] = subject
        called["plain"] = plain
        called["html"] = html_
        return {"MessageId": "abc"}

    monkeypatch.setattr(alert_trigger.awsses, "send_html_message", fake_send)

    out = alert_trigger._deliver_email(
        to_email="priya@example.com",
        subject="Alert",
        plain_body="hello",
        html_body="<p>hello</p>",
    )
    assert out["channel"] == "email"
    assert out["target"] == "priya@example.com"
    assert out["status"] == "sent"
    assert out["error_reason"] is None
    assert "sent_at" in out
    assert called["to"] == "priya@example.com"
    assert called["plain"] == "hello"
    assert called["html"] == "<p>hello</p>"


def test_deliver_email_failure_returns_failed_dict(monkeypatch):
    def fake_send(to, subject, plain, html_):  # noqa: ANN001
        raise RuntimeError("SES denied")

    monkeypatch.setattr(alert_trigger.awsses, "send_html_message", fake_send)

    out = alert_trigger._deliver_email(
        to_email="x@y.com", subject="s", plain_body="b", html_body="<p>b</p>"
    )
    assert out["status"] == "failed"
    assert "SES denied" in out["error_reason"]
    assert out["target"] == "x@y.com"


# ── deliver_slack ──────────────────────────────────────────────────────────


def test_deliver_slack_2xx_marks_sent(monkeypatch):
    class _Resp:
        status_code = 200
        text = "ok"
        reason = "OK"

    monkeypatch.setattr(delivery.requests, "post", lambda *a, **kw: _Resp())

    out = delivery.deliver_slack(webhook_url="https://hooks.slack.com/x", body="hello")
    assert out["status"] == "sent"
    assert out["http_status"] == 200
    assert out["target"] == delivery.SLACK_TARGET


def test_deliver_slack_non_2xx_marks_failed_with_body(monkeypatch):
    class _Resp:
        status_code = 500
        text = "internal_error"
        reason = "Server Error"

    monkeypatch.setattr(delivery.requests, "post", lambda *a, **kw: _Resp())

    out = delivery.deliver_slack(webhook_url="https://hooks.slack.com/x", body="hello")
    assert out["status"] == "failed"
    assert out["http_status"] == 500
    assert "internal_error" in out["error_reason"]


def test_deliver_slack_network_error_marks_failed(monkeypatch):
    import requests

    def boom(*a, **kw):  # noqa: ANN001
        raise requests.ConnectionError("DNS")

    monkeypatch.setattr(delivery.requests, "post", boom)

    out = delivery.deliver_slack(webhook_url="https://hooks.slack.com/x", body="hello")
    assert out["status"] == "failed"
    assert out["http_status"] == 0
    assert "DNS" in out["error_reason"]


# ── deliver_all wraps email but keeps Slack raw ───────────────────────────


def test_deliver_all_wraps_email_body_and_keeps_slack_raw(monkeypatch):
    """v1.1 contract:
    - Email delivery goes through render_alert_email → send_html_message.
    - Slack delivery uses the raw user body (no HTML shell).
    """
    from types import SimpleNamespace

    alert = SimpleNamespace(
        id=7,
        name="High errors",
        org_id=1,
        delivery_channels=["email", "slack"],
        recipients=[{"type": "external", "email": "ops@example.com"}],
        slack_webhook_url="https://hooks.slack.com/x",
    )

    ses_call: dict = {}

    def fake_send_html(to, subject, plain, html_):  # noqa: ANN001
        ses_call["to"] = to
        ses_call["subject"] = subject
        ses_call["plain"] = plain
        ses_call["html"] = html_
        return {"MessageId": "ok"}

    # The email fan-out now lives in the notifications trigger. Patch there.
    monkeypatch.setattr(alert_trigger.awsses, "send_html_message", fake_send_html)

    slack_call: dict = {}

    class _Resp:
        status_code = 200
        text = "ok"
        reason = "OK"

    def fake_post(url, json, timeout):  # noqa: ANN001, A002
        slack_call["url"] = url
        slack_call["payload"] = json
        return _Resp()

    monkeypatch.setattr(delivery.requests, "post", fake_post)

    deliveries = delivery.deliver_all(
        alert, subject="[Dalgo alert] High errors", body="Current value 42 crossed threshold."
    )

    # Email side — recipient got the shell-wrapped HTML body
    assert ses_call["to"] == "ops@example.com"
    assert "Dalgo" in ses_call["html"]  # shell wordmark present
    assert "#00897B" in ses_call["html"]  # shell teal present
    assert "Alert fired: High errors" in ses_call["html"]
    assert "Current value 42 crossed threshold." in ses_call["html"]

    # Slack side — raw body only, NOT the HTML shell
    assert slack_call["payload"] == {"text": "Current value 42 crossed threshold."}
    assert "<html>" not in slack_call["payload"]["text"]
    assert "Dalgo" not in slack_call["payload"]["text"]

    assert len(deliveries) == 2
    assert {d["channel"] for d in deliveries} == {"email", "slack"}


# ── summarize ─────────────────────────────────────────────────────────────


def test_summarize_classifies_mixed_outcomes():
    assert delivery.summarize([]) == "not_attempted"
    assert delivery.summarize([{"status": "sent"}, {"status": "sent"}]) == "success"
    assert delivery.summarize([{"status": "sent"}, {"status": "failed"}]) == "partial"
    assert delivery.summarize([{"status": "failed"}]) == "failed"


# ── notify_alert_recipients — group expansion + deduplication ─────────────


@pytest.fixture
def delivery_org():
    o = Org.objects.create(
        name="Delivery Test Org", slug="delivery-test", airbyte_workspace_id="ws-d"
    )
    yield o
    o.delete()


def _make_orguser(org, username, email):
    user = User.objects.create(username=username, email=email)
    role = Role.objects.filter(slug="analyst").first()
    ou = OrgUser.objects.create(user=user, org=org, new_role=role)
    return ou, user


def _fake_alert(org, recipients):
    return SimpleNamespace(id=99, name="Test", org_id=org.id, recipients=recipients)


def _patch_ses(monkeypatch):
    sent = []
    monkeypatch.setattr(
        alert_trigger.awsses, "send_html_message", lambda to, s, p, h: sent.append(to)
    )
    return sent


def test_group_recipient_expands_to_active_members(seed_db, monkeypatch, delivery_org):
    """A user_group recipient generates one delivery per active member."""
    ou1, u1 = _make_orguser(delivery_org, "gd_u1", "gd1@example.com")
    ou2, u2 = _make_orguser(delivery_org, "gd_u2", "gd2@example.com")
    group = OrgUserGroup.objects.create(name="G1", org=delivery_org, created_by=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=ou2)

    sent = _patch_ses(monkeypatch)
    alert = _fake_alert(delivery_org, [{"type": "user_group", "user_group_id": group.id}])
    deliveries = alert_trigger.notify_alert_recipients(alert, subject="s", body="b")

    assert sorted(sent) == ["gd1@example.com", "gd2@example.com"]
    assert len(deliveries) == 2
    assert all(d["status"] == "sent" for d in deliveries)

    OrgUserGroupMember.objects.filter(group=group).delete()
    group.delete()
    ou1.delete()
    ou2.delete()
    u1.delete()
    u2.delete()


def test_pending_members_skipped(seed_db, monkeypatch, delivery_org):
    """Group members whose orguser is None (pending invite) receive no email."""
    ou1, u1 = _make_orguser(delivery_org, "pd_u1", "pd1@example.com")
    group = OrgUserGroup.objects.create(name="G2", org=delivery_org, created_by=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=None)  # pending

    sent = _patch_ses(monkeypatch)
    alert = _fake_alert(delivery_org, [{"type": "user_group", "user_group_id": group.id}])
    deliveries = alert_trigger.notify_alert_recipients(alert, subject="s", body="b")

    assert sent == ["pd1@example.com"]
    assert len(deliveries) == 1

    OrgUserGroupMember.objects.filter(group=group).delete()
    group.delete()
    ou1.delete()
    u1.delete()


def test_deduplication_orguser_and_group(seed_db, monkeypatch, delivery_org):
    """A user who is both a named orguser recipient and in a group gets exactly one email."""
    ou1, u1 = _make_orguser(delivery_org, "dd_u1", "dd1@example.com")
    ou2, u2 = _make_orguser(delivery_org, "dd_u2", "dd2@example.com")
    group = OrgUserGroup.objects.create(name="G3", org=delivery_org, created_by=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=ou1)  # ou1 also named directly
    OrgUserGroupMember.objects.create(group=group, orguser=ou2)

    sent = _patch_ses(monkeypatch)
    alert = _fake_alert(
        delivery_org,
        [
            {"type": "orguser", "orguser_id": ou1.id},  # named directly
            {"type": "user_group", "user_group_id": group.id},  # group also contains ou1
        ],
    )
    deliveries = alert_trigger.notify_alert_recipients(alert, subject="s", body="b")

    # ou1 appears via both paths — should receive only one email
    assert sent.count("dd1@example.com") == 1
    assert sent.count("dd2@example.com") == 1
    assert len(deliveries) == 2

    OrgUserGroupMember.objects.filter(group=group).delete()
    group.delete()
    ou1.delete()
    ou2.delete()
    u1.delete()
    u2.delete()


def test_empty_group_no_deliveries(seed_db, monkeypatch, delivery_org):
    """A group with 0 active members produces 0 deliveries and no error."""
    ou1, u1 = _make_orguser(delivery_org, "eg_u1", "eg1@example.com")
    group = OrgUserGroup.objects.create(name="G4", org=delivery_org, created_by=ou1)

    sent = _patch_ses(monkeypatch)
    alert = _fake_alert(delivery_org, [{"type": "user_group", "user_group_id": group.id}])
    deliveries = alert_trigger.notify_alert_recipients(alert, subject="s", body="b")

    assert sent == []
    assert deliveries == []

    group.delete()
    ou1.delete()
    u1.delete()


def test_existing_types_unchanged_alongside_group(seed_db, monkeypatch, delivery_org):
    """orguser and external recipients continue to work alongside user_group."""
    ou1, u1 = _make_orguser(delivery_org, "et_u1", "et1@example.com")
    ou2, u2 = _make_orguser(delivery_org, "et_u2", "et2@example.com")
    group = OrgUserGroup.objects.create(name="G5", org=delivery_org, created_by=ou1)
    OrgUserGroupMember.objects.create(group=group, orguser=ou2)

    sent = _patch_ses(monkeypatch)
    alert = _fake_alert(
        delivery_org,
        [
            {"type": "orguser", "orguser_id": ou1.id},
            {"type": "external", "email": "ext@partner.org"},
            {"type": "user_group", "user_group_id": group.id},
        ],
    )
    deliveries = alert_trigger.notify_alert_recipients(alert, subject="s", body="b")

    assert sorted(sent) == ["et1@example.com", "et2@example.com", "ext@partner.org"]
    assert len(deliveries) == 3

    OrgUserGroupMember.objects.filter(group=group).delete()
    group.delete()
    ou1.delete()
    ou2.delete()
    u1.delete()
    u2.delete()
