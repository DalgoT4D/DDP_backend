"""Tests for ddpui.core.alerts.delivery — SES + Slack delivery shape + summarize."""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pytest

from ddpui.core.alerts import delivery


# ── deliver_email ──────────────────────────────────────────────────────────


def test_deliver_email_success_returns_sent_dict(monkeypatch):
    called: dict = {}

    def fake_send(to, subject, body):  # noqa: ANN001
        called["to"] = to
        called["subject"] = subject
        called["body"] = body
        return {"MessageId": "abc"}

    monkeypatch.setattr(delivery.awsses, "send_text_message", fake_send)

    out = delivery.deliver_email(to_email="priya@example.com", subject="Alert", body="hello")
    assert out["channel"] == "email"
    assert out["target"] == "priya@example.com"
    assert out["status"] == "sent"
    assert out["error_reason"] is None
    assert "sent_at" in out
    assert called["to"] == "priya@example.com"


def test_deliver_email_failure_returns_failed_dict(monkeypatch):
    def fake_send(to, subject, body):  # noqa: ANN001
        raise RuntimeError("SES denied")

    monkeypatch.setattr(delivery.awsses, "send_text_message", fake_send)

    out = delivery.deliver_email(to_email="x@y.com", subject="s", body="b")
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


# ── summarize ─────────────────────────────────────────────────────────────


def test_summarize_classifies_mixed_outcomes():
    assert delivery.summarize([]) == "not_attempted"
    assert delivery.summarize([{"status": "sent"}, {"status": "sent"}]) == "success"
    assert delivery.summarize([{"status": "sent"}, {"status": "failed"}]) == "partial"
    assert delivery.summarize([{"status": "failed"}]) == "failed"
