"""Tests for ddpui.core.alerts.scheduling"""

import datetime as dt
from types import SimpleNamespace

import pytest
from django.utils import timezone

from ddpui.core.alerts import scheduling


# ── derive_frequency_label ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "cron,expected",
    [
        ("0 9 * * *", "daily"),
        ("30 3 * * *", "daily"),
        ("0 9 * * 1", "weekly"),
        ("30 3 * * 6", "weekly"),
        ("0 9 15 * *", "monthly"),
        ("30 3 28 * *", "monthly"),
        # Patterns the wizard doesn't produce
        ("*/5 * * * *", "cron"),
        ("0 9 1-15 * *", "cron"),
        ("not a cron", "cron"),
    ],
)
def test_derive_frequency_label(cron, expected):
    assert scheduling.derive_frequency_label(cron) == expected


# ── validate_cron ──────────────────────────────────────────────────────────


def test_validate_cron_accepts_valid():
    scheduling.validate_cron("0 9 * * *")


def test_validate_cron_rejects_invalid():
    with pytest.raises(ValueError):
        scheduling.validate_cron("not a cron")


# ── previous_fire ──────────────────────────────────────────────────────────


def test_previous_fire_daily():
    # Daily 09:00 UTC. Now is 10:00 UTC same day → prev = today 09:00.
    now = dt.datetime(2026, 6, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    prev = scheduling.previous_fire("0 9 * * *", now)
    assert prev == dt.datetime(2026, 6, 11, 9, 0, 0, tzinfo=dt.timezone.utc)


def test_previous_fire_before_first_tick():
    # Daily 09:00. Now is 08:30 UTC → prev = yesterday 09:00.
    now = dt.datetime(2026, 6, 11, 8, 30, 0, tzinfo=dt.timezone.utc)
    prev = scheduling.previous_fire("0 9 * * *", now)
    assert prev == dt.datetime(2026, 6, 10, 9, 0, 0, tzinfo=dt.timezone.utc)


# ── is_due ─────────────────────────────────────────────────────────────────


def _fake_alert(cron, last_evaluated_at, created_at=None):
    if created_at is None:
        # Default far in the past so created_at floor doesn't interfere with
        # tests that focus on last_evaluated_at semantics.
        created_at = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    return SimpleNamespace(
        schedule_cron=cron,
        last_evaluated_at=last_evaluated_at,
        created_at=created_at,
    )


def test_is_due_when_never_evaluated_and_created_before_last_tick():
    now = dt.datetime(2026, 6, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    # created yesterday; today's 09:00 tick has passed and not been served
    created_at = dt.datetime(2026, 6, 10, 8, 0, 0, tzinfo=dt.timezone.utc)
    alert = _fake_alert("0 9 * * *", last_evaluated_at=None, created_at=created_at)
    assert scheduling.is_due(alert, now) is True


def test_is_not_due_when_never_evaluated_and_created_after_last_tick():
    now = dt.datetime(2026, 6, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    # alert created at 09:30 today — last scheduled tick (09:00) predates creation,
    # so dispatcher should NOT fire it; the alert should wait for tomorrow's 09:00
    created_at = dt.datetime(2026, 6, 11, 9, 30, 0, tzinfo=dt.timezone.utc)
    alert = _fake_alert("0 9 * * *", last_evaluated_at=None, created_at=created_at)
    assert scheduling.is_due(alert, now) is False


def test_is_due_when_last_eval_before_scheduled_tick():
    now = dt.datetime(2026, 6, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    # last evaluated yesterday at the previous 09:00 → today's 09:00 hasn't been served yet
    last_eval = dt.datetime(2026, 6, 10, 9, 0, 5, tzinfo=dt.timezone.utc)
    alert = _fake_alert("0 9 * * *", last_evaluated_at=last_eval)
    assert scheduling.is_due(alert, now) is True


def test_is_not_due_when_already_evaluated_for_this_tick():
    now = dt.datetime(2026, 6, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    # last evaluated today right after 09:00 → not due again until tomorrow
    last_eval = dt.datetime(2026, 6, 11, 9, 0, 5, tzinfo=dt.timezone.utc)
    alert = _fake_alert("0 9 * * *", last_evaluated_at=last_eval)
    assert scheduling.is_due(alert, now) is False
