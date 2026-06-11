"""Schedule helpers: cron evaluation + frequency-label derivation.

The cron expression is the only thing stored on Alert.schedule_cron. Everything
about scheduling — when an alert is "due," what frequency label to display, what
the next/previous fire instant is — flows from this single string via croniter.
"""

from datetime import datetime
from typing import Optional

from croniter import croniter
from django.utils import timezone

from ddpui.models.alert import Alert


def validate_cron(cron_expr: str) -> None:
    """Raise ValueError if the expression isn't a valid 5-field cron."""
    if not croniter.is_valid(cron_expr):
        raise ValueError(f"Invalid cron expression: {cron_expr!r}")


def previous_fire(cron_expr: str, now: Optional[datetime] = None) -> datetime:
    """Return the most recent scheduled instant at or before `now`.

    `croniter.get_prev` returns a datetime strictly before now. We seed it with
    a tiny look-ahead so an alert whose cron tick equals now is still considered
    'past' for dispatch — otherwise we'd skip it for up to a full cycle.
    """
    if now is None:
        now = timezone.now()
    # get_prev with the next-second boundary so equality at exactly the tick counts as past
    itr = croniter(cron_expr, now)
    prev = itr.get_prev(datetime)
    # croniter returns naive datetime; the caller's `now` is timezone-aware UTC
    if prev.tzinfo is None and now.tzinfo is not None:
        prev = prev.replace(tzinfo=now.tzinfo)
    return prev


def is_due(alert: Alert, now: Optional[datetime] = None) -> bool:
    """Whether an alert's most recent scheduled instant has not yet been evaluated.

    Spec — plan §4.5: dispatcher loops active alerts; each is due iff
    `last_evaluated_at` is NULL or strictly less than the most recent cron tick.
    """
    if now is None:
        now = timezone.now()
    last_scheduled = previous_fire(alert.schedule_cron, now)
    if alert.last_evaluated_at is None:
        return True
    return alert.last_evaluated_at < last_scheduled


def derive_frequency_label(cron_expr: str) -> str:
    """Reverse-derive a human-friendly frequency from the cron.

    Returns one of: 'daily', 'weekly', 'monthly', or 'cron' (fallback for any
    pattern the wizard didn't produce).

    Wizard-produced shapes (all in UTC):
      daily   "M H * * *"
      weekly  "M H * * D"   where D is 0–6
      monthly "M H D * *"   where D is 1–28
    """
    try:
        parts = cron_expr.strip().split()
    except AttributeError:
        return "cron"
    if len(parts) != 5:
        return "cron"
    minute, hour, dom, month, dow = parts
    # Wizard-produced shapes require concrete minute + hour (single integers),
    # month = "*". Anything else (steps, ranges, lists) falls back to cron.
    if month != "*":
        return "cron"
    if not (minute.isdigit() and hour.isdigit()):
        return "cron"

    if dom == "*" and dow == "*":
        return "daily"
    if dom == "*" and dow.isdigit() and 0 <= int(dow) <= 6:
        return "weekly"
    if dow == "*" and dom.isdigit() and 1 <= int(dom) <= 28:
        return "monthly"
    return "cron"
