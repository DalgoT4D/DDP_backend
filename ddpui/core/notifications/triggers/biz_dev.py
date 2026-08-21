"""Biz-dev / partnerships internal notification triggers.

Sent to the addresses in ``BIZ_DEV_EMAILS`` — the internal team inbox that
reads new-org signups and subscription-upgrade requests. Plain text, no
in-app bell entry (recipients aren't OrgUsers).
"""

import os

from ddpui.utils.awsses import send_text_message
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.biz_dev")


def biz_dev_recipients() -> list:
    """The partnerships/biz-dev addresses from the BIZ_DEV_EMAILS env var.

    Comma-separated so recipients can change without a deploy; blanks and
    stray whitespace are dropped, so "a@x.org, ,b@x.org" yields two
    addresses. Returns [] when unset — each caller decides whether that is
    an error (the subscription-request endpoint) or something to log and
    move on from (the new-org notification).
    """
    return [email.strip() for email in os.getenv("BIZ_DEV_EMAILS", "").split(",") if email.strip()]


def send_notification(subject: str, body: str) -> None:
    """Send a plain-text internal notification to every BIZ_DEV_EMAILS address.

    Best-effort and never raises: callers are on success paths of work that is
    already done (an org exists), and a mail problem must not turn that into a
    failure. Sent per-recipient so one bad or bouncing address cannot stop the
    others.
    """
    recipients = biz_dev_recipients()
    if not recipients:
        logger.warning("BIZ_DEV_EMAILS is not configured; not sending %r", subject)
        return

    for to_email in recipients:
        try:
            send_text_message(to_email, subject, body)
        except Exception as err:  # skipcq PYL-W0703
            logger.error("failed to send biz-dev notification %r to %s: %s", subject, to_email, err)
