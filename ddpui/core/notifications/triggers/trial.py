"""Trial-lifecycle email triggers.

Every helper renders through the trial-shell templates in
``../templates/trial.py`` and sends via SES. Recipients are unauthenticated
(pre-signup) or in mid-trial with no persisted preferences, so these emails
are always sent — no in-app bell entry, no ``UserPreferences`` gating.

Also owns ``send_ops_alert`` — the internal engineering address for trial
teardown/cleanup failures. Distinct from ``triggers/biz_dev`` (partnerships)
because it goes to ``TRIAL_ALERT_EMAIL``, not ``BIZ_DEV_EMAILS``.
"""

from django.conf import settings

from ddpui.core.notifications.templates import (
    render_trial_completion_email,
    render_trial_day3_in_progress_email,
    render_trial_day3_not_started_email,
    render_trial_midpoint_email,
    render_trial_pre_end_email,
    render_trial_welcome_email,
    render_verify_email,
)
from ddpui.utils.awsses import send_html_message, send_text_message
from ddpui.utils.custom_logger import CustomLogger


logger = CustomLogger("ddpui.notifications.triggers.trial")


def send_verification(to_email: str, verify_url: str) -> None:
    """Branded HTML email to verify a free-trial signup and set a password."""
    subject = "Verify your email to start your Dalgo trial"
    text_body, html_body = render_verify_email(verify_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_welcome(to_email: str, login_url: str) -> None:
    """Sent once a free-trial clone finishes — so the user gets in even if they closed the tab."""
    subject = "Welcome to Dalgo — your trial workspace is ready"
    text_body, html_body = render_trial_welcome_email(login_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_day3_not_started(to_email: str, workspace_url: str, schedule_call_url: str) -> None:
    """Day-3 nudge for a trial user who has completed no walkthrough yet."""
    subject = "Ready to see Dalgo in action?"
    text_body, html_body = render_trial_day3_not_started_email(workspace_url, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_day3_in_progress(
    to_email: str, completed_flow: str, workspace_url: str, schedule_call_url: str
) -> None:
    """Day-3 nudge for a trial user who has completed exactly one walkthrough."""
    subject = "Pick up where you left off"
    text_body, html_body = render_trial_day3_in_progress_email(
        completed_flow, workspace_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_completion(to_email: str, workspace_url: str, schedule_call_url: str) -> None:
    """Sent once both tracked walkthroughs are complete, on or after day 3."""
    subject = "You've completed your tour of Dalgo"
    text_body, html_body = render_trial_completion_email(workspace_url, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_midpoint(to_email: str, day_number: int, total_days: int, schedule_call_url: str) -> None:
    """Mid-trial nudge, e.g. day 7 of 14."""
    subject = "You're halfway through your Dalgo trial"
    text_body, html_body = render_trial_midpoint_email(day_number, total_days, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_pre_end(
    to_email: str,
    day_number: int,
    total_days: int,
    end_date: str,
    schedule_call_url: str,
) -> None:
    """Expiry warning, sent two days before the trial ends.

    ``end_date`` is already formatted for display (e.g. "15 Aug 2026") — the
    renderer does no date maths of its own.
    """
    subject = f"{total_days - day_number} days left in your Dalgo trial"
    text_body, html_body = render_trial_pre_end_email(
        day_number, total_days, end_date, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_ops_alert(subject: str, body: str) -> None:
    """Report a trial teardown/cleanup failure to the engineering address.

    Best-effort and never raises: every caller is an error path that must not
    be replaced by a mail error. A swallowed send is logged and the underlying
    failure is already in the logs.
    """
    try:
        send_text_message(settings.TRIAL_ALERT_EMAIL, f"[Dalgo trials] {subject}", body)
    except Exception as err:  # skipcq PYL-W0703
        logger.error("failed to send trial ops alert %r: %s", subject, err)
