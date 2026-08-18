"""send emails using SES"""

import os
import email.mime.multipart
import email.mime.text
import email.mime.application

from django.conf import settings

from ddpui.utils.aws_client import AWSClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.email_templates import (
    render_verify_email,
    render_trial_welcome_email,
    render_trial_day3_not_started_email,
    render_trial_day3_in_progress_email,
    render_trial_completion_email,
    render_trial_midpoint_email,
    render_trial_pre_end_email,
)


logger = CustomLogger("ddpui.utils.awsses")


def _get_ses_client():
    """Get SES client instance - lazy initialization to avoid import-time failures"""
    return AWSClient.get_instance("ses")


def send_text_message(to_email, subject, message):
    """
    send a plain-text email using ses
    """
    ses = _get_ses_client()
    response = ses.send_email(
        Destination={"ToAddresses": [to_email]},
        Message={
            "Body": {"Text": {"Charset": "UTF-8", "Data": message}},
            "Subject": {"Charset": "UTF-8", "Data": subject},
        },
        Source=os.getenv("SES_SENDER_EMAIL"),
    )
    return response


def send_html_message(to_email, subject, text_body, html_body):
    """
    send an email with both HTML and plain-text body using ses
    """
    ses = _get_ses_client()
    response = ses.send_email(
        Destination={"ToAddresses": [to_email]},
        Message={
            "Body": {
                "Text": {"Charset": "UTF-8", "Data": text_body},
                "Html": {"Charset": "UTF-8", "Data": html_body},
            },
            "Subject": {"Charset": "UTF-8", "Data": subject},
        },
        Source=os.getenv("SES_SENDER_EMAIL"),
    )
    return response


def send_email_with_attachment(
    to_email: str,
    subject: str,
    text_body: str,
    html_body: str,
    attachment_bytes: bytes,
    attachment_filename: str,
):
    """Send an HTML email with a PDF attachment via SES send_raw_email."""
    ses = _get_ses_client()
    sender = os.getenv("SES_SENDER_EMAIL")

    msg = email.mime.multipart.MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_email

    # HTML + plain-text body (alternative part)
    body_part = email.mime.multipart.MIMEMultipart("alternative")
    body_part.attach(email.mime.text.MIMEText(text_body, "plain", "utf-8"))
    body_part.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))
    msg.attach(body_part)

    # PDF attachment
    attachment = email.mime.application.MIMEApplication(attachment_bytes, "pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=attachment_filename)
    msg.attach(attachment)

    return ses.send_raw_email(
        Source=sender,
        Destinations=[to_email],
        RawMessage={"Data": msg.as_string()},
    )


def send_password_reset_email(to_email: str, reset_url: str) -> None:
    """send a password reset email"""
    message = f"""Hello,

We received a request to reset your Dalgo password.

Please click this link to begin: {reset_url}.

If you did not request a password reset you may safely ignore this email.

"""
    send_text_message(to_email, "You've requested a password reset", message)


def send_trial_verification_email(to_email: str, verify_url: str) -> None:
    """send a branded HTML email to verify a free-trial signup and set a password"""
    subject = "Verify your email to start your Dalgo trial"
    text_body, html_body = render_verify_email(verify_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_welcome_email(to_email: str, login_url: str) -> None:
    """sent once a free-trial clone finishes — so the user gets in even if they closed the tab"""
    subject = "Welcome to Dalgo — your trial workspace is ready"
    text_body, html_body = render_trial_welcome_email(login_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_day3_not_started_email(
    to_email: str, workspace_url: str, schedule_call_url: str
) -> None:
    """day-3 nudge for a trial user who has completed no walkthrough yet"""
    subject = "Ready to see Dalgo in action?"
    text_body, html_body = render_trial_day3_not_started_email(workspace_url, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_day3_in_progress_email(
    to_email: str, completed_flow: str, workspace_url: str, schedule_call_url: str
) -> None:
    """day-3 nudge for a trial user who has completed exactly one walkthrough"""
    subject = "Pick up where you left off"
    text_body, html_body = render_trial_day3_in_progress_email(
        completed_flow, workspace_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_completion_email(to_email: str, workspace_url: str, schedule_call_url: str) -> None:
    """sent once both tracked walkthroughs are complete, on or after day 3"""
    subject = "You've completed your tour of Dalgo"
    text_body, html_body = render_trial_completion_email(workspace_url, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_midpoint_email(
    to_email: str, day_number: int, total_days: int, schedule_call_url: str
) -> None:
    """mid-trial nudge, e.g. day 7 of 14"""
    subject = "You're halfway through your Dalgo trial"
    text_body, html_body = render_trial_midpoint_email(day_number, total_days, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_pre_end_email(
    to_email: str,
    day_number: int,
    total_days: int,
    end_date: str,
    schedule_call_url: str,
) -> None:
    """expiry warning, sent two days before the trial ends.

    `end_date` is already formatted for display (e.g. "15 Aug 2026") — the renderer does no
    date maths of its own.
    """
    subject = f"{total_days - day_number} days left in your Dalgo trial"
    text_body, html_body = render_trial_pre_end_email(
        day_number, total_days, end_date, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_signup_email(to_email: str, verification_url: str) -> None:
    """send a signup email with an email verification link"""
    message = f"""Hello,

Welcome to Dalgo! Please verify your email address by clicking the link below

{verification_url}

    """
    send_text_message(to_email, "Welcome to Dalgo", message)


def send_invite_user_email(to_email: str, invited_by_email: str, invite_url: str) -> None:
    """send an invitation email to the user with the invite link through which they will set their password"""
    message = f"""Hello,

Welcome to Dalgo.

You have been invited by {invited_by_email}

Click here to accept: {invite_url}

    """
    send_text_message(to_email, "Welcome to Dalgo", message)


def send_youve_been_added_email(to_email: str, added_by: str, org_name: str) -> None:
    """sends an email notification informing an existing dalgo user that they have
    been granted access to a new org
    """
    url = os.getenv("FRONTEND_URL")
    message = f"""Hello,

You've been added to {org_name} by {added_by}.

Open your dashboard at {url}
    """
    send_text_message(to_email, "Added to Dalgo Org", message)


def biz_dev_recipients() -> list:
    """The partnerships/biz-dev addresses from the BIZ_DEV_EMAILS env var.

    Comma-separated so recipients can change without a deploy; blanks and stray whitespace
    are dropped, so "a@x.org, ,b@x.org" yields two addresses. Returns [] when unset — each
    caller decides whether that is an error (the subscription-request endpoint) or something
    to log and move on from (the new-org notification).
    """
    return [email.strip() for email in os.getenv("BIZ_DEV_EMAILS", "").split(",") if email.strip()]


def send_biz_dev_notification(subject: str, body: str) -> None:
    """Send a plain-text internal notification to every BIZ_DEV_EMAILS address.

    Best-effort and never raises: callers are on success paths of work that is already done
    (an org exists), and a mail problem must not turn that into a failure. Sent per-recipient
    so one bad or bouncing address cannot stop the others.
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


def send_trial_ops_alert(subject: str, body: str) -> None:
    """Report a trial teardown/cleanup failure to the engineering address.

    Best-effort and never raises: every caller is an error path that must not be replaced by a
    mail error. A swallowed send is logged and the underlying failure is already in the logs.
    """
    try:
        send_text_message(settings.TRIAL_ALERT_EMAIL, f"[Dalgo trials] {subject}", body)
    except Exception as err:  # skipcq PYL-W0703
        logger.error("failed to send trial ops alert %r: %s", subject, err)
