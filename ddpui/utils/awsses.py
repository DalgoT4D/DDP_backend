"""SES transport primitives + biz-dev broadcast helpers.

Higher-level notification flows (alerts, mentions, report shares, user
transactional, trial lifecycle) build on ``send_text_message`` /
``send_html_message`` / ``send_email_with_attachment`` here but live under
``ddpui.core.notifications.triggers``.
"""

import os
import email.mime.multipart
import email.mime.text
import email.mime.application

from ddpui.utils.aws_client import AWSClient
from ddpui.utils.custom_logger import CustomLogger


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
