import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

SENDGRID_APIKEY = os.getenv("SENDGRID_APIKEY")
SENDGRID_SENDER = os.getenv("SENDGRID_SENDER")


def send_text_message(to_email, subject, message):
    """
    send a plain-text email using sendgrid
    """
    sendgrid_client = SendGridAPIClient(SENDGRID_APIKEY)

    content = Content("text/plain", message)
    message = Mail(
        SENDGRID_SENDER,
        to_email,
        subject,
        content,
    )

    try:
        sendgrid_client.send(message)
    except Exception as error:
        logger.exception(error)
        raise


def send_template_message(template_id: str, to_email: str, template_vars: dict) -> None:
    """
    this function sends a templated email to a single recipient
    using sendgrid's api.
    """
    sendgrid_client = SendGridAPIClient(SENDGRID_APIKEY)

    message = Mail(from_email=SENDGRID_SENDER, to_emails=[to_email])
    message.template_id = template_id
    message.dynamic_template_data = template_vars

    try:
        sendgrid_client.send(message)
    except Exception as error:
        logger.exception(error)
        raise


def send_password_reset_email(to_email: str, reset_url: str) -> None:
    """send a password reset email"""
    send_template_message(
        os.getenv("SENDGRID_RESET_PASSWORD_TEMPLATE"), to_email, {"url": reset_url}
    )


def send_signup_email(to_email: str, verification_url: str) -> None:
    """send a signup email with an email verification link"""
    send_template_message(
        os.getenv("SENDGRID_SIGNUP_TEMPLATE"), to_email, {"url": verification_url}
    )


def send_invite_user_email(
    to_email: str, invited_by_email: str, invite_url: str
) -> None:
    """send an invitation email to the user with the invite link through which they will set their password"""
    send_template_message(
        os.getenv("SENDGRID_INVITE_USER_TEMPLATE"),
        to_email,
        {"url": invite_url, "invited_by_email": invited_by_email},
    )
