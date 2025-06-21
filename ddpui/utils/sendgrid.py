import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

SENDGRID_APIKEY = os.getenv("SENDGRID_APIKEY")
SENDGRID_SENDER = os.getenv("SENDGRID_SENDER")


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
        logger.info(f"sent email to {to_email} using template {template_id}")
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


def send_invite_user_email(to_email: str, invited_by_email: str, invite_url: str) -> None:
    """send an invitation email to the user with the invite link through which they will set their password"""
    send_template_message(
        os.getenv("SENDGRID_INVITE_USER_TEMPLATE"),
        to_email,
        {"url": invite_url, "invited_by_email": invited_by_email},
    )


def send_youve_been_added_email(to_email: str, added_by: str, org_name: str) -> None:
    """sends an email notification informing an existing dalgo user that they have
    been granted access to a new org
    """
    send_template_message(
        os.getenv("SENDGRID_YOUVE_BEEN_ADDED_TEMPLATE"),
        to_email,
        {
            "org_name": org_name,
            "added_by": added_by,
            "url": os.getenv("FRONTEND_URL"),
        },
    )
