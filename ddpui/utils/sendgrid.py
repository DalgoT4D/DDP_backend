import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from ddpui.utils.ddp_logger import logger

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
    except Exception as error:
        logger.exception(error)
        raise


def send_password_reset_email(to_email: str, reset_url: str) -> None:
    """
    send a password reset email
    """
    send_template_message(
        os.getenv("SENDGRID_RESET_PASSWORD_TEMPLATE"), to_email, reset_url
    )
