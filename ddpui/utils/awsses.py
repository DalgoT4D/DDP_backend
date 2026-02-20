"""send emails using SES"""

import os
from ddpui.utils.aws_client import AWSClient


def get_ses_client():
    """Get SES client from unified AWS client using SES credentials"""
    return AWSClient.get_instance("ses", "ses")


def send_text_message(to_email, subject, message):
    """
    send a plain-text email using ses
    """
    ses = get_ses_client()
    response = ses.send_email(
        Destination={"ToAddresses": [to_email]},
        Message={
            "Body": {"Text": {"Charset": "UTF-8", "Data": message}},
            "Subject": {"Charset": "UTF-8", "Data": subject},
        },
        Source=os.getenv("SES_SENDER_EMAIL"),
    )
    return response


def send_password_reset_email(to_email: str, reset_url: str) -> None:
    """send a password reset email"""
    message = f"""Hello,

We received a request to reset your Dalgo password.

Please click this link to begin: {reset_url}.

If you did not request a password reset you may safely ignore this email.

"""
    send_text_message(to_email, "You've requested a password reset", message)


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
