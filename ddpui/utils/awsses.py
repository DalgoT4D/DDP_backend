"""send emails using SES"""

import os
import boto3

ses = boto3.client(
    "ses",
    "ap-south-1",
    aws_access_key_id=os.getenv("SES_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("SES_SECRET_ACCESS_KEY"),
)


def send_text_message(to_email, subject, message):
    """
    send a plain-text email using ses
    """
    response = ses.send_email(
        Destination={"ToAddresses": [to_email]},
        Message={
            "Body": {"Text": {"Charset": "UTF-8", "Data": message}},
            "Subject": {"Charset": "UTF-8", "Data": subject},
        },
        Source=os.getenv("SES_SENDER_EMAIL"),
    )
    return response
