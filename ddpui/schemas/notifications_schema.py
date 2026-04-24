from ninja import Schema


class NotificationMessageInfo(Schema):
    """
    Schema for information about a notification message, including its content, subject,
    whether it should be sent, and any reason for skipping it.
    """

    content: str = ""
    subject: str = ""
    should_send: bool = False
    skip_reason: str = ""
