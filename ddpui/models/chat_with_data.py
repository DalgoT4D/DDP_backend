import uuid
from enum import Enum
from django.db import models
from django.utils import timezone
from ddpui.models.org_user import OrgUser


class MessageType(str, Enum):
    """an enum representing the type of messages"""

    AI = "ai"
    HUMAN = "human"
    SYSTEM = "system"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Thread(models.Model):
    """
    Model to store thread that represents a series of message(questions) on particular set of data
    The session_id ties it to the llm service file search session
    """

    uuid = models.UUIDField(editable=False, unique=True)
    session_id = models.CharField(max_length=200, null=True)
    meta = models.JSONField(null=True)
    orguser = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Thread[{self.uuid}]"

    class Meta:
        db_table = "ddpui_threads"


class Message(models.Model):
    """Model to store messages of conversation between a user and chat with data bot"""

    content = models.TextField()
    sender = models.ForeignKey(OrgUser, null=True, on_delete=models.SET_NULL, related_name="sender")
    recipient = models.ForeignKey(
        OrgUser, null=True, on_delete=models.SET_NULL, related_name="recipient"
    )
    thread = models.ForeignKey(Thread, on_delete=models.CASCADE)
    type = models.CharField(
        choices=MessageType.choices(), max_length=50, default=MessageType.HUMAN.value
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        sender_name = self.sender.user.email if self.sender.user else "Bot"
        recipient_name = self.recipient.user.email if self.recipient.user else "Bot"
        content = self.content[:50]
        return f"Message[{sender_name} -> {recipient_name}|{self.type}|{content}|]"

    class Meta:
        db_table = "ddpui_messages"
