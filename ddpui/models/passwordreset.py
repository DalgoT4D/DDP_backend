from datetime import datetime
from uuid import uuid4

from django.db import models

from ddpui.models.org_user import OrgUser


class PasswordReset(models.Model):
    """
    this table holds the short-lived password-reset tokens
    we create an entry when a user requests a password reset
    we send them a reset url containing the token
    that url opens in the frontend
    they provide their new password and the frontend sends it
    along with the token
    """

    orguser = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=datetime.now)
    token = models.UUIDField(default=uuid4)
