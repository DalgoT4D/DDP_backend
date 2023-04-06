from django.db import models
from ninja import Schema
from django.contrib.auth.models import User


class AdminUser(models.Model):
    """Docstring"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)


class AdminUserResponse(Schema):
    """Docstring"""

    email: str
    active: str
