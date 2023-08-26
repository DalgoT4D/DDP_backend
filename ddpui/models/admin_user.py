from ninja import Schema
from django.db import models
from django.contrib.auth.models import User


class AdminUser(models.Model):
    """Docstring"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)


class AdminUserResponse(Schema):
    """Docstring"""

    email: str
    active: str

    @staticmethod
    def fromadminuser(adminuser):
        """helper"""
        return AdminUserResponse(
            email=adminuser.user.email,
            active=adminuser.user.is_active,
        )
