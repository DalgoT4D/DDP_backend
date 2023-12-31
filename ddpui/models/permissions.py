"""user permissions"""
from enum import IntEnum
from django.db import models
from ddpui.models.org_user import OrgUser


class Verb(IntEnum):
    """create / update / delete / view"""

    NONE = 0
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    VIEW = 4

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Target(IntEnum):
    """targets for permissions"""

    WAREHOUSE = 0
    SOURCES = 1
    CONNECTIONS = 2
    USERS = 3

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Permission(models.Model):
    """a permission that can be assigned to a user"""

    default_is_able = models.BooleanField(null=False)
    verb = models.IntegerField(choices=Verb.choices(), default=Verb.NONE)
    target = models.IntegerField(choices=Target.choices())

    def __str__(self):
        return f"{'can' if self.default_is_able else 'cannot'} {Verb(self.verb).name} {Target(self.target).name}"


class UserPermissions(models.Model):
    """maps users to their permissions"""

    orguser = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)
    is_able = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.orguser.user.email} {'can' if self.is_able else 'cannot'} {Verb(self.permission.verb).name} {Target(self.permission.target).name}"
