from datetime import datetime
import uuid
from enum import IntEnum
from django.utils.text import slugify


from django.db import models
from django.contrib.auth.models import User

from ninja import Schema
from pydantic import SecretStr

from ddpui.models.org import Org, OrgSchema
from ddpui.models.role_based_access import Role


class UserAttributes(models.Model):
    """
    extensions to the django User object
    please update the `manage-user-attributes` management command
      when modifying this list
    """

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    email_verified = models.BooleanField(default=False)
    can_create_orgs = models.BooleanField(default=False)
    is_consultant = models.BooleanField(default=False)
    is_platform_admin = models.BooleanField(default=False)

    def __str__(self):
        return f"""
{self.user.email}
  email_verified={self.email_verified}
  can_create_orgs={self.can_create_orgs}
  is_consultant={self.is_consultant}
  is_platform_admin={self.is_platform_admin}
"""  # pylint: disable=no-member


class OrgUserRole(IntEnum):
    """an enum for roles assignable to org-users"""

    REPORT_VIEWER = 1
    PIPELINE_MANAGER = 2
    ACCOUNT_MANAGER = 3

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]

    @classmethod
    def role_slugs(cls):
        """return a dictionary with slug as key and role_id as value"""
        role_dict = {}
        for key in cls:
            slug = slugify(key.name)
            role_dict[slug] = key.value
        return role_dict


class OrgUser(models.Model):
    """a user from a client NGO"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    org = models.ForeignKey(Org, on_delete=models.CASCADE, null=True)
    role = models.IntegerField(
        choices=OrgUserRole.choices(), default=OrgUserRole.REPORT_VIEWER
    )
    new_role = models.ForeignKey(Role, on_delete=models.SET_NULL, null=True)
    email_verified = models.BooleanField(default=False)

    def __str__(self):
        return self.user.email  # pylint: disable=no-member


class OrgUserCreate(Schema):
    """payload to create a new OrgUser"""

    email: str
    password: str
    signupcode: str
    role: str = None


class OrgUserUpdate(Schema):
    """payload to update an existing OrgUser"""

    toupdate_email: str
    email: str = None
    active: bool = None
    role: str = None


class OrgUserUpdatev1(Schema):
    """payload to update an existing OrgUser"""

    toupdate_email: str
    role_uuid: uuid.UUID = None
    email: str = None
    active: bool = None


class OrgUserUpdateNewRole(Schema):
    """Payload to change the role of an orguser"""

    toupdate_email: str
    role_uuid: uuid.UUID


class OrgUserNewOwner(Schema):
    """payload to transfer account ownership"""

    new_owner_email: str


class OrgUserResponse(Schema):
    """structure for returning an OrgUser in an http response"""

    email: str
    org: OrgSchema = None
    active: bool
    role: int
    role_slug: str
    wtype: str | None
    is_demo: bool = False
    new_role_slug: str | None
    permissions: list[dict]


class Invitation(models.Model):
    """Invitation to join an org"""

    invited_email = models.CharField(max_length=50)
    invited_role = models.IntegerField(
        choices=OrgUserRole.choices(), default=OrgUserRole.REPORT_VIEWER
    )
    invited_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    invited_on = models.DateTimeField()
    invite_code = models.CharField(max_length=36)
    invited_new_role = models.ForeignKey(Role, on_delete=models.CASCADE, null=True)


class NewInvitationSchema(Schema):
    """Invitation schema based on the new_role field added to invitations"""

    invited_email: str
    invited_role_uuid: uuid.UUID


class InvitationSchema(Schema):
    """Docstring"""

    invited_email: str
    invited_role_slug: str
    invited_role: int = None
    invited_by: OrgUserResponse = None
    invited_on: datetime = None
    invite_code: str = None
    invited_new_role_slug: str | None


class AcceptInvitationSchema(Schema):
    """Docstring"""

    invite_code: str
    password: str = (
        None  # the password is required only when the user has no platform account
    )


class ForgotPasswordSchema(Schema):
    """the payload for the forgot-password workflow, step 1"""

    email: str


class ResetPasswordSchema(Schema):
    """the payload for the forgot-password workflow, step 2"""

    token: str
    password: SecretStr


class VerifyEmailSchema(Schema):
    """the payload for the verify-email workflow"""

    token: str


class DeleteOrgUserPayload(Schema):
    """payload to delete an org user"""

    email: str
