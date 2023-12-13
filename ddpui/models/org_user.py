from datetime import datetime
from enum import IntEnum
from django.utils.text import slugify


from django.db import models
from django.contrib.auth.models import User

from ninja import Schema
from pydantic import SecretStr

from ddpui.models.org import Org, OrgSchema, OrgWarehouse


class UserAttributes(models.Model):
    """extensions to the django User object"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    email_verified = models.BooleanField(default=False)
    can_create_orgs = models.BooleanField(default=False)


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
    email_verified = models.BooleanField(default=False)
    can_accept_tnc = models.BooleanField(default=False)

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

    @staticmethod
    def from_orguser(orguser: OrgUser):
        """helper to turn an OrgUser into an OrgUserResponse"""
        warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
        return OrgUserResponse(
            email=orguser.user.email,
            org=orguser.org,
            active=orguser.user.is_active,
            can_accept_tnc=orguser.can_accept_tnc,
            role=orguser.role,
            role_slug=slugify(OrgUserRole(orguser.role).name),
            wtype=warehouse.wtype if warehouse else None,
        )


class Invitation(models.Model):
    """
    can_accept_tnc is only relevant for new users from the org being invited by
    t4d or one of our partners
    """

    invited_email = models.CharField(max_length=50)
    invited_role = models.IntegerField(
        choices=OrgUserRole.choices(), default=OrgUserRole.REPORT_VIEWER
    )
    invited_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    invited_on = models.DateTimeField()
    invite_code = models.CharField(max_length=36)
    can_accept_tnc = models.BooleanField(default=False)


class InvitationSchema(Schema):
    """Docstring"""

    invited_email: str
    invited_role_slug: str
    invited_role: int = None
    invited_by: OrgUserResponse = None
    invited_on: datetime = None
    invite_code: str = None
    can_accept_tnc: bool = None

    @staticmethod
    def from_invitation(invitation: Invitation):
        """Docstring"""
        return InvitationSchema(
            invited_email=invitation.invited_email,
            invited_role=invitation.invited_role,
            invited_role_slug=slugify(OrgUserRole(invitation.invited_role).name),
            invited_by=OrgUserResponse.from_orguser(invitation.invited_by),
            invited_on=invitation.invited_on,
            invite_code=invitation.invite_code,
            can_accept_tnc=invitation.can_accept_tnc,
        )


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
