from datetime import datetime
from enum import IntEnum, Enum

from django.db import models
from django.contrib.auth.models import User

from ninja import Schema
from pydantic import SecretStr

from ddpui.models.org import Org, OrgSchema


class OrgUserRole(IntEnum):
    """an enum for roles assignable to org-users"""

    REPORT_VIEWER = 1
    PIPELINE_MANAGER = 2
    ACCOUNT_MANAGER = 3

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class OrgUser(models.Model):
    """a user from a client NGO"""

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    org = models.ForeignKey(Org, on_delete=models.CASCADE, null=True)
    role = models.IntegerField(
        choices=OrgUserRole.choices(), default=OrgUserRole.REPORT_VIEWER
    )
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


class OrgUserResponse(Schema):
    """structure for returning an OrgUser in an http response"""

    email: str
    org: OrgSchema = None
    active: bool
    role: int

    @staticmethod
    def from_orguser(orguser: OrgUser):
        """helper to turn an OrgUser into an OrgUserResponse"""
        return OrgUserResponse(
            email=orguser.user.email,
            org=orguser.org,
            active=orguser.user.is_active,
            role=orguser.role,
        )


class InvitationStatus(str, Enum):
    """an enum for statuses of invitation"""

    PENDING = "pending"
    ACCEPTED = "accepted"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class Invitation(models.Model):
    """Docstring"""

    invited_email = models.CharField(max_length=50)
    invited_role = models.IntegerField(
        choices=OrgUserRole.choices(), default=OrgUserRole.REPORT_VIEWER
    )
    invited_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    invited_on = models.DateTimeField()
    invite_code = models.CharField(max_length=36)
    status = models.CharField(
        max_length=50, default="pending", choices=InvitationStatus.choices()
    )


class InvitationSchema(Schema):
    """Docstring"""

    invited_email: str
    invited_role: int
    invited_by: OrgUserResponse = None
    invited_on: datetime = None
    invite_code: str = None

    @staticmethod
    def from_invitation(invitation: Invitation):
        """Docstring"""
        return InvitationSchema(
            invited_email=invitation.invited_email,
            invited_role=invitation.invited_role,
            invited_by=OrgUserResponse.from_orguser(invitation.invited_by),
            invited_on=invitation.invited_on,
            invite_code=invitation.invite_code,
        )


class AcceptInvitationSchema(Schema):
    """Docstring"""

    invite_code: str
    password: str


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
