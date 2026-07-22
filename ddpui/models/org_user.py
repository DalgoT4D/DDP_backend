from typing import Optional
from datetime import datetime, timedelta
import uuid
from django.utils import timezone
from enum import IntEnum
from django.utils.text import slugify

from django.db import models
from django.contrib.auth.models import User

from ninja import Schema
from pydantic import SecretStr, BaseModel

from ddpui.models.org import Org
from ddpui.models.role_based_access import Role

from ddpui.schemas.org_schema import OrgSchema


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
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"""
{self.user.email}
  email_verified={self.email_verified}
  can_create_orgs={self.can_create_orgs}
  is_consultant={self.is_consultant}
  is_platform_admin={self.is_platform_admin}
"""  # pylint: disable=no-member


### deprecated
# the OrgUserRole enum is deprecated and replaced by Role model
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
    new_role = models.ForeignKey(Role, on_delete=models.SET_NULL, null=True)
    email_verified = models.BooleanField(default=False)
    llm_optin = models.BooleanField(default=False)  # deprecated
    has_seen_rbac_notice = models.BooleanField(
        default=False,
        help_text="Whether the user has seen the one-time RBAC v2 migration notice",
    )
    landing_dashboard = models.ForeignKey(
        "ddpui.dashboard",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="users_with_as_landing",
        help_text="User's personal landing dashboard",
    )
    work_domain = models.CharField(max_length=500, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.user.email  # pylint: disable=no-member


class OrgUserCreate(Schema):
    """payload to create a new OrgUser"""

    email: str
    password: str
    signupcode: str
    role: Optional[str] = None


class OrgUserUpdate(Schema):
    """payload to update an existing OrgUser"""

    toupdate_email: str
    email: Optional[str] = None
    active: Optional[bool] = None
    role: Optional[str] = None


class OrgUserUpdatev1(Schema):
    """payload to update an existing OrgUser"""

    toupdate_email: str
    role_uuid: Optional[uuid.UUID] = None
    email: Optional[str] = None
    active: Optional[bool] = None
    has_seen_rbac_notice: Optional[bool] = None


class OrgUserUpdateNewRole(Schema):
    """Payload to change the role of an orguser"""

    toupdate_email: str
    role_uuid: uuid.UUID


class OrgUserResponse(Schema):
    """structure for returning an OrgUser in an http response"""

    user_id: int
    # The OrgUser PK — distinct from `user_id` (the Django User FK). The
    # sharing/group endpoints key principals by this, not by `user_id`.
    orguser_id: int
    email: str
    org: Optional[OrgSchema] = None
    active: bool
    wtype: str | None
    is_demo: bool = False
    new_role_slug: str | None
    permissions: list[dict]
    is_llm_active: Optional[bool] = None
    landing_dashboard_id: int | None = None
    org_default_dashboard_id: int | None = None
    subscription_plan: str | None = None
    work_domain: str | None = None
    has_seen_rbac_notice: bool = False
    # The inviter's email for the People table's "Created By" column; None
    # when the user joined without an invitation. Only populated by
    # GET /organizations/users.
    invited_by: str | None = None


def default_invitation_expiry():
    """30 days from now — the default (and resend-refreshed) `Invitation.expires_at`."""
    return timezone.now() + timedelta(days=30)


class Invitation(models.Model):
    """Invitation to join an org"""

    invited_email = models.CharField(max_length=50)
    invited_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
    invited_on = models.DateTimeField()
    invite_code = models.CharField(max_length=36)
    invited_new_role = models.ForeignKey(Role, on_delete=models.CASCADE, null=True)
    expires_at = models.DateTimeField(default=default_invitation_expiry)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)


class NewInvitationSchema(Schema):
    """Invitation schema based on the new_role field added to invitations"""

    invited_email: str
    invited_role_uuid: uuid.UUID


class InvitationSchema(Schema):
    """Docstring"""

    invited_email: str
    invited_by: Optional[OrgUserResponse] = None
    invited_on: Optional[datetime] = None
    invite_code: Optional[str] = None
    invited_new_role_slug: str | None


class AcceptInvitationSchema(Schema):
    """Docstring"""

    invite_code: str
    password: Optional[str] = (
        None  # the password is required only when the user has no platform account
    )
    work_domain: Optional[str] = None


class ForgotPasswordSchema(Schema):
    """the payload for the forgot-password workflow, step 1"""

    email: str


class ResetPasswordSchema(Schema):
    """the payload for the forgot-password workflow, step 2"""

    token: str
    password: SecretStr


class ChangePasswordSchema(Schema):
    """Reset password from settings pannel"""

    password: SecretStr
    confirmPassword: SecretStr


class VerifyEmailSchema(Schema):
    """the payload for the verify-email workflow"""

    token: str


class DeleteOrgUserPayload(Schema):
    """payload to delete an org user"""

    email: str


class LoginPayload(BaseModel):
    """the payload for the login workflow"""

    username: str
    password: str


class LogoutPayload(BaseModel):
    """the payload for the login workflow"""

    refresh: str


class OrgUserGroup(models.Model):
    """A named set of ``OrgUser``s within one org (e.g. "Funders")."""

    org = models.ForeignKey(Org, on_delete=models.CASCADE, related_name="user_groups")
    name = models.CharField(max_length=255)
    created_by = models.ForeignKey(
        OrgUser,
        on_delete=models.SET_NULL,
        null=True,
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "orguser_group"

    def __str__(self):
        return f"{self.name} (org={self.org_id})"


class OrgUserGroupMember(models.Model):
    """One membership row: a group has this ``OrgUser`` (active) or this
    ``pending_email`` (invited, not yet a user) as a member."""

    group = models.ForeignKey(OrgUserGroup, on_delete=models.CASCADE, related_name="members")
    orguser = models.ForeignKey(OrgUser, on_delete=models.CASCADE, null=True)
    pending_email = models.CharField(max_length=255, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "orguser_group_member"
        constraints = [
            models.UniqueConstraint(
                fields=["group", "pending_email"], name="uq_orguser_group_member_pending_email"
            )
        ]

    def __str__(self):
        principal = self.orguser_id if self.orguser_id is not None else self.pending_email
        return f"group={self.group_id} member={principal} ({self.status})"
