from django.db import models
from datetime import datetime
from ninja import ModelSchema, Schema

from ddpui.models.org import Org, OrgSchema

# ====================================================================================================
class OrgUser(models.Model):
  active = models.BooleanField(default=True)
  email = models.CharField(max_length=50, null=True, unique=True)
  org = models.ForeignKey(Org, on_delete=models.CASCADE, null=True)

  def __str__(self):
    return self.email

class OrgUserCreate(ModelSchema):
  class Config:
    model = OrgUser
    model_fields = ['email']

class OrgUserUpdate(Schema):
  email: str = None
  active: bool = None

class OrgUserResponse(Schema):
  email: str
  org: OrgSchema = None
  active: bool

  @staticmethod
  def from_orguser(orguser: OrgUser):
    return OrgUserResponse(email=orguser.email, org=orguser.org, active=orguser.active)

# ====================================================================================================
class Invitation(models.Model):
  invited_email = models.CharField(max_length=50)
  invited_by = models.ForeignKey(OrgUser, on_delete=models.CASCADE)
  invited_on = models.DateTimeField()
  invite_code = models.CharField(max_length=36)

class InvitationSchema(Schema):
  invited_email: str
  invited_by: OrgUserResponse = None
  invited_on: datetime = None
  invite_code: str = None

  @staticmethod
  def from_invitation(invitation: Invitation):
    return InvitationSchema(
      invited_email=invitation.invited_email,
      invited_by=OrgUserResponse.from_clientuser(invitation.invited_by),
      invited_on=invitation.invited_on,
      invite_code=invitation.invite_code,
    )

class AcceptInvitationSchema(Schema):
  invite_code: str
  password: str


# ====================================================================================================
