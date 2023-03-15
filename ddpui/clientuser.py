from django.db import models
from datetime import datetime
from ninja import ModelSchema, Schema

from .clientorg import ClientOrg, ClientOrgSchema

# ====================================================================================================
class ClientUser(models.Model):
  active = models.BooleanField(default=True)
  email = models.CharField(max_length=50, null=True, unique=True)
  clientorg = models.ForeignKey(ClientOrg, on_delete=models.CASCADE, null=True)

  def __str__(self):
    return self.email

class ClientUserCreate(ModelSchema):
  class Config:
    model = ClientUser
    model_fields = ['email']

class ClientUserUpdate(Schema):
  email: str = None
  active: bool = None

class ClientUserResponse(Schema):
  email: str
  clientorg: ClientOrgSchema = None
  active: bool

  @staticmethod
  def from_clientuser(clientuser: ClientUser):
    return ClientUserResponse(email=clientuser.email, clientorg=clientuser.clientorg, active=clientuser.active)

# ====================================================================================================
class Invitation(models.Model):
  invited_email = models.CharField(max_length=50)
  invited_by = models.ForeignKey(ClientUser, on_delete=models.CASCADE)
  invited_on = models.DateTimeField()
  invite_code = models.CharField(max_length=36)

class InvitationSchema(Schema):
  invited_email: str
  invited_by: ClientUserResponse = None
  invited_on: datetime = None
  invite_code: str = None

  @staticmethod
  def from_invitation(invitation: Invitation):
    return InvitationSchema(
      invited_email=invitation.invited_email,
      invited_by=ClientUserResponse.from_clientuser(invitation.invited_by),
      invited_on=invitation.invited_on,
      invite_code=invitation.invite_code,
    )

class AcceptInvitationSchema(Schema):
  invite_code: str
  password: str


# ====================================================================================================
