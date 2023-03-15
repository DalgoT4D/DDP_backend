from django.db import models
from ninja import ModelSchema, Schema

# ====================================================================================================
class AdminUser(models.Model):
  active = models.BooleanField(default=True)
  email = models.CharField(max_length=50, null=True, unique=True)

class AdminUserResponse(ModelSchema):
  class Config:
    model = AdminUser
    model_fields = ['email', 'active']

  # ====================================================================================================
