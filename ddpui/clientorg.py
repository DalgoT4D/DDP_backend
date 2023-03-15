from django.db import models
from ninja import ModelSchema, Schema

# ====================================================================================================
class ClientOrg(models.Model):
  name = models.CharField(max_length=50)
  airbyte_workspace_id = models.CharField(max_length=36, null=True)
  dbt_repo_url = models.CharField(max_length=100, null=True)

class ClientOrgSchema(Schema):
  name: str
  airbyte_workspace_id: str = None
  dbt_repo_url: str = None
  