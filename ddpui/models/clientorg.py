from django.db import models
from ninja import ModelSchema, Schema

# ====================================================================================================
class ClientDbt(models.Model):
  gitrepo_url = models.CharField(max_length=100)
  project_dir = models.CharField(max_length=200)
  dbtversion = models.CharField(max_length=10)
  targetname = models.CharField(max_length=10)
  targettype = models.CharField(max_length=10)
  targetschema = models.CharField(max_length=10)

  # connection to target warehouse
  host = models.CharField(max_length=100)
  port = models.CharField(max_length=5)
  username = models.CharField(max_length=100)
  password = models.CharField(max_length=100) # encrypted
  database = models.CharField(max_length=50)

class ClientDbtSchema(ModelSchema):
  class Config:
    model = ClientDbt
    model_fields = [
      'gitrepo_url',
      # 'project_dir',
      'dbtversion',
      'targetname',
      'targettype',
      'targetschema',
      'host',
      'port',
      'username',
      'password',
      'database',
    ]

# ====================================================================================================
class ClientOrg(models.Model):
  name = models.CharField(max_length=50)
  slug = models.CharField(max_length=20, null=True)
  airbyte_workspace_id = models.CharField(max_length=36, null=True)
  dbt = models.ForeignKey(ClientDbt, on_delete=models.CASCADE, null=True)

class ClientOrgSchema(Schema):
  name: str
  airbyte_workspace_id: str = None
  