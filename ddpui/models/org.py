from django.db import models
from ninja import Schema


class OrgDbt(models.Model):
    """Docstring"""

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
    password = models.CharField(max_length=100)  # encrypted
    database = models.CharField(max_length=50)


class Org(models.Model):
    """Docstring"""

    name = models.CharField(max_length=50)
    slug = models.CharField(max_length=20, null=True)
    airbyte_workspace_id = models.CharField(max_length=36, null=True)
    dbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE, null=True)


class OrgPrefectBlock(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    blocktype = models.CharField(max_length=25)  # all dbt blocks have the same type!
    blockid = models.CharField(max_length=36, unique=True)
    blockname = models.CharField(
        max_length=100, unique=True
    )  # use blockname to distinguish between different dbt commands
    displayname = models.CharField(max_length=100, null=True)

    def __str__(self) -> str:
        return f"{self.org.name} {self.blocktype} {self.blockname}"


class OrgSchema(Schema):
    """Docstring"""

    name: str
    airbyte_workspace_id: str = None
