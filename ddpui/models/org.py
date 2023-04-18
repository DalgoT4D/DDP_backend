from django.db import models
from ninja import Schema


class OrgDbt(models.Model):
    """Docstring"""

    gitrepo_url = models.CharField(max_length=100)
    project_dir = models.CharField(max_length=200)
    dbt_version = models.CharField(max_length=10)
    target_name = models.CharField(max_length=10)
    target_type = models.CharField(max_length=10)
    target_schema = models.CharField(max_length=10)

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
    block_type = models.CharField(max_length=25)  # all dbt blocks have the same type!
    block_id = models.CharField(max_length=36, unique=True)
    block_name = models.CharField(
        max_length=100, unique=True
    )  # use blockname to distinguish between different dbt commands
    display_name = models.CharField(max_length=100, null=True)

    def __str__(self) -> str:
        return f"{self.org.name} {self.block_type} {self.block_name}"


class OrgSchema(Schema):
    """Docstring"""

    name: str
    airbyte_workspace_id: str = None
