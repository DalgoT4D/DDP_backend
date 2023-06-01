from django.db import models
from ninja import Schema


class OrgDbt(models.Model):
    """Docstring"""

    gitrepo_url = models.CharField(max_length=100)
    gitrepo_access_token_secret = models.CharField(max_length=100, null=True)

    project_dir = models.CharField(max_length=200)
    dbt_version = models.CharField(max_length=10)

    target_type = models.CharField(max_length=10)
    default_schema = models.CharField(max_length=50)


class Org(models.Model):
    """Docstring"""

    name = models.CharField(max_length=50)
    slug = models.CharField(max_length=20, null=True)
    airbyte_workspace_id = models.CharField(max_length=36, null=True)
    dbt = models.ForeignKey(OrgDbt, on_delete=models.SET_NULL, null=True)


class OrgPrefectBlock(models.Model):
    """Docstring"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    block_type = models.CharField(max_length=25)  # all dbt blocks have the same type!
    block_id = models.CharField(max_length=36, unique=True)
    block_name = models.CharField(
        max_length=100, unique=True
    )  # use blockname to distinguish between different dbt commands
    display_name = models.CharField(max_length=100, null=True)
    seq = models.SmallIntegerField(null=True)

    def __str__(self) -> str:
        return f"{self.org.name} {self.block_type} {self.block_name}"


class OrgDataFlow(models.Model):
    """This contains the deployment id of an organization to schedule flows/pipelines"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    deployment_name = models.CharField(max_length=100, null=True)
    deployment_id = models.CharField(max_length=36, unique=True, null=True)
    cron = models.CharField(max_length=36, null=True)
    # and if the deployment is a manual airbyte-connection-sync, then we store the conn_id
    connection_id = models.CharField(max_length=36, unique=True, null=True)


class OrgSchema(Schema):
    """Docstring"""

    name: str
    airbyte_workspace_id: str = None


class OrgWarehouse(models.Model):
    """A data warehouse for an org. Typically we expect exactly one"""

    wtype = models.CharField(max_length=25)  # postgres, bigquery
    credentials = models.CharField(max_length=200)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    airbyte_destination_id = models.TextField(max_length=36, null=True)
    airbyte_norm_op_id = models.TextField(max_length=36, null=True)


class OrgWarehouseSchema(Schema):
    """payload to register an organization's data warehouse"""

    wtype: str
    destinationDefId: str
    airbyteConfig: dict
