from django.db import models

# from ninja import ModelSchema, Schema
from ddpui.models.org import Org


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
