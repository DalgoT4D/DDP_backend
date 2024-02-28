"""
All the models related to UI for transformation will go here
"""

from enum import Enum
from django.db import models

from ddpui.models.org import OrgDbt


class OrgDbtModelType(str, Enum):
    """an enum for roles assignable to org-users"""

    SOURCE = "source"
    MODEL = "model"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class OrgDbtModel(models.Model):
    """Model to store dbt models/sources in a project"""

    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    uuid = models.UUIDField(editable=False, unique=True, null=True)
    name = models.CharField(max_length=100)
    display_name = models.CharField(max_length=100)
    schema = models.CharField(max_length=100, null=True)
    sql_path = models.CharField(max_length=200, null=True)
    type = models.CharField(
        choices=OrgDbtModelType.choices(), max_length=50, default="model"
    )
    source_name = models.CharField(max_length=100, null=True)
    output_cols = models.JSONField(default=list)

    def __str__(self) -> str:
        return f"DbtModel[{self.type} | {self.schema}.{self.name} | {self.orgdbt.project_dir}]"


class OrgDbtOperation(models.Model):
    """Model to store dbt operations for a model. Basically steps to create/reach a OrgDbtModel"""

    dbtmodel = models.ForeignKey(OrgDbtModel, on_delete=models.CASCADE)
    uuid = models.UUIDField(editable=False, unique=True)
    seq = models.IntegerField(default=0)
    output_cols = models.JSONField(default=list)
    config = models.JSONField(null=True)

    def __str__(self) -> str:
        return (
            f"DbtOperation[{self.uuid} | {self.dbtmodel.schema}.{self.dbtmodel.name}]"
        )


class DbtEdge(models.Model):
    """Edge to help generate the DAG of a dbt project. Edge is between two OrgDbtModel(s)"""

    from_node = models.ForeignKey(
        OrgDbtModel, on_delete=models.CASCADE, related_name="from_node", default=None
    )
    to_node = models.ForeignKey(
        OrgDbtModel, on_delete=models.CASCADE, related_name="to_node", default=None
    )

    def __str__(self) -> str:
        return f"DbtEdge[{self.from_node.schema}.{self.from_node.name} -> {self.to_node.schema}.{self.to_node.name}]"
