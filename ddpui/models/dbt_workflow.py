"""
All the models related to UI for transformation will go here
"""

from django.db import models

from ddpui.models.org import OrgDbt


class OrgDbtModel(models.Model):
    """Model to store dbt models/sources in a project"""

    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    display_name = models.CharField(max_length=100)
    schema = models.CharField(max_length=100, null=True)
    sql_path = models.CharField(max_length=200, null=True)

    def __str__(self) -> str:
        return f"DbtModel[{self.schema}.{self.name} | {self.orgdbt.project_dir}]"


class DbtEdge(models.Model):
    """Edge to help generate the DAG of a dbt project. Edge is between two OrgDbtModel(s)"""

    source = models.ForeignKey(
        OrgDbtModel, on_delete=models.CASCADE, related_name="source"
    )
    target = models.ForeignKey(
        OrgDbtModel, on_delete=models.CASCADE, related_name="target"
    )
    config = models.JSONField(null=True)

    def __str__(self) -> str:
        return f"DbtEdge[{self.source.schema}.{self.source.name} -> {self.target.schema}.{self.target.name}]"
