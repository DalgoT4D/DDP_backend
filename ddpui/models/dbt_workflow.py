"""
All the models related to UI for transformation will go here
"""

from django.db import models

from ddpui.models.org import OrgDbt


class OrgDbtModel(models.Model):
    """Docstring"""

    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    display_name = models.CharField(max_length=100)
    sql_path = models.CharField(max_length=200, null=True)
    config = models.JSONField(null=True)

    def __str__(self) -> str:
        return f"DbtModel[{self.name} | {self.orgdbt.project_dir}]"
