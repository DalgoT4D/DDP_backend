"""
All the models related to UI for transformation will go here
"""

import uuid
from ninja import Schema
from enum import Enum
from django.db import models
from django.utils import timezone

from ddpui.models.org import OrgDbt
from ddpui.models.dbt_workflow import OrgDbtModelType


class EdgeConfig(Schema):
    source_columns: list[str]
    seq: int = 0


class OrgDbtModelv1(models.Model):
    """Model to store dbt models/sources in a project"""

    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    uuid = models.UUIDField(editable=False, unique=True)
    name = models.CharField(max_length=300, null=True)
    display_name = models.CharField(max_length=300, null=True)
    schema = models.CharField(max_length=300, null=True)
    sql_path = models.CharField(max_length=300, null=True)
    type = models.CharField(choices=OrgDbtModelType.choices(), max_length=50)
    source_name = models.CharField(max_length=300, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"DbtModel[{self.type} | {self.schema}.{self.name}]"


class DbtOperation(models.Model):
    """Model to store dbt operations for a model. Basically steps to create/reach a OrgDbtModel"""

    uuid = models.UUIDField(editable=False, unique=True)
    op_type = models.CharField(max_length=100)
    config = models.JSONField(null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"DbtOperation[{self.uuid} | {self.op_type}]"


class DbtNode(models.Model):
    """A generic node for models or operations in a dbt project"""

    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    uuid = models.UUIDField(editable=False, unique=True)
    # only one of these will be set
    dbtoperation = models.ForeignKey(
        DbtOperation,
        on_delete=models.CASCADE,
        null=True,
        related_name="operation",
    )
    dbtmodel = models.ForeignKey(
        OrgDbtModelv1,
        on_delete=models.CASCADE,
        null=True,
        related_name="model",
    )
    output_cols = models.JSONField(default=list)


class DbtEdgev1(models.Model):
    """Edge to join two Canvas nodes of a dbt project"""

    uuid = models.UUIDField(editable=False, unique=True)
    from_node = models.ForeignKey(
        DbtNode,
        on_delete=models.CASCADE,
        related_name="from_node",
    )
    to_node = models.ForeignKey(
        DbtNode,
        on_delete=models.CASCADE,
        related_name="to_node",
    )

    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"DbtEdge[{self.from_node.dbtoperation.op_type or self.from_node.dbtmodel.name} -> {self.to_node.dbtoperation.op_type or self.to_node.dbtmodel.name}]"
