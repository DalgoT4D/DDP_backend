"""
New unified canvas models for UI4T architecture cleanup
These models will eventually replace OrgDbtModel, OrgDbtOperation, and DbtEdge
"""

import uuid
from enum import Enum
from django.db import models
from django.utils import timezone

from ddpui.models.dbt_workflow import OrgDbtModel
from ddpui.models.org import OrgDbt


class CanvasNodeType(str, Enum):
    """Node types for the unified canvas model"""

    SOURCE = "source"
    MODEL = "model"
    OPERATION = "operation"

    @classmethod
    def choices(cls):
        """Django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class CanvasNode(models.Model):
    """
    Unified node model for transform canvas.
    This replaces both OrgDbtModel and OrgDbtOperation with a single unified approach.
    """

    # Core properties
    orgdbt = models.ForeignKey(OrgDbt, on_delete=models.CASCADE)
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    node_type = models.CharField(choices=CanvasNodeType.choices(), max_length=20)
    name = models.CharField(max_length=300)

    # Type-specific data stored as JSON for flexibility
    operation_config = models.JSONField(default=dict)  # For OPERATION nodes

    # Common metadata
    output_cols = models.JSONField(default=list)

    # config for source/models
    dbtmodel = models.ForeignKey(
        OrgDbtModel, on_delete=models.SET_NULL, null=True
    )  # For MODEL/SOURCE nodes

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"CanvasNode[{self.node_type} | {self.name} | {self.orgdbt.project_dir}]"

    def save(self, *args, **kwargs):
        """Validate that only appropriate config is set based on node type"""
        if self.node_type in [CanvasNodeType.SOURCE, CanvasNodeType.MODEL]:
            # Clear operation config for model nodes
            if self.operation_config:
                self.operation_config = {}
        super().save(*args, **kwargs)

    @property
    def operation_type(self):
        """Get operation type for OPERATION nodes"""
        if self.node_type == CanvasNodeType.OPERATION:
            return self.operation_config.get("operation_type")
        return None


class CanvasEdge(models.Model):
    """
    Edges between canvas nodes.
    This replaces DbtEdge with a more flexible approach that works between any node types.
    Using same related_name as old DbtEdge for compatibility.
    """

    from_node = models.ForeignKey(
        CanvasNode,
        on_delete=models.CASCADE,
        related_name="from_node",  # Same as old DbtEdge
    )
    to_node = models.ForeignKey(
        CanvasNode,
        on_delete=models.CASCADE,
        related_name="to_node",  # Same as old DbtEdge
    )
    seq = models.IntegerField(default=1)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        # Ensure no duplicate edges
        unique_together = ["from_node", "to_node"]
        # Add index for better query performance
        indexes = [
            models.Index(fields=["from_node"]),
            models.Index(fields=["to_node"]),
        ]

    def __str__(self) -> str:
        return f"CanvasEdge[{self.from_node.name} -> {self.to_node.name}]"
