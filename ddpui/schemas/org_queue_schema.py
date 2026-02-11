from typing import Optional
from ninja import Schema


class QueueDetailsSchema(Schema):
    """Individual queue configuration with name and workpool"""

    name: str
    workpool: str


class QueueConfigSchema(Schema):
    """Queue configuration with nested structure"""

    scheduled_pipeline_queue: QueueDetailsSchema
    connection_sync_queue: QueueDetailsSchema
    transform_task_queue: QueueDetailsSchema


class QueueConfigUpdateSchema(Schema):
    """Schema for updating queue configuration with nested structure - all fields optional"""

    scheduled_pipeline_queue: Optional[QueueDetailsSchema] = None
    connection_sync_queue: Optional[QueueDetailsSchema] = None
    transform_task_queue: Optional[QueueDetailsSchema] = None
