import os

from enum import Enum
from django.db import models
from django.utils import timezone

from ddpui.ddpprefect import DDP_WORK_QUEUE, MANUL_DBT_WORK_QUEUE
from ddpui.schemas.org_queue_schema import (
    QueueConfigSchema,
    QueueDetailsSchema,
    QueueConfigUpdateSchema,
)


class OrgType(str, Enum):
    """an enum representing the type of organization"""

    SUBSCRIPTION = "subscription"
    TRIAL = "trial"
    DEMO = "demo"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class OrgVizLoginType(str, Enum):
    """an enum for roles assignable to org-users"""

    BASIC_AUTH = "basic"
    GOOGLE_AUTH = "google"

    @classmethod
    def choices(cls):
        """django model definition needs an iterable for `choices`"""
        return [(key.value, key.name) for key in cls]


class TransformType(str, Enum):
    """an enum for transform type available either via ui or github"""

    UI = "ui"
    GIT = "github"


def get_default_queue_config():
    """Returns the new nested structure as default"""
    default_workpool = os.getenv("PREFECT_WORKER_POOL_NAME") or "default"
    return {
        "scheduled_pipeline_queue": {"name": DDP_WORK_QUEUE, "workpool": default_workpool},
        "connection_sync_queue": {"name": DDP_WORK_QUEUE, "workpool": default_workpool},
        "transform_task_queue": {"name": MANUL_DBT_WORK_QUEUE, "workpool": default_workpool},
    }


class OrgDbt(models.Model):
    """Docstring"""

    gitrepo_url = models.CharField(max_length=100, null=True)
    gitrepo_access_token_secret = models.CharField(
        max_length=100, null=True
    )  # skipcq: PTC-W0901, PTC-W0906

    project_dir = models.CharField(max_length=200)
    dbt_venv = models.CharField(max_length=200, null=True)

    target_type = models.CharField(max_length=10)
    default_schema = models.CharField(max_length=50)
    transform_type = models.CharField(max_length=10, null=True)
    branch_name = models.CharField(max_length=256, null=True)
    is_default_branch = models.BooleanField(null=True)
    cli_profile_block = models.ForeignKey(
        "ddpui.OrgPrefectBlockv1",
        on_delete=models.SET_NULL,
        null=True,
    )
    dbtcloud_creds_block = models.ForeignKey(
        "ddpui.OrgPrefectBlockv1",
        on_delete=models.SET_NULL,
        null=True,
        related_name="dbtcloud_creds_block",
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"OrgDbt[{self.gitrepo_url}|{self.target_type}|{self.default_schema}|{self.transform_type}]"


class Org(models.Model):
    """Docstring"""

    name = models.CharField(max_length=50)
    slug = models.CharField(max_length=20, null=True)  # skipcq: PTC-W0901, PTC-W0906
    airbyte_workspace_id = models.CharField(  # skipcq: PTC-W0901, PTC-W0906
        max_length=36, null=True
    )
    dbt = models.ForeignKey(  # skipcq: PTC-W0901, PTC-W0906
        OrgDbt, on_delete=models.SET_NULL, null=True
    )
    viz_url = models.CharField(max_length=100, null=True)
    viz_login_type = models.CharField(choices=OrgVizLoginType.choices(), max_length=50, null=True)
    dalgouser_superset_creds_key = models.TextField(null=True)
    website = models.CharField(max_length=1000, null=True)
    queue_config = models.JSONField(
        default=get_default_queue_config,
        help_text="Queue configuration for different task types (scheduled_pipeline_queue, connection_sync_queue, transform_task_queue)",
    )
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Org[{self.slug}|{self.name}|{self.airbyte_workspace_id}]"

    def get_queue_config(self) -> QueueConfigSchema:
        """Returns validated queue config with automatic defaults - supports both formats"""
        stored = self.queue_config or {}

        # Get defaults from the function that uses environment variables
        default_config = get_default_queue_config()

        def get_queue_details(key: str) -> QueueDetailsSchema:
            if key not in stored:
                # Use default from function
                default_data = default_config[key]
                return QueueDetailsSchema(
                    name=default_data["name"], workpool=default_data["workpool"]
                )

            queue_data = stored[key]

            # Handle new nested format
            if isinstance(queue_data, dict) and "name" in queue_data:
                workpool = queue_data.get("workpool")
                if workpool is None:
                    # Use the default workpool from the default config
                    default_data = default_config[key]
                    workpool = default_data["workpool"]
                return QueueDetailsSchema(name=queue_data.get("name"), workpool=workpool)
            # Handle legacy flat format (for backward compatibility during migration)
            elif isinstance(queue_data, str):
                # Use the default workpool from the default config
                default_data = default_config[key]
                workpool = default_data["workpool"]
                return QueueDetailsSchema(name=queue_data, workpool=workpool)

            # Fallback to defaults
            default_data = default_config[key]
            return QueueDetailsSchema(name=default_data["name"], workpool=default_data["workpool"])

        return QueueConfigSchema(
            scheduled_pipeline_queue=get_queue_details("scheduled_pipeline_queue"),
            connection_sync_queue=get_queue_details("connection_sync_queue"),
            transform_task_queue=get_queue_details("transform_task_queue"),
        )

    def update_queue_config(self, update_data: QueueConfigUpdateSchema):
        """Update queue config with nested data"""
        current = self.get_queue_config()

        if update_data.scheduled_pipeline_queue is not None:
            current.scheduled_pipeline_queue = update_data.scheduled_pipeline_queue
        if update_data.connection_sync_queue is not None:
            current.connection_sync_queue = update_data.connection_sync_queue
        if update_data.transform_task_queue is not None:
            current.transform_task_queue = update_data.transform_task_queue

        # Store as nested structure
        self.queue_config = current.dict()
        self.save()

    def base_plan(self):
        """returns the base plan of the organization"""
        return self.org_plans.base_plan if hasattr(self, "org_plans") else None


class OrgWarehouse(models.Model):
    """A data warehouse for an org. Typically we expect exactly one"""

    wtype = models.CharField(max_length=25)  # postgres, bigquery, snowflake
    name = models.CharField(max_length=256, default="", blank=True)
    credentials = models.CharField(max_length=1000)
    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    airbyte_destination_id = models.TextField(  # skipcq: PTC-W0901, PTC-W0906
        max_length=36, null=True
    )
    airbyte_docker_repository = models.TextField(  # skipcq: PTC-W0901, PTC-W0906
        max_length=100, null=True
    )
    airbyte_docker_image_tag = models.TextField(  # skipcq: PTC-W0901, PTC-W0906
        max_length=10, null=True
    )
    bq_location = models.CharField(max_length=100, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"OrgWarehouse[{self.org.slug}|{self.wtype}|{self.airbyte_destination_id}]"


# ============================================================================================
# new models to go away from blocks architecture


class OrgPrefectBlockv1(models.Model):
    """This containes the update version of the orgprefectblock model"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    block_type = models.CharField(max_length=25)  # all dbt blocks have the same type!
    block_id = models.CharField(max_length=36, unique=True)
    block_name = models.CharField(
        max_length=100, unique=True
    )  # use blockname to distinguish between different dbt commands
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"OrgPrefectBlockv1[{self.org.name}|{self.block_type}|{self.block_name}]"


class OrgDataFlowv1(models.Model):
    """This contains the deployment id of an organization to schedule flows/pipelines"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    deployment_name = models.CharField(max_length=100, null=True)  # skipcq: PTC-W0901, PTC-W0906
    deployment_id = models.CharField(  # skipcq: PTC-W0901, PTC-W0906
        max_length=36, unique=True, null=True
    )
    cron = models.CharField(max_length=36, null=True)  # skipcq: PTC-W0901, PTC-W0906

    dataflow_type = models.CharField(
        max_length=25,
        choices=(("orchestrate", "orchestrate"), ("manual", "manual")),
        default="orchestrate",
    )  # skipcq: PTC-W0901, PTC-W0906

    reset_conn_dataflow = models.ForeignKey("self", on_delete=models.SET_NULL, null=True)
    clear_conn_dataflow = models.ForeignKey(
        "self", on_delete=models.SET_NULL, null=True, related_name="clear_connection_dataflow"
    )
    meta = models.JSONField(null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"OrgDataFlowv1[{self.name}|{self.deployment_name}|{self.deployment_id}|{self.cron}]"


class ConnectionMeta(models.Model):
    """A model to store details about the airbyte connection
    like no of rows in staging table,
    whether the reset is big enough for scheduling it later"""

    connection_id = models.CharField(max_length=36, null=False)
    connection_name = models.CharField(max_length=100, null=True)


class ConnectionJob(models.Model):
    """
    All the large jobs (reset or update schema) scheduled by the system are stored here
    - ddpui.utils.constants.UPDATE_SCHEMA
    - ddpui.utils.constants.TASK_AIRBYTERESET
    """

    connection_id = models.CharField(max_length=36, null=False)
    job_type = models.CharField(max_length=36, null=False)
    scheduled_at = models.DateTimeField(null=False)
    flow_run_id = models.CharField(max_length=36, null=False)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)


class OrgSchemaChange(models.Model):
    """This contains the deployment id of an organization to schedule flows/pipelines"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    connection_id = models.CharField(max_length=36, unique=True, null=True)
    change_type = models.CharField(max_length=36, null=True)
    created_at = models.DateTimeField(auto_created=True, default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    schedule_job = models.ForeignKey(ConnectionJob, null=True, on_delete=models.SET_NULL)


class OrgFeatureFlag(models.Model):
    """A model to store feature flags enabled for an org; org specific flags override global flags"""

    org = models.ForeignKey(
        Org, on_delete=models.CASCADE, null=True
    )  # empty or null org means global flag
    flag_name = models.CharField(max_length=100, null=False)
    flag_value = models.BooleanField(null=False)

    class Meta:
        unique_together = [["org", "flag_name"]]
        indexes = [
            models.Index(fields=["org", "flag_name"]),
            # Unique partial index for global flags (where org is NULL)
            models.Index(
                fields=["flag_name"],
                name="unique_global_flag",
                condition=models.Q(org__isnull=True),
            ),
        ]
        constraints = [
            # Unique constraint for global flags (org=NULL)
            models.UniqueConstraint(
                fields=["flag_name"],
                condition=models.Q(org__isnull=True),
                name="unique_global_feature_flag",
            ),
        ]

    def __str__(self) -> str:
        org_display = self.org.slug if self.org else "GLOBAL"
        return f"OrgFeatureFlag[{org_display}|{self.flag_name}={self.flag_value}]"
