"""track sync times, number of records, volume of data by client and connection"""

from django.db import models
from ddpui.models.org import Org


class SyncStats(models.Model):
    """single table to track connection sync stats"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    connection_id = models.CharField(max_length=36)
    job_id = models.IntegerField(null=True)
    attempt = models.IntegerField(default=0)
    status = models.TextField()
    sync_type = models.CharField(choices=[("manual", "manual"), ("orchestrate", "orchestrate")])
    sync_time = models.DateTimeField()
    sync_duration_s = models.BigIntegerField(default=0)
    sync_records = models.BigIntegerField(default=0)
    sync_data_volume_b = models.BigIntegerField(default=0)

    def __str__(self) -> str:
        return f"SyncStats[{self.org.name}|{self.connection_id}]"

    def to_json(self) -> dict:
        return {
            "org": self.org.slug,
            "connection_id": self.connection_id,
            "attempt": self.attempt,
            "status": self.status,
            "sync_time": self.sync_time,
            "sync_duration_s": self.sync_duration_s,
            "sync_records": self.sync_records,
            "sync_data_volume_b": self.sync_data_volume_b,
        }


class AirbyteJob(models.Model):
    """model to track airbyte jobs"""

    job_id = models.BigIntegerField(primary_key=True, null=False)
    job_type = models.CharField(
        max_length=20
    )  # check_connection_source┃check_connection_destination┃discover_schema┃get_spec┃sync┃reset_connection┃refresh┃clear
    config_id = models.CharField(max_length=100)  # connection_id, source_id, destination_id
    status = models.CharField(
        max_length=20
    )  # pending┃running┃incomplete┃failed┃succeeded┃cancelled
    reset_config = models.JSONField(
        null=True
    )  # contains information about how a reset was configured. only populated if the job was a reset.
    refresh_config = models.JSONField(
        null=True
    )  # contains information about how a refresh was configured. only populated if the job was a refresh.
    stream_stats = models.JSONField(
        null=True
    )  # contains information about the streams processed by the job
    records_emitted = models.BigIntegerField(default=0)
    bytes_emitted = models.BigIntegerField(default=0)
    records_committed = models.BigIntegerField(default=0)
    bytes_committed = models.BigIntegerField(default=0)

    attempts = models.JSONField(
        null=True
    )  # contains information about the attempts made for this job. only populated if the job has attempts.

    started_at = models.DateTimeField(null=True)  # because the api spec says this will be optional
    ended_at = models.DateTimeField()  # when the job ended
    created_at = models.DateTimeField()  # when the job was created in airbyte
    updated_at = models.DateTimeField(auto_now=True)  # when the django record was last updated

    def __str__(self) -> str:
        return f"AirbyteJob[Job ID: {self.job_id}|Job Type: {self.job_type}|Status: {self.status}]"

    @property
    def duration(self):
        """Returns duration in seconds between created_at and ended_at, or None if either is missing."""
        if self.created_at and self.ended_at:
            delta = self.ended_at - self.created_at
            return int(delta.total_seconds())
        return None

    @property
    def attempt_duration(self):
        """Returns duration in seconds between started_at and ended_at, or None if either is missing."""
        if self.started_at and self.ended_at:
            delta = self.ended_at - self.started_at
            return int(delta.total_seconds())
        return None
