""" track sync times, number of records, volume of data by client and connection """

from django.db import models
from ddpui.models.org import Org


class SyncStats(models.Model):
    """single table to track connection sync stats"""

    org = models.ForeignKey(Org, on_delete=models.CASCADE)
    connection_id = models.CharField(max_length=36)
    attempt = models.IntegerField(default=0)
    status = models.TextField()
    sync_type = models.CharField(
        choices=[("manual", "manual"), ("orchestrate", "orchestrate")]
    )
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
