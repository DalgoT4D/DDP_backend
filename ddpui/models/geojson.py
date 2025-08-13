"""GeoJSON models for storing geographic data"""

from django.db import models
from ddpui.models.org import Org


class GeoJSON(models.Model):
    """Stores GeoJSON data as per specification"""

    id = models.BigAutoField(primary_key=True)
    region_id = models.BigIntegerField()  # References ddpui_georegion(id) - MANY TO ONE
    geojson_data = models.JSONField()  # The actual GeoJSON
    properties_key = models.CharField(max_length=100)  # Which property contains the region name
    is_default = models.BooleanField(default=False)  # True for system-provided GeoJSONs
    org_id = models.IntegerField(null=True, blank=True)  # References ddpui.org - NULL for defaults
    version_name = models.CharField(max_length=100)  # Version identifier
    description = models.TextField(null=True, blank=True)  # Optional description
    file_size = models.IntegerField()  # Size in bytes
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ddpui_geojson"
        indexes = [
            models.Index(fields=["region_id", "org_id"]),
        ]

    def __str__(self):
        return f"GeoJSON for region {self.region_id} ({self.version_name})"
