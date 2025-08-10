"""GeoJSON models for storing geographic data"""

from django.db import models
from ddpui.models.org import Org


class GeoJSON(models.Model):
    """Stores GeoJSON data"""

    id = models.BigAutoField(primary_key=True)
    layer_id = models.IntegerField()  # References map_layer(id)
    name = models.CharField(max_length=255)  # 'india_states', 'maharashtra_districts'
    display_name = models.CharField(max_length=255)  # 'India - States', 'Maharashtra - Districts'
    is_default = models.BooleanField(default=False)  # True for system-provided GeoJSONs
    geojson_data = models.JSONField()  # The actual GeoJSON
    properties_key = models.CharField(max_length=100)  # Which property contains the region name
    file_size = models.IntegerField()
    org = models.ForeignKey(
        Org, on_delete=models.CASCADE, null=True, blank=True
    )  # NULL for default GeoJSONs
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "geojson"
        indexes = [
            models.Index(fields=["layer_id", "org"]),
        ]

    def __str__(self):
        return f"{self.display_name} ({self.name})"
