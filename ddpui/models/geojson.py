"""GeoJSON models for storing geographic data"""

from django.db import models
from ddpui.models.org import Org


class GeoJSON(models.Model):
    """Stores GeoJSON data as per specification"""

    id = models.BigAutoField(primary_key=True)
    region = models.ForeignKey(
        "ddpui.GeoRegion", on_delete=models.CASCADE, db_column="region_id", related_name="geojsons"
    )  # Foreign key to GeoRegion - ONE TO MANY
    geojson_data = models.JSONField()  # The actual GeoJSON
    properties_key = models.CharField(max_length=100)  # Which property contains the region name
    is_default = models.BooleanField(default=False)  # True for system-provided GeoJSONs
    org = models.ForeignKey(
        "ddpui.Org",
        on_delete=models.CASCADE,
        db_column="org_id",
        null=True,
        blank=True,
        related_name="geojsons",
    )  # Foreign key to Org - NULL for system defaults
    description = models.TextField(null=True, blank=True)  # Optional description
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ddpui_geojson"
        indexes = [
            models.Index(fields=["region_id", "org_id"]),
        ]

    def __str__(self):
        return f"GeoJSON for region {self.region.display_name}"
