"""GeoRegion model for storing geographic hierarchy"""

from django.db import models


class GeoRegion(models.Model):
    """Stores geographic region hierarchy as per specification"""

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255)  # e.g., 'Maharashtra', 'Mumbai'
    type = models.CharField(max_length=50)  # e.g., 'state', 'district', 'ward'
    parent = models.ForeignKey(
        "self", on_delete=models.CASCADE, null=True, blank=True, related_name="children"
    )
    country_code = models.CharField(max_length=10)  # e.g., 'IND', 'KEN'
    region_code = models.CharField(max_length=10)  # e.g., 'MH' for Maharashtra
    display_name = models.CharField(max_length=255)  # User-friendly name
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ddpui_georegion"
        indexes = [
            models.Index(fields=["country_code", "type"]),
            models.Index(fields=["parent_id"]),
        ]

    def __str__(self):
        return f"{self.display_name} ({self.type})"
