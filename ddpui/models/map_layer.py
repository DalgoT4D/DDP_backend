"""Map Layer models for geographic data hierarchy"""

from django.db import models


class MapLayer(models.Model):
    """Stores geographic layers hierarchy"""

    id = models.BigAutoField(primary_key=True)
    country_code = models.CharField(max_length=3)  # 'IND', 'KEN'
    country_name = models.CharField(max_length=100)
    layer_level = models.IntegerField()  # 0=country, 1=state/county, 2=district, etc.
    layer_name = models.CharField(max_length=100)  # 'states', 'districts', 'blocks'
    display_name = models.CharField(max_length=100)  # 'Indian States', 'Kenya Counties'
    parent_layer_id = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "map_layer"
        indexes = [
            models.Index(fields=["country_code", "layer_level"]),
        ]

    def __str__(self):
        return f"{self.country_name} - {self.display_name}"
