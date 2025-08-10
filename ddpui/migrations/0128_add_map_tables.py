# Generated migration for Maps feature

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0127_org_dalgouser_superset_creds_key_chart"),
    ]

    operations = [
        migrations.CreateModel(
            name="MapLayer",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("country_code", models.CharField(max_length=3)),
                ("country_name", models.CharField(max_length=100)),
                ("layer_level", models.IntegerField()),
                ("layer_name", models.CharField(max_length=100)),
                ("display_name", models.CharField(max_length=100)),
                ("parent_layer_id", models.IntegerField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "map_layer",
            },
        ),
        migrations.CreateModel(
            name="GeoJSON",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("layer_id", models.IntegerField()),
                ("name", models.CharField(max_length=255)),
                ("display_name", models.CharField(max_length=255)),
                ("is_default", models.BooleanField(default=False)),
                ("geojson_data", models.JSONField()),
                ("properties_key", models.CharField(max_length=100)),
                ("file_size", models.IntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "org",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        to="ddpui.org",
                    ),
                ),
            ],
            options={
                "db_table": "geojson",
            },
        ),
        migrations.AddIndex(
            model_name="geojson",
            index=models.Index(fields=["layer_id", "org"], name="ddpui_geojso_layer_i_a85c17_idx"),
        ),
        migrations.AddIndex(
            model_name="maplayer",
            index=models.Index(
                fields=["country_code", "layer_level"], name="ddpui_maplaye_country_28cd98_idx"
            ),
        ),
    ]
