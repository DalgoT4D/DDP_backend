# Generated migration for Maps feature - Phase One

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("ddpui", "0128_add_map_tables"),
    ]

    operations = [
        # Drop old tables first
        migrations.RunSQL(
            "DROP TABLE IF EXISTS geojson CASCADE;",
            reverse_sql="",
        ),
        migrations.RunSQL(
            "DROP TABLE IF EXISTS map_layer CASCADE;",
            reverse_sql="",
        ),
        # Create new tables as per specification
        migrations.CreateModel(
            name="GeoRegion",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=255)),
                ("type", models.CharField(max_length=50)),
                ("parent_id", models.BigIntegerField(blank=True, null=True)),
                ("country_code", models.CharField(max_length=10)),
                ("region_code", models.CharField(max_length=10)),
                ("display_name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "ddpui_georegion",
            },
        ),
        migrations.CreateModel(
            name="GeoJSON",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("region_id", models.BigIntegerField()),
                ("geojson_data", models.JSONField()),
                ("properties_key", models.CharField(max_length=100)),
                ("is_default", models.BooleanField(default=False)),
                ("org_id", models.IntegerField(blank=True, null=True)),
                ("version_name", models.CharField(max_length=100)),
                ("description", models.TextField(blank=True, null=True)),
                ("file_size", models.IntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "ddpui_geojson",
            },
        ),
        # Add foreign key constraint for parent_id
        migrations.RunSQL(
            """
            ALTER TABLE ddpui_georegion 
            ADD CONSTRAINT ddpui_georegion_parent_id_fkey 
            FOREIGN KEY (parent_id) REFERENCES ddpui_georegion(id) 
            ON DELETE CASCADE;
            """,
            reverse_sql="ALTER TABLE ddpui_georegion DROP CONSTRAINT IF EXISTS ddpui_georegion_parent_id_fkey;",
        ),
        # Add indexes
        migrations.AddIndex(
            model_name="georegion",
            index=models.Index(
                fields=["country_code", "type"], name="ddpui_georegion_country_type_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="georegion",
            index=models.Index(fields=["parent_id"], name="ddpui_georegion_parent_idx"),
        ),
        migrations.AddIndex(
            model_name="geojson",
            index=models.Index(fields=["region_id", "org_id"], name="ddpui_geojson_region_org_idx"),
        ),
    ]
