"""
Django management command to seed GeoRegions and GeoJSONs data
Usage: python manage.py seed_map_data
"""

import json
from pathlib import Path
from django.core.management.base import BaseCommand
from django.db import transaction
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON


class Command(BaseCommand):
    help = "Seed GeoRegions and GeoJSONs data from seed/geojsons folder"

    def add_arguments(self, parser):
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Clear existing data before seeding",
        )
        parser.add_argument(
            "--georegions-only",
            action="store_true",
            help="Only seed GeoRegions, skip GeoJSONs",
        )
        parser.add_argument(
            "--geojsons-only",
            action="store_true",
            help="Only seed GeoJSONs, skip GeoRegions",
        )
        parser.add_argument(
            "--file",
            type=str,
            help="Process only a specific GeoJSON file",
        )

    def handle(self, *args, **options):
        self.stdout.write("🌍 MAP DATA SEEDER")
        self.stdout.write("=" * 50)

        try:
            with transaction.atomic():
                if options["clear"]:
                    self.clear_data()

                if not options["geojsons_only"]:
                    self.seed_georegions()

                if not options["georegions_only"]:
                    self.seed_geojsons(options.get("file"))

                self.stdout.write(self.style.SUCCESS("\n✅ Seeding completed!"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {e}"))
            raise

    def clear_data(self):
        """Clear existing data"""
        geojson_count = GeoJSON.objects.count()
        georegion_count = GeoRegion.objects.count()

        GeoJSON.objects.all().delete()
        GeoRegion.objects.all().delete()

        self.stdout.write(f"🗑️  Cleared {geojson_count} GeoJSONs, {georegion_count} GeoRegions")

    def seed_georegions(self):
        """Seed GeoRegions from georegions_seed.json"""
        base_dir = Path(__file__).parent.parent.parent.parent
        seed_file = base_dir / "seed" / "geojsons" / "georegions_seed.json"

        if not seed_file.exists():
            self.stdout.write(self.style.WARNING(f"⚠️  Seed file not found: {seed_file}"))
            return

        with open(seed_file, "r") as f:
            data = json.load(f)

        georegions = data.get("georegions", [])
        created_count = 0
        updated_count = 0
        parent_relationships = {}

        # First pass: Create regions
        for region_data in georegions:
            region_id = region_data["id"]
            parent_id = region_data.get("parent_id")

            if parent_id:
                parent_relationships[region_id] = parent_id

            region, created = GeoRegion.objects.update_or_create(
                id=region_id,
                defaults={
                    "name": region_data["name"],
                    "type": region_data["type"],
                    "country_code": region_data["country_code"],
                    "region_code": region_data["region_code"],
                    "display_name": region_data["display_name"],
                },
            )

            if created:
                created_count += 1
            else:
                updated_count += 1

        # Second pass: Set parent relationships
        for region_id, parent_id in parent_relationships.items():
            try:
                region = GeoRegion.objects.get(id=region_id)
                parent = GeoRegion.objects.get(id=parent_id)
                region.parent = parent
                region.save(update_fields=["parent"])
            except GeoRegion.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f"Failed to link region {region_id} to parent {parent_id}")
                )

        self.stdout.write(f"📍 GeoRegions: {created_count} created, {updated_count} updated")

    def seed_geojsons(self, specific_file=None):
        """Seed GeoJSON files"""
        base_dir = Path(__file__).parent.parent.parent.parent
        geojson_dir = base_dir / "seed" / "geojsons"

        if not geojson_dir.exists():
            self.stdout.write(self.style.ERROR(f"❌ Directory not found: {geojson_dir}"))
            return

        # Get files to process
        if specific_file:
            file_path = geojson_dir / specific_file
            if not file_path.exists():
                self.stdout.write(self.style.ERROR(f"❌ File not found: {specific_file}"))
                return
            geojson_files = [file_path]
        else:
            geojson_files = list(geojson_dir.glob("*.geojson"))

        if not geojson_files:
            self.stdout.write(self.style.WARNING("⚠️  No GeoJSON files found"))
            return

        created_count = 0
        updated_count = 0
        skipped_count = 0

        for file_path in geojson_files:
            filename = file_path.name

            # Parse filename to get region name and type
            region_info = self.parse_filename(filename)
            if not region_info:
                self.stdout.write(
                    self.style.WARNING(f"⚠️  Skipping {filename}: Cannot parse region name")
                )
                skipped_count += 1
                continue

            # Load GeoJSON
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    geojson_data = json.load(f)
            except json.JSONDecodeError:
                self.stdout.write(self.style.ERROR(f"❌ Invalid JSON: {filename}"))
                skipped_count += 1
                continue

            # Validate basic structure
            if not self.is_valid_geojson(geojson_data):
                self.stdout.write(self.style.ERROR(f"❌ Invalid GeoJSON: {filename}"))
                skipped_count += 1
                continue

            # Find matching region
            try:
                region = GeoRegion.objects.get(name__iexact=region_info["region_name"])
            except GeoRegion.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(
                        f'❌ Region not found: {region_info["region_name"]} for {filename}'
                    )
                )
                skipped_count += 1
                continue
            except GeoRegion.MultipleObjectsReturned:
                region = GeoRegion.objects.filter(name__iexact=region_info["region_name"]).first()

            # Create or update GeoJSON
            geojson, created = GeoJSON.objects.update_or_create(
                region=region,
                description=region_info["description"],
                defaults={
                    "geojson_data": geojson_data,
                    "properties_key": "name",
                    "is_default": region_info["is_default"],
                    "org": None,  # System defaults
                },
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f'✅ Created: {region_info["description"]}'))
            else:
                updated_count += 1
                self.stdout.write(f'📝 Updated: {region_info["description"]}')

        self.stdout.write(
            f"🗺️  GeoJSONs: {created_count} created, {updated_count} updated, {skipped_count} skipped"
        )

    def parse_filename(self, filename):
        """Parse filename to extract region info"""
        # Remove .geojson extension
        base_name = filename.replace(".geojson", "")

        # Replace underscores with spaces for region names
        normalized_name = base_name.replace("_", " ")

        # Check for variant keywords
        variant_keywords = ["custom", "test", "alt", "simplified"]
        is_default = True

        for keyword in variant_keywords:
            if normalized_name.lower().endswith(f" {keyword}"):
                region_name = normalized_name[: -len(f" {keyword}")].strip()
                is_default = False
                description = f"{keyword.title()} GeoJSON for {region_name}"
                break
        else:
            region_name = normalized_name
            description = f"Default GeoJSON for {region_name}"

        return {"region_name": region_name, "description": description, "is_default": is_default}

    def is_valid_geojson(self, data):
        """Basic GeoJSON validation"""
        return (
            isinstance(data, dict)
            and data.get("type") == "FeatureCollection"
            and "features" in data
            and isinstance(data["features"], list)
            and len(data["features"]) > 0
        )
