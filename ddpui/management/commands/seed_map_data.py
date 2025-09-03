"""
Enhanced GeoJSON seeding with comprehensive validation
Addresses all potential failure scenarios
"""

import json
import os
from pathlib import Path
from django.core.management.base import BaseCommand
from django.db import transaction
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON


class Command(BaseCommand):
    help = "Enhanced seed GeoRegions and GeoJSONs with full validation"

    def add_arguments(self, parser):
        parser.add_argument(
            "--validate-only", action="store_true", help="Only validate, don't seed"
        )
        parser.add_argument("--country", type=str, help="Process only specific country")

    def handle(self, *args, **options):
        self.stdout.write("🌍 ENHANCED MAP DATA SEEDER")
        self.stdout.write("=" * 50)

        try:
            # Step 1: Validate all metadata files
            validation_results = self.validate_all_countries()

            if not validation_results["valid"]:
                self.stdout.write(
                    self.style.ERROR("❌ Validation failed! Fix errors before seeding.")
                )
                for error in validation_results["errors"]:
                    self.stdout.write(self.style.ERROR(f"  • {error}"))
                return

            if options["validate_only"]:
                self.stdout.write(self.style.SUCCESS("✅ All validations passed!"))
                return

            # Step 2: Seed with validated data
            with transaction.atomic():
                self.seed_from_metadata(options.get("country"))
                self.stdout.write(self.style.SUCCESS("✅ Enhanced seeding completed!"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Critical error: {e}"))
            raise

    def validate_all_countries(self):
        """Comprehensive validation of all country metadata and files"""
        base_dir = Path(__file__).parent.parent.parent.parent
        countries_dir = base_dir / "seed" / "geojsons" / "countries"

        errors = []
        used_ids = set()

        if not countries_dir.exists():
            return {"valid": False, "errors": ["Countries directory not found"]}

        for country_dir in countries_dir.iterdir():
            if not country_dir.is_dir():
                continue

            country_name = country_dir.name

            # Check 1: Metadata file exists
            metadata_file = country_dir / "country_metadata.json"
            if not metadata_file.exists():
                errors.append(f"{country_name}: Missing country_metadata.json")
                continue

            # Check 2: Valid JSON structure
            try:
                with open(metadata_file) as f:
                    metadata = json.load(f)
            except json.JSONDecodeError as e:
                errors.append(f"{country_name}: Invalid JSON in metadata - {e}")
                continue

            # Check 3: Required fields
            if not self.validate_metadata_structure(metadata, country_name, errors):
                continue

            # Check 4: ID uniqueness across countries
            country_id = metadata["country"]["id"]
            if country_id in used_ids:
                errors.append(f"{country_name}: Duplicate country ID {country_id}")
            used_ids.add(country_id)

            for region in metadata.get("regions", []):
                region_id = region["id"]
                if region_id in used_ids:
                    errors.append(f"{country_name}: Duplicate region ID {region_id}")
                used_ids.add(region_id)

            # Check 5: File-metadata consistency
            self.validate_file_metadata_consistency(country_dir, metadata, country_name, errors)

            # Check 6: Parent-child relationships
            self.validate_parent_child_relationships(metadata, country_name, errors)

        return {"valid": len(errors) == 0, "errors": errors}

    def validate_metadata_structure(self, metadata, country_name, errors):
        """Validate metadata JSON structure"""
        required_fields = {
            "country": ["id", "name", "type", "country_code", "region_code", "display_name"],
            "hierarchy": [],  # At least one layer definition
            "regions": [],  # At least regions array
        }

        for section, fields in required_fields.items():
            if section not in metadata:
                errors.append(f"{country_name}: Missing '{section}' section in metadata")
                return False

            for field in fields:
                if field not in metadata[section]:
                    errors.append(f"{country_name}: Missing '{field}' in {section}")
                    return False

        # Validate hierarchy has at least one layer
        if not metadata["hierarchy"]:
            errors.append(f"{country_name}: No hierarchy layers defined")
            return False

        return True

    def validate_file_metadata_consistency(self, country_dir, metadata, country_name, errors):
        """Ensure all GeoJSON files have corresponding metadata entries"""

        # Get all GeoJSON files
        geojson_files = set()

        # Check country-level files
        for geojson_file in country_dir.glob("*.json"):
            if geojson_file.name == "country_metadata.json":
                continue
            region_name = geojson_file.stem.replace("_", " ").title()
            geojson_files.add(region_name)

        # Check layer files
        for layer_dir in country_dir.iterdir():
            if layer_dir.is_dir() and layer_dir.name.startswith("layer"):
                for geojson_file in layer_dir.glob("*.json"):
                    region_name = geojson_file.stem.replace("_", " ").title()
                    geojson_files.add(region_name)

        # Get all metadata regions (including country)
        metadata_regions = set()
        metadata_regions.add(metadata["country"]["name"])  # Add country
        for region in metadata.get("regions", []):
            metadata_regions.add(region["name"])

        # Check for missing metadata
        missing_metadata = geojson_files - metadata_regions
        for region in missing_metadata:
            errors.append(f"{country_name}: GeoJSON file exists but no metadata for '{region}'")

        # Check for orphaned metadata
        orphaned_metadata = metadata_regions - geojson_files
        for region in orphaned_metadata:
            errors.append(f"{country_name}: Metadata exists but no GeoJSON file for '{region}'")

    def validate_parent_child_relationships(self, metadata, country_name, errors):
        """Validate all parent_id references exist"""
        region_ids = {metadata["country"]["id"]}  # Start with country ID

        for region in metadata.get("regions", []):
            region_ids.add(region["id"])

        for region in metadata.get("regions", []):
            parent_id = region.get("parent_id")
            if parent_id and parent_id not in region_ids:
                errors.append(
                    f"{country_name}: Region '{region['name']}' has invalid parent_id {parent_id}"
                )

    def seed_from_metadata(self, specific_country=None):
        """Seed database using validated metadata"""
        base_dir = Path(__file__).parent.parent.parent.parent
        countries_dir = base_dir / "seed" / "geojsons" / "countries"

        created_regions = 0
        created_geojsons = 0

        for country_dir in countries_dir.iterdir():
            if not country_dir.is_dir():
                continue

            country_name = country_dir.name
            if specific_country and country_name != specific_country:
                continue

            metadata_file = country_dir / "country_metadata.json"
            with open(metadata_file) as f:
                metadata = json.load(f)

            # Seed country
            country_data = metadata["country"]
            country_region, created = GeoRegion.objects.update_or_create(
                id=country_data["id"], defaults=country_data
            )
            if created:
                created_regions += 1
                self.stdout.write(f"✅ Created country: {country_data['name']}")

            # Seed regions in dependency order
            regions = metadata.get("regions", [])
            regions_by_parent = {}

            for region in regions:
                parent_id = region.get("parent_id", country_data["id"])
                if parent_id not in regions_by_parent:
                    regions_by_parent[parent_id] = []
                regions_by_parent[parent_id].append(region)

            # Process regions level by level
            created_regions = self.seed_regions_by_level(
                regions_by_parent, country_data["id"], created_regions
            )

            # Seed GeoJSONs
            geojson_count = self.seed_geojsons_for_country(country_dir, metadata)
            created_geojsons += geojson_count

        self.stdout.write(
            f"📍 Total: {created_regions} regions, {created_geojsons} geojsons created"
        )

    def seed_regions_by_level(self, regions_by_parent, current_parent_id, created_count):
        """Recursively seed regions maintaining parent-child dependencies"""
        if current_parent_id not in regions_by_parent:
            return created_count

        for region_data in regions_by_parent[current_parent_id]:
            region, created = GeoRegion.objects.update_or_create(
                id=region_data["id"],
                defaults={
                    "name": region_data["name"],
                    "type": region_data["type"],
                    "parent_id": current_parent_id,
                    "country_code": region_data["country_code"],
                    "region_code": region_data["region_code"],
                    "display_name": region_data["display_name"],
                },
            )
            if created:
                created_count += 1
                self.stdout.write(f"✅ Created region: {region_data['name']}")

            # Recursively process children
            created_count = self.seed_regions_by_level(
                regions_by_parent, region_data["id"], created_count
            )

        return created_count

    def seed_geojsons_for_country(self, country_dir, metadata):
        """Seed GeoJSON files for a specific country"""
        created_count = 0

        # First, process country-level files (in country root directory)
        for geojson_file in country_dir.glob("*.json"):
            if geojson_file.name == "country_metadata.json":
                continue  # Skip metadata file
            created_count += self.process_geojson_file(geojson_file, metadata)

        # Then, process layer directories
        for layer_dir in country_dir.iterdir():
            if not layer_dir.is_dir() or not layer_dir.name.startswith("layer"):
                continue

            for geojson_file in layer_dir.glob("*.json"):
                created_count += self.process_geojson_file(geojson_file, metadata)

        return created_count

    def process_geojson_file(self, geojson_file, metadata):
        """Process a single GeoJSON file"""
        region_name = geojson_file.stem.replace("_", " ").title()

        # Find matching region in metadata (check both regions and country)
        region_data = None

        # Check in regions array
        for region in metadata.get("regions", []):
            if region["name"].lower() == region_name.lower():
                region_data = region
                break

        # If not found, check if it's the country itself
        if not region_data and metadata["country"]["name"].lower() == region_name.lower():
            region_data = metadata["country"]

        if not region_data:
            self.stdout.write(self.style.ERROR(f"❌ No metadata for {region_name}"))
            return 0

        # Load and validate GeoJSON
        try:
            with open(geojson_file, "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.stdout.write(self.style.ERROR(f"❌ Invalid file {geojson_file.name}: {e}"))
            return 0

        if not self.is_valid_geojson(geojson_data):
            self.stdout.write(self.style.ERROR(f"❌ Invalid GeoJSON structure: {geojson_file.name}"))
            return 0

        # Create GeoJSON record
        region = GeoRegion.objects.get(id=region_data["id"])
        geojson, created = GeoJSON.objects.update_or_create(
            region=region,
            defaults={
                "geojson_data": geojson_data,
                "properties_key": "name",
                "is_default": True,
                "name": f"{region_name} Map",
                "description": f"Default GeoJSON for {region_name}",
                "org": None,
            },
        )

        if created:
            self.stdout.write(self.style.SUCCESS(f"✅ Created GeoJSON: {region_name}"))
            return 1

        return 0

    def is_valid_geojson(self, data):
        """Validate GeoJSON structure"""
        return (
            isinstance(data, dict)
            and data.get("type") == "FeatureCollection"
            and "features" in data
            and isinstance(data["features"], list)
            and len(data["features"]) > 0
        )
