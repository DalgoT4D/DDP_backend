"""management/commands/github_to_ui4t.py {org} {file_path}"""

import json
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from ddpui.models.org import Org, OrgDbt
from ddpui.utils import importdbtproject


class Command(BaseCommand):
    """Load DBT manifest data into the database"""

    help = "Load DBT manifest data into the database"
    exclude_schemas = [
        "elementary",
        "prod_elementary",
        "intermediate_elementary",
        "information_schema",
        "pg_catalog",
        "public",
    ]

    def add_arguments(self, parser):
        """command line arguments for the script"""
        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument("file_path", type=str, help="Path to the DBT manifest JSON file")

    def handle(self, *args, **options):
        """Loads DBT manifest data into the database for a specified organization."""
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return
        orgdbt = OrgDbt.objects.filter(org=org).first()
        if orgdbt is None:
            raise CommandError(f"No OrgDbt for {org.slug}")

        try:
            with open(options["file_path"], "r", encoding="utf-8") as f:
                manifest_data = json.load(f)
        except FileNotFoundError as err:
            raise CommandError(f"File not found: {options['file_path']}") from err
        except json.JSONDecodeError as e:
            raise CommandError(f"Invalid JSON in file {options['file_path']}: {e}") from e
        except Exception as e:
            raise CommandError(f"Error reading file {options['file_path']}: {e}") from e

        models_metadata = importdbtproject.extract_models(manifest_data)

        try:
            with transaction.atomic():
                print("create OrgDbtModel:models")
                for modelname, model_metadata in models_metadata.items():
                    if model_metadata.dbschema in self.exclude_schemas:
                        continue
                    print(modelname)
                    importdbtproject.create_orgdbtmodel_model(orgdbt, model_metadata)

                print("create OrgDbtModel:sources")
                sources_metadata = importdbtproject.extract_sources(manifest_data)
                for sourcename, source_metadata in sources_metadata.items():
                    if source_metadata.dbschema in self.exclude_schemas:
                        continue
                    print(sourcename)
                    importdbtproject.create_orgdbtmodel_source(orgdbt, source_metadata)

                print("creating DbtEdges")
                for modelname, model_metadata in models_metadata.items():
                    if model_metadata.dbschema in self.exclude_schemas:
                        continue
                    print(modelname)
                    importdbtproject.create_orgdbtedges(orgdbt, model_metadata)

                print("creating DbtOperations")
                for modelname, model_metadata in models_metadata.items():
                    if model_metadata.dbschema in self.exclude_schemas:
                        continue
                    print(modelname)
                    importdbtproject.create_orgdbtoperation(orgdbt, model_metadata)

                print("completed importing dbt project")
        except Exception as e:
            raise CommandError(f"Error importing dbt project: {e}") from e
